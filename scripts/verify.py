"""End-to-end contract check (zero-key). Exit 0 = every contract of the pipeline holds.

    python -m scripts.verify

The repo ships with THREE planted bugs, so a fresh clone prints FAILURES.
Your job (Lab 17): make this print ALL PASS without weakening any check.
"""
from __future__ import annotations

import re
import sys

from pipeline import config
from pipeline.bronze import batch_path, bronze_files, bronze_scan, land_batch
from pipeline.gold import feature_daily_full_recompute_sql, lateness_profile
from pipeline.checksum import query_checksum
from pipeline.run import connect, run_day
from main import fresh_build
from scripts.rerun_check import rerun_check

PII_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"
                    r"|(\+84|0)[ .-]?[0-9]{2,3}[ .-]?[0-9]{3}[ .-]?[0-9]{3,4}")

_results: list[tuple[str, bool, str]] = []


def check(layer: str, label: str, cond: bool, detail: str = "") -> bool:
    _results.append((layer, bool(cond), label))
    mark = "OK " if cond else "XX "
    print(f"  [{mark}] {layer:<7} {label}" + (f"  ({detail})" if detail and not cond else ""))
    return bool(cond)


def one(con, sql, params=None):
    return con.execute(sql, params or []).fetchone()


def run() -> bool:
    fresh_build(quiet=True)          # reset Silver/Gold, backfill every day from Bronze
    con = connect()
    try:
        days = config.days_between(config.FIRST_DAY, config.LAST_DAY)

        # ── Bronze ──────────────────────────────────────────────────────────
        n_files = sum(len(bronze_files(s)) for s in config.SOURCES)
        check("Bronze", "every daily batch landed as Parquet (7 days x 3 sources)",
              n_files == len(days) * len(config.SOURCES), f"{n_files} files")
        p = batch_path("tickets", config.RERUN_DAY)
        before = (p.stat().st_size, p.stat().st_mtime_ns)
        again = land_batch(con, "tickets", config.RERUN_DAY)
        check("Bronze", "re-landing a batch is a no-op (append-only, no duplicate file)",
              again["status"] == "already-landed" and before == (p.stat().st_size, p.stat().st_mtime_ns))
        (n_tomb,) = one(con, f"SELECT count(*) FROM {bronze_scan('tickets')} WHERE _op IS NULL")
        (n_dup,) = one(con, f"""SELECT count(*) - count(DISTINCT (_kafka_partition, _kafka_offset))
                               FROM {bronze_scan('events')}""")
        check("Bronze", "Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there",
              n_tomb == 1 and n_dup == 2, f"tombstones={n_tomb}, redeliveries={n_dup}")

        # ── Silver ──────────────────────────────────────────────────────────
        n, n_ids = one(con, "SELECT count(*), count(DISTINCT ticket_id) FROM silver_tickets")
        check("Silver", "silver_tickets has exactly one row per ticket_id", n == n_ids,
              f"{n} rows for {n_ids} tickets")
        t91 = con.execute("SELECT priority, status, category FROM silver_tickets "
                          "WHERE ticket_id = 'T-91'").fetchall()
        check("Silver", "T-91 shows its latest state: high / closed / bug",
              t91 == [("high", "closed", "bug")], f"got {t91}")
        t97 = con.execute("SELECT is_deleted, user_id, subject, body FROM silver_tickets "
                          "WHERE ticket_id = 'T-97'").fetchall()
        check("Silver", "deleted ticket T-97 is a tombstone: is_deleted and no personal data left",
              t97 == [(True, None, None, None)], f"got {t97}")
        texts = [r[0] for r in con.execute("""
            SELECT body FROM silver_tickets UNION ALL SELECT text FROM silver_transcripts
            UNION ALL SELECT text FROM gold_training_set UNION ALL SELECT chunk_text FROM gold_doc_chunks
        """).fetchall() if r[0]]
        leaks = [t for t in texts if PII_RE.search(t)]
        check("Silver", "no email / phone number survives past Bronze", not leaks,
              f"{len(leaks)} leaking rows")
        n, n_ids = one(con, "SELECT count(*), count(DISTINCT event_id) FROM silver_events")
        check("Silver", "silver_events has one row per event_id (Kafka redeliveries removed)",
              n == n_ids == 39, f"{n} rows / {n_ids} ids, expected 39")
        q = con.execute("SELECT event_id, reason FROM quarantine_events ORDER BY event_id").fetchall()
        check("Silver", "2 malformed events quarantined with a reason; the run did not halt",
              [e for e, _ in q] == ["e-bad-1", "e-bad-2"] and all(r for _, r in q), f"got {q}")

        # ── Gold ────────────────────────────────────────────────────────────
        got = query_checksum(con, "SELECT * FROM gold_feature_daily")
        want = query_checksum(con, feature_daily_full_recompute_sql())
        check("Gold", "gold_feature_daily reconciles with a full recompute from Silver",
              got == want, f"{got[:12]} != {want[:12]}")
        row = one(con, """SELECT n_events, n_feedback_down FROM gold_feature_daily
                          WHERE user_id = 'u05' AND event_date = DATE '2026-08-12'""")
        check("Gold", "u05's offline events of 08-12 (arrived 08-15) are counted on 08-12",
              row == (5, 1), f"got {row}, expected (5, 1)")
        lp = lateness_profile(con)
        check("Gold", f"LOOKBACK_DAYS covers measured P99 lateness (p99={lp['p99']:.2f} days)",
              config.LOOKBACK_DAYS >= lp["p99_days"],
              f"LOOKBACK_DAYS={config.LOOKBACK_DAYS} < {lp['p99_days']}")
        pit = one(con, """SELECT priority_at_creation FROM gold_training_set
                          WHERE snapshot_version = 'v2026-08-16' AND ticket_id = 'T-91'""")
        check("Gold", "training set uses point-in-time priority (T-91 created as 'low')",
              pit == ("low",), f"got {pit}")
        v14 = one(con, """SELECT got_negative_feedback FROM gold_training_set
                          WHERE snapshot_version = 'v2026-08-14' AND ticket_id = 'T-88'""")
        v15 = one(con, """SELECT got_negative_feedback FROM gold_training_set
                          WHERE snapshot_version = 'v2026-08-15' AND ticket_id = 'T-88'""")
        check("Gold", "late feedback creates a NEW snapshot version; the old one is untouched",
              v14 == (False,) and v15 == (True,), f"v08-14={v14}, v08-15={v15}")
        in_latest = one(con, f"""SELECT count(*) FROM gold_training_set
                                 WHERE snapshot_version = 'v{config.LAST_DAY}' AND ticket_id = 'T-97'""")
        check("Gold", "latest training snapshot excludes the deleted ticket T-97",
              in_latest == (0,), f"{in_latest[0]} row(s)")
        (n_chunks_97,) = one(con, "SELECT count(*) FROM gold_doc_chunks WHERE ticket_id = 'T-97'")
        check("Gold", "deletes propagate to the RAG index: no chunk of T-97",
              n_chunks_97 == 0, f"{n_chunks_97} chunk(s)")
        n_ch, n_ch_distinct = one(con, """SELECT count(*), count(DISTINCT (ticket_id, chunk_idx))
                                         FROM gold_doc_chunks""")
        r = run_day(con, config.LAST_DAY)
        check("Gold", "gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks",
              n_ch == n_ch_distinct and r["gold_doc_chunks"]["embedded_new"] == 0,
              f"{n_ch} rows / {n_ch_distinct} chunks, embedded {r['gold_doc_chunks']['embedded_new']}")
    finally:
        con.close()

    # ── the grading test: fresh build, then re-run an old day three times ────
    rr = rerun_check(config.RERUN_DAY, write=True, quiet=True)
    check("Rerun", f"re-run {config.RERUN_DAY} three times -> Gold checksum identical to a fresh build",
          rr["ok"], "see submission/checksums.txt")
    return all(ok for _, ok, _ in _results)


if __name__ == "__main__":
    print("=== verify.py — Day 17 pipeline contracts ===")
    success = run()
    passed = sum(ok for _, ok, _ in _results)
    print(f"\nRESULT: {passed}/{len(_results)} checks — "
          + ("ALL PASS" if success else "FAILURES ABOVE"))
    print("re-run checksums written to submission/checksums.txt")
    sys.exit(0 if success else 1)
