"""Contracts of the pipeline's tables (Bronze / Silver / Gold) on a fresh build.

A fresh clone ships with three planted bugs: several of these tests FAIL until
you fix them. Never edit a test to make it pass — fix the pipeline.
"""
from __future__ import annotations

from pipeline import config
from pipeline.bronze import bronze_scan
from pipeline.checksum import query_checksum
from pipeline.gold import feature_daily_full_recompute_sql, lateness_profile


def rows(con, sql):
    return con.execute(sql).fetchall()


# ── Bronze ──────────────────────────────────────────────────────────────────

def test_bronze_is_raw_and_append_only(built):
    con, _ = built
    (tomb,) = con.execute(f"SELECT count(*) FROM {bronze_scan('tickets')} WHERE _op IS NULL").fetchone()
    (n_t91,) = con.execute(f"""SELECT count(*) FROM {bronze_scan('tickets')}
                              WHERE _payload LIKE '%T-91%' AND _op = 'u'""").fetchone()
    assert tomb == 1          # the Kafka tombstone is kept
    assert n_t91 == 3         # 2 updates, one delivered twice: Bronze keeps all 3


# ── Silver ──────────────────────────────────────────────────────────────────

def test_silver_tickets_one_row_per_ticket(built):
    con, _ = built
    n, n_ids = con.execute("SELECT count(*), count(DISTINCT ticket_id) FROM silver_tickets").fetchone()
    assert n == n_ids == 12


def test_silver_tickets_latest_state_wins(built):
    con, _ = built
    assert rows(con, "SELECT priority, status, category FROM silver_tickets "
                     "WHERE ticket_id = 'T-91'") == [("high", "closed", "bug")]


def test_cdc_delete_becomes_tombstone(built):
    con, _ = built
    assert rows(con, "SELECT is_deleted, user_id, subject, body FROM silver_tickets "
                     "WHERE ticket_id = 'T-97'") == [(True, None, None, None)]


def test_scd2_history_has_every_version(built):
    con, _ = built
    hist = rows(con, """SELECT priority, status, is_current FROM silver_ticket_history
                        WHERE ticket_id = 'T-91' ORDER BY valid_from""")
    assert hist == [("low", "open", False), ("high", "open", False), ("high", "closed", True)]


def test_silver_events_dedup_and_quarantine(built):
    con, _ = built
    n, n_ids = con.execute("SELECT count(*), count(DISTINCT event_id) FROM silver_events").fetchone()
    assert n == n_ids == 39
    assert rows(con, "SELECT event_id FROM quarantine_events ORDER BY 1") == [("e-bad-1",), ("e-bad-2",)]


# ── Gold ────────────────────────────────────────────────────────────────────

def test_feature_daily_reconciles_with_full_recompute(built):
    con, _ = built
    assert query_checksum(con, "SELECT * FROM gold_feature_daily") == \
        query_checksum(con, feature_daily_full_recompute_sql())


def test_late_events_land_in_their_event_day(built):
    con, _ = built
    assert rows(con, """SELECT n_events, n_clicks, n_feedback_down FROM gold_feature_daily
                        WHERE user_id = 'u05' AND event_date = DATE '2026-08-12'""") == [(5, 3, 1)]


def test_lookback_covers_measured_lateness(built):
    con, _ = built
    assert config.LOOKBACK_DAYS >= lateness_profile(con)["p99_days"]


def test_training_set_is_point_in_time(built):
    con, _ = built
    assert rows(con, """SELECT priority_at_creation FROM gold_training_set
                        WHERE snapshot_version = 'v2026-08-16' AND ticket_id = 'T-91'""") == [("low",)]


def test_training_snapshots_are_versioned(built):
    con, _ = built
    got = rows(con, """SELECT snapshot_version, got_negative_feedback FROM gold_training_set
                       WHERE ticket_id = 'T-88' ORDER BY 1""")
    assert got == [("v2026-08-13", False), ("v2026-08-14", False),
                   ("v2026-08-15", True), ("v2026-08-16", True)]


def test_deleted_ticket_leaves_training_and_rag(built):
    con, _ = built
    assert rows(con, """SELECT count(*) FROM gold_training_set
                        WHERE snapshot_version = 'v2026-08-16' AND ticket_id = 'T-97'""") == [(0,)]
    assert rows(con, "SELECT count(*) FROM gold_doc_chunks WHERE ticket_id = 'T-97'") == [(0,)]


def test_doc_chunks_unique(built):
    con, _ = built
    n, n_distinct = con.execute("""SELECT count(*), count(DISTINCT (ticket_id, chunk_idx))
                                   FROM gold_doc_chunks""").fetchone()
    assert n == n_distinct == 8


# ── the grading test ────────────────────────────────────────────────────────

def test_rerun_old_day_three_times_keeps_gold_checksum(sandbox):
    from rerun_check import rerun_check
    res = rerun_check(config.RERUN_DAY, write=False, quiet=True)
    assert res["ok"], res["runs"]
