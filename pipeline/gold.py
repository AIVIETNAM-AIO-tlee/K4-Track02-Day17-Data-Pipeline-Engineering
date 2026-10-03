"""Gold — "đúng hình dạng cho đúng người dùng". Three tables, three meanings of "clean".

    gold_feature_daily   routing agent   1 row = 1 user x 1 day (event time);
                                          late events must still land in the right day
    gold_training_set    classifier      immutable, versioned snapshots: v<day> is
                                          rebuilt from Bronze as of that day, never edited
    gold_doc_chunks      RAG index       1 row = 1 chunk of a live ticket's transcript;
                                          embedding cached by hash(text) + model version
"""
from __future__ import annotations

import math

import duckdb

from . import config
from .checksum import query_checksum
from .embed import chunk_words, embed_text, text_hash
from .staging import event_lateness_sql, ticket_changes_sql


class SnapshotImmutableError(RuntimeError):
    """Raised when a rebuild would change an existing training snapshot."""


# ── gold_feature_daily ──────────────────────────────────────────────────────

FEATURE_SELECT = """
    SELECT user_id,
           CAST(event_time AS DATE)                                  AS event_date,
           count(*)                                                  AS n_events,
           count(*) FILTER (WHERE type = 'click')                    AS n_clicks,
           count(*) FILTER (WHERE type = 'feedback' AND rating = 'up')   AS n_feedback_up,
           count(*) FILTER (WHERE type = 'feedback' AND rating = 'down') AS n_feedback_down
    FROM silver_events
"""


def lateness_profile(con: duckdb.DuckDBPyConnection) -> dict:
    """Measure, don't guess: how late do events reach Bronze (calendar days)?"""
    p50, p95, p99, mx, n = con.execute(f"""
        SELECT quantile_cont(lateness_days, 0.50), quantile_cont(lateness_days, 0.95),
               quantile_cont(lateness_days, 0.99), max(lateness_days), count(*)
        FROM ({event_lateness_sql()})
    """).fetchone()
    if not n:
        return {"n": 0, "p50": 0, "p95": 0, "p99": 0, "max": 0, "p99_days": 0}
    return {"n": n, "p50": p50, "p95": p95, "p99": p99, "max": mx,
            "p99_days": int(math.ceil(p99))}


def build_feature_daily(con: duckdb.DuckDBPyConnection, day: str) -> dict:
    """Recompute the partitions [day - LOOKBACK_DAYS, day] from Silver (overwrite-partition)."""
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS gold_feature_daily AS
        {FEATURE_SELECT} WHERE false GROUP BY ALL
    """)
    start = config.shift(day, -config.LOOKBACK_DAYS)
    con.execute("DELETE FROM gold_feature_daily WHERE event_date BETWEEN ? AND ?", [start, day])
    con.execute(f"""
        INSERT INTO gold_feature_daily
        {FEATURE_SELECT}
        WHERE CAST(event_time AS DATE) BETWEEN ? AND ?
        GROUP BY ALL
    """, [start, day])
    (n,) = con.execute("SELECT count(*) FROM gold_feature_daily").fetchone()
    return {"window": f"{start}..{day}", "rows": n}


def feature_daily_full_recompute_sql() -> str:
    """What gold_feature_daily SHOULD contain: one aggregate over all of Silver."""
    return f"{FEATURE_SELECT} GROUP BY ALL"


# ── gold_training_set ───────────────────────────────────────────────────────

def _training_snapshot_sql(day: str) -> str:
    """Ticket state AS OF `day`, rebuilt from Bronze -> reproducible forever."""
    return f"""
    WITH changes AS (
        SELECT * FROM ({ticket_changes_sql(upto=day)})
        QUALIFY row_number() OVER (PARTITION BY ticket_id, _lsn ORDER BY _ingested_at) = 1
    ),
    latest AS (          -- state of each ticket as of `day`
        SELECT * FROM changes
        QUALIFY row_number() OVER (PARTITION BY ticket_id ORDER BY _lsn DESC) = 1
    ),
    at_creation AS (     -- point-in-time: the priority the ticket had when it was created
        SELECT ticket_id, priority AS priority_at_creation FROM changes
        WHERE _op IN ('c', 'r')
        QUALIFY row_number() OVER (PARTITION BY ticket_id ORDER BY _lsn) = 1
    ),
    feedback AS (        -- only feedback that had reached us by `day`
        SELECT ticket_id, bool_or(rating = 'down') AS got_negative_feedback
        FROM silver_events
        WHERE type = 'feedback' AND ticket_id IS NOT NULL AND _batch_id <= '{day}'
        GROUP BY ticket_id
    )
    SELECT 'v{day}'                                     AS snapshot_version,
           l.ticket_id,
           mask_pii(l.subject || '. ' || l.body)       AS text,
           l.category                                  AS label,
           a.priority_at_creation,
           coalesce(f.got_negative_feedback, false)    AS got_negative_feedback,
           CAST(l.created_at AS DATE)                  AS created_date
    FROM latest l
    JOIN at_creation a USING (ticket_id)
    LEFT JOIN feedback f USING (ticket_id)
    WHERE l._op <> 'd' AND l.status = 'closed' AND l.category IS NOT NULL
    """


def build_training_snapshot(con: duckdb.DuckDBPyConnection, day: str) -> dict:
    version = f"v{day}"
    con.execute(f"""
        CREATE TABLE IF NOT EXISTS gold_training_set AS
        {_training_snapshot_sql(day)} LIMIT 0
    """)
    con.execute(f"CREATE OR REPLACE TEMP TABLE _snapshot AS {_training_snapshot_sql(day)}")
    (existing,) = con.execute(
        "SELECT count(*) FROM gold_training_set WHERE snapshot_version = ?", [version]).fetchone()
    if existing:
        old = query_checksum(con, f"SELECT * FROM gold_training_set WHERE snapshot_version = '{version}'")
        new = query_checksum(con, "SELECT * FROM _snapshot")
        if old != new:
            raise SnapshotImmutableError(
                f"snapshot {version} already exists with different content "
                f"({old[:12]} != {new[:12]}). Snapshots are immutable: create a new "
                f"version, or rebuild the warehouse from Bronze (python main.py).")
        status = "unchanged"
    else:
        con.execute("INSERT INTO gold_training_set SELECT * FROM _snapshot")
        status = "created"
    (rows,) = con.execute("SELECT count(*) FROM _snapshot").fetchone()
    return {"version": version, "rows": rows, "status": status}


# ── gold_doc_chunks ─────────────────────────────────────────────────────────

def build_doc_chunks(con: duckdb.DuckDBPyConnection) -> dict:
    """Chunk every live ticket's transcript; embed only chunks never embedded before."""
    con.execute("""CREATE TABLE IF NOT EXISTS embedding_cache (
        text_hash VARCHAR, model_version VARCHAR, embedding DOUBLE[])""")
    live = con.execute("""
        SELECT tr.ticket_id, tr.text, t.category, t.priority, t.status
        FROM silver_transcripts tr
        JOIN silver_tickets t ON t.ticket_id = tr.ticket_id
        WHERE NOT t.is_deleted
        ORDER BY tr.ticket_id
    """).fetchall()
    rows = []
    for ticket_id, text, category, priority, status in live:
        for idx, chunk in enumerate(chunk_words(text, config.CHUNK_WORDS, config.CHUNK_OVERLAP)):
            rows.append((ticket_id, idx, chunk, text_hash(chunk), category, priority, status))
    con.execute("""CREATE OR REPLACE TEMP TABLE _chunks (
        ticket_id VARCHAR, chunk_idx INTEGER, chunk_text VARCHAR, text_hash VARCHAR,
        category VARCHAR, priority VARCHAR, status VARCHAR)""")
    if rows:
        con.executemany("INSERT INTO _chunks VALUES (?, ?, ?, ?, ?, ?, ?)", rows)

    missing = con.execute("""
        SELECT DISTINCT c.text_hash, c.chunk_text FROM _chunks c
        WHERE NOT EXISTS (SELECT 1 FROM embedding_cache e
                          WHERE e.text_hash = c.text_hash AND e.model_version = ?)
        ORDER BY c.text_hash
    """, [config.EMBEDDING_MODEL_VERSION]).fetchall()
    if missing:
        con.executemany(
            "INSERT INTO embedding_cache VALUES (?, ?, ?)",
            [(h, config.EMBEDDING_MODEL_VERSION, embed_text(t)) for h, t in missing])

    con.execute("""
        CREATE OR REPLACE TABLE gold_doc_chunks AS
        SELECT c.ticket_id, c.chunk_idx, c.chunk_text, c.text_hash,
               e.model_version AS embedding_model_version, e.embedding,
               c.category, c.priority, c.status
        FROM _chunks c
        JOIN embedding_cache e
          ON e.text_hash = c.text_hash AND e.model_version = ?
        ORDER BY c.ticket_id, c.chunk_idx
    """, [config.EMBEDDING_MODEL_VERSION])
    (n,) = con.execute("SELECT count(*) FROM gold_doc_chunks").fetchone()
    return {"chunks": n, "embedded_new": len(missing)}
