"""One daily run = one DAG over one ingest day. Backfill = the same code path, day by day.

    land (Bronze) ─┬─> silver_tickets ──> silver_history ──────────────┐
                   ├─> silver_events ──> gold_feature_daily             ├─> gold_training_set
                   │                  └──────────────────────────────────┘
                   └─> silver_transcripts ─┐
                       silver_tickets ─────┴─> gold_doc_chunks
"""
from __future__ import annotations

import duckdb

from . import config
from .bronze import land_day
from .dag import DAG
from .gold import build_doc_chunks, build_feature_daily, build_training_snapshot
from .silver import (
    build_ticket_history, ensure_tables, upsert_silver_events, upsert_silver_transcripts,
    upsert_silver_tickets,
)


def connect(path=None) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(str(path or config.WAREHOUSE))
    ensure_tables(con)
    return con


def reset_warehouse() -> None:
    """Drop Silver + Gold. Bronze (the lake) is never touched: everything rebuilds from it."""
    for p in (config.WAREHOUSE, config.WAREHOUSE.with_name(config.WAREHOUSE.name + ".wal")):
        p.unlink(missing_ok=True)


def build_dag(con: duckdb.DuckDBPyConnection, day: str) -> DAG:
    dag = DAG()

    @dag.task("land")
    def _land():
        return land_day(con, day)

    @dag.task("silver_tickets", upstream=["land"])
    def _silver_tickets(_):
        return upsert_silver_tickets(con, day)

    @dag.task("silver_history", upstream=["silver_tickets"])
    def _silver_history(_):
        return build_ticket_history(con)

    @dag.task("silver_events", upstream=["land"])
    def _silver_events(_):
        return upsert_silver_events(con, day)

    @dag.task("silver_transcripts", upstream=["land"])
    def _silver_transcripts(_):
        return upsert_silver_transcripts(con, day)

    @dag.task("gold_feature_daily", upstream=["silver_events"])
    def _gold_features(_):
        return build_feature_daily(con, day)

    @dag.task("gold_training_set", upstream=["silver_history", "silver_events"])
    def _gold_training(_a, _b):
        return build_training_snapshot(con, day)

    @dag.task("gold_doc_chunks", upstream=["silver_tickets", "silver_transcripts"])
    def _gold_chunks(_a, _b):
        return build_doc_chunks(con)

    return dag


def run_day(con: duckdb.DuckDBPyConnection, day: str) -> dict:
    con.execute("BEGIN TRANSACTION")       # a failed run leaves the warehouse untouched
    try:
        results = build_dag(con, day).run()
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return results


def backfill(con: duckdb.DuckDBPyConnection, start: str, end: str, quiet: bool = False) -> list[dict]:
    out = []
    for day in config.days_between(start, end):
        r = run_day(con, day)
        out.append(r)
        if not quiet:
            print(summarize(day, r))
    return out


def summarize(day: str, r: dict) -> str:
    land = r["land"]
    landed = sum(v["rows"] for v in land.values() if v["status"] == "landed")
    st, ev, ch = r["silver_tickets"], r["silver_events"], r["gold_doc_chunks"]
    return (f"  {day}  bronze+{landed:<3} tickets:{st['silver_rows']:<3} "
            f"events:{ev['silver_rows']:<3} quarantined:{ev['quarantined']} "
            f"features[{r['gold_feature_daily']['window']}] "
            f"snapshot {r['gold_training_set']['version']}:{r['gold_training_set']['rows']} "
            f"chunks:{ch['chunks']} (embedded {ch['embedded_new']})")
