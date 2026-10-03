"""Bonus: the SAME daily run as main.py, on a real Airflow 3 (TaskFlow, Task SDK).

Airflow 3 changes you can see here (slide "Airflow 2 -> 3"):
  * imports come from `airflow.sdk`
  * catchup defaults to False (explicit here; this DAG requires Airflow 3)
  * backfill is a scheduler-managed object:  airflow backfill create ...
  * every run records which DAG version produced it (DAG versioning)

One task per layer, strictly sequential: DuckDB allows a single writer.
"""
from __future__ import annotations

import pendulum
from airflow.sdk import dag, get_current_context, task

from pipeline.bronze import land_day
from pipeline.checksum import gold_checksums
from pipeline.gold import build_doc_chunks, build_feature_daily, build_training_snapshot
from pipeline.run import connect
from pipeline.silver import (
    build_ticket_history, upsert_silver_events, upsert_silver_tickets, upsert_silver_transcripts,
)


def _ingest_day() -> str:
    """The ingest day this run processes = the run's logical date (YYYY-MM-DD)."""
    ctx = get_current_context()
    if ctx.get("ds"):
        return ctx["ds"]
    return ctx["dag_run"].run_after.date().isoformat()   # manual run without logical date


@dag(
    dag_id="day17_support_pipeline",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 8, 10, tz="UTC"),
    end_date=pendulum.datetime(2026, 8, 16, tz="UTC"),
    catchup=False,
    max_active_runs=1,          # backfill has its own limit; see docs/AIRFLOW.md
    tags=["day17", "track2"],
)
def day17_support_pipeline():
    @task
    def land_bronze() -> dict:
        con = connect()
        try:
            return land_day(con, _ingest_day())
        finally:
            con.close()

    @task
    def build_silver(_landed: dict) -> dict:
        day, con = _ingest_day(), connect()
        try:
            return {
                "tickets": upsert_silver_tickets(con, day),
                "events": upsert_silver_events(con, day),
                "transcripts": upsert_silver_transcripts(con, day),
                "history_rows": build_ticket_history(con),
            }
        finally:
            con.close()

    @task
    def build_gold(_silver: dict) -> dict:
        day, con = _ingest_day(), connect()
        try:
            return {
                "feature_daily": build_feature_daily(con, day),
                "training_set": build_training_snapshot(con, day),
                "doc_chunks": build_doc_chunks(con),
                "checksums": gold_checksums(con),
            }
        finally:
            con.close()

    build_gold(build_silver(land_bronze()))


day17_support_pipeline()
