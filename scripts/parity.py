"""dbt track: same Bronze in, same answer out?

    python -m scripts.parity          (after `make dbt`)

Compares the dbt build (dbt_project/dbt.duckdb) with the lite pipeline
(warehouse.duckdb, rebuilt fresh here) on the two tables both implement:
    silver_tickets       MERGE on ticket_id, newest LSN wins, deletes = tombstones
    gold_feature_daily   microbatch by event day with lookback
Two implementations, one checksum — or one of them is wrong.
"""
from __future__ import annotations

import sys

import duckdb

from main import fresh_build
from pipeline import config
from pipeline.checksum import query_checksum

DBT_DB = config.ROOT / "dbt_project" / "dbt.duckdb"

QUERIES = {
    "silver_tickets": """SELECT ticket_id, user_id, subject, body, priority, status, category,
                                created_at, updated_at, is_deleted, _lsn FROM silver_tickets""",
    "gold_feature_daily": """SELECT user_id, CAST(event_date AS DATE) AS event_date, n_events,
                                    n_clicks, n_feedback_up, n_feedback_down
                             FROM gold_feature_daily""",
}


def main() -> int:
    if not DBT_DB.exists():
        print("dbt_project/dbt.duckdb not found — run `make dbt` first")
        return 2
    fresh_build(quiet=True)
    lite = duckdb.connect(str(config.WAREHOUSE), read_only=True)
    dbt = duckdb.connect(str(DBT_DB), read_only=True)
    ok = True
    print("=== parity: lite pipeline vs dbt ===")
    for name, sql in QUERIES.items():
        a, b = query_checksum(lite, sql), query_checksum(dbt, sql)
        same = a == b
        ok &= same
        print(f"  [{'OK ' if same else 'XX '}] {name:<20} lite {a[:12]}  dbt {b[:12]}")
    lite.close()
    dbt.close()
    print("RESULT: " + ("PARITY — both implementations agree" if ok else "MISMATCH"))
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
