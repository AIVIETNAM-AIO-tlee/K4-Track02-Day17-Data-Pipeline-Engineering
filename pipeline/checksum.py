"""Order-independent table checksums — the lab's grading instrument.

A table's checksum is the md5 of all its rows rendered as text and sorted, so it
ignores row order but changes if any value, row or duplicate changes. You can
run the same SQL yourself in the DuckDB CLI:

    SELECT md5(coalesce(string_agg(CAST(t AS VARCHAR), chr(10)
                                   ORDER BY CAST(t AS VARCHAR)), ''))
    FROM gold_feature_daily AS t;
"""
from __future__ import annotations

import hashlib

import duckdb

from . import config


def query_checksum(con: duckdb.DuckDBPyConnection, sql: str) -> str:
    (h,) = con.execute(f"""
        SELECT md5(coalesce(string_agg(CAST(t AS VARCHAR), chr(10)
                                       ORDER BY CAST(t AS VARCHAR)), ''))
        FROM ({sql}) AS t
    """).fetchone()
    return h


def table_checksum(con: duckdb.DuckDBPyConnection, table: str) -> str:
    return query_checksum(con, f"SELECT * FROM {table}")


def gold_checksums(con: duckdb.DuckDBPyConnection) -> dict[str, str]:
    """Checksum of each Gold table + one combined 'gold' checksum."""
    out = {t: table_checksum(con, t) for t in config.GOLD_TABLES}
    out["gold"] = hashlib.md5("|".join(out[t] for t in config.GOLD_TABLES).encode()).hexdigest()
    return out
