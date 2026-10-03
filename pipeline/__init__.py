"""Day 17 lab — the data pipeline behind an AI customer-support platform.

Zero-key, DuckDB-only. Bronze (immutable Parquet) -> Silver (MERGE on keys)
-> three Gold tables, one per AI consumer. See README.md.
"""
