"""The grading test: re-run an old day three times, Gold checksum must match a fresh build.

Kept in its own module on purpose: the `built` fixture holds a DuckDB connection
open for the whole module, and DuckDB reuses an open database within the process,
so a "fresh build" next to it would silently run on top of the old warehouse.
"""
from __future__ import annotations

from pipeline import config


def test_rerun_old_day_three_times_keeps_gold_checksum(sandbox):
    from scripts.rerun_check import rerun_check
    res = rerun_check(config.RERUN_DAY, write=False, quiet=True)
    assert res["ok"], res["runs"]
