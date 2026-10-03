"""Shared fixtures: every test builds into a temporary lake + warehouse, never the repo's."""
from __future__ import annotations

import pytest

from pipeline import config


@pytest.fixture(scope="module")
def sandbox(tmp_path_factory):
    """Point the pipeline at a throw-away lake/ and warehouse.duckdb."""
    root = tmp_path_factory.mktemp("lab17")
    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(config, "LAKE_DIR", root / "lake")
        mp.setattr(config, "BRONZE_DIR", root / "lake" / "bronze")
        mp.setattr(config, "WAREHOUSE", root / "warehouse.duckdb")
        mp.setattr(config, "SUBMISSION_DIR", root / "submission")
        yield root


@pytest.fixture(scope="module")
def built(sandbox):
    """A fresh build (all days) and an open connection to it."""
    from main import fresh_build
    from pipeline.run import connect

    checksums = fresh_build(quiet=True)
    con = connect()
    yield con, checksums
    con.close()
