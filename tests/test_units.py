"""Unit tests for the building blocks — these pass on a fresh clone."""
from __future__ import annotations

import json

import duckdb
import pytest

from pipeline import config
from pipeline.checksum import query_checksum
from pipeline.dag import DAG
from pipeline.embed import EMBED_DIM, chunk_words, embed_text, text_hash
from pipeline.quality import validate_events
from pipeline.silver import ensure_tables


def test_days_between_and_shift():
    assert config.days_between("2026-08-10", "2026-08-12") == ["2026-08-10", "2026-08-11", "2026-08-12"]
    assert config.shift("2026-08-01", -3) == "2026-07-29"


def test_chunks_are_deterministic_and_overlap():
    text = " ".join(f"w{i}" for i in range(100))
    a, b = chunk_words(text, 40, 8), chunk_words(text, 40, 8)
    assert a == b and len(a) == 3
    assert a[0].split()[-8:] == a[1].split()[:8]           # 8-word overlap
    assert chunk_words("", 40, 8) == []


def test_embedding_is_stable_and_keyed_by_text():
    assert embed_text("hoàn tiền 30 ngày") == embed_text("hoàn tiền 30 ngày")
    assert len(embed_text("x")) == EMBED_DIM
    assert text_hash("a") != text_hash("a ")                 # any change -> new cache key


def test_mask_pii_macro():
    con = duckdb.connect()
    ensure_tables(con)
    masked = con.execute("""SELECT mask_pii('mail an.nguyen@gmail.com, gọi 0912 345 678
        hoặc +84 987 654 321; hoá đơn 199.000đ, 40MB, năm 2025')""").fetchone()[0]
    assert "<EMAIL>" in masked and masked.count("<PHONE>") == 2
    assert "199.000đ" in masked and "40MB" in masked and "2025" in masked


def test_checksum_ignores_order_but_not_duplicates():
    con = duckdb.connect()
    a = query_checksum(con, "SELECT * FROM (VALUES (1,'x'),(2,'y')) t(a,b)")
    b = query_checksum(con, "SELECT * FROM (VALUES (2,'y'),(1,'x')) t(a,b)")
    c = query_checksum(con, "SELECT * FROM (VALUES (1,'x'),(2,'y'),(2,'y')) t(a,b)")
    assert a == b and a != c


def test_dag_runs_in_topological_order():
    dag, seen = DAG(), []

    @dag.task("b", upstream=["a"])
    def _b(x):
        seen.append("b")
        return x + 1

    @dag.task("a")
    def _a():
        seen.append("a")
        return 1

    assert dag.run()["b"] == 2 and seen == ["a", "b"]


def test_dag_detects_cycles():
    dag = DAG()
    dag.task("a", upstream=["b"])(lambda x: x)
    dag.task("b", upstream=["a"])(lambda x: x)
    with pytest.raises(ValueError):
        dag.run()


def _rec(value, offset=0):
    return (json.dumps({"value": value}), 0, offset, None, "2026-08-13")


def test_quality_gate_splits_valid_and_bad():
    ok = {"event_id": "e1", "user_id": "u1", "type": "feedback", "rating": "down",
          "event_time": "2026-08-12T21:10:00Z"}
    bad_rating = {**ok, "event_id": "e2", "rating": "meh"}
    no_user = {**ok, "event_id": "e3", "user_id": None}
    click_with_rating = {**ok, "event_id": "e4", "type": "click"}
    valid, bad = validate_events([_rec(ok), _rec(bad_rating, 1), _rec(no_user, 2),
                                  _rec(click_with_rating, 3)])
    assert [v["event_id"] for v in valid] == ["e1"]
    assert [b["event_id"] for b in bad] == ["e2", "e3", "e4"]
    assert valid[0]["event_time"].tzinfo is None             # stored as naive UTC
    assert all(b["reason"] for b in bad)


def test_bronze_landing_is_idempotent(sandbox):
    from pipeline.bronze import land_batch

    con = duckdb.connect()
    first = land_batch(con, "events", "2026-08-13")
    second = land_batch(con, "events", "2026-08-13")
    assert first["status"] == "landed" and second["status"] == "already-landed"
    assert first["rows"] == second["rows"] == 7
