"""Paths for the extensions (independent of the graded pipeline)."""
from pathlib import Path

EXT_DIR = Path(__file__).resolve().parent
TRACES_JSON = EXT_DIR / "data" / "traces" / "agent_traces.json"
DOCS_DIR = EXT_DIR / "data" / "docs"
DATASETS_DIR = EXT_DIR.parent / "datasets"
EVAL_JSONL = DATASETS_DIR / "eval_golden.jsonl"
PREF_JSONL = DATASETS_DIR / "preference_pairs.jsonl"
