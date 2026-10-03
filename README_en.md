# Lab 17 — Data Pipeline Engineering (Track 2)

> 🇻🇳 Vietnamese version (default): [`README.md`](README.md)

Build the data pipeline behind an **AI customer-support platform** — the running
example of the Day 17 deck — then **fix three planted bugs** and prove the pipeline
can be re-run, with **checksums**.

```
Postgres tickets ── Debezium CDC ──┐
Kafka support.events ──────────────┼─▶ Bronze ─────────▶ Silver ──────────────┬─▶ gold_doc_chunks    → RAG index
S3 transcripts (JSON) ─────────────┘   Parquet,          MERGE on keys,       ├─▶ gold_training_set  → classifier
                                       immutable         PII masked           └─▶ gold_feature_daily → routing agent
```

Everything runs **zero-key and cross-platform** on DuckDB + Python. No Docker, no
cloud. The dbt track reads the same Bronze.

---

## Your task (2.5 hours)

This repo **ships with 3 bugs on purpose**. A fresh clone prints `FAILURES` on `make verify`.

1. Run the pipeline, read the failing checks, **find the 3 bugs** in `pipeline/`.
2. **Fix** them — without editing `verify.py`, `tests/`, `data/` or the checksum logic.
3. Prove it: `make rerun3` re-runs **2026-08-12 three times**; the three Gold checksums
   must be **identical and equal to a fresh build** (`submission/checksums.txt`).
4. dbt track: `make dbt` passes and `make parity` shows both implementations agree.
5. Write `submission/REPORT.md` (≤ 1 page): for each bug — symptom, root cause, fix,
   deck concept; for each tool choice — why.

Suggested split: 20' read + run · 75' three bugs · 25' dbt · 30' report.

---

## Quick start

```bash
make setup        # create .venv + install requirements.txt
make run          # fresh build: reset Silver/Gold, backfill 08-10 .. 08-16 from Bronze
make verify       # 18 contracts — a fresh clone FAILS, that is the lab
make test         # pytest
make rerun3       # THE GRADING TEST: re-run 2026-08-12 three times
make lateness     # measure event lateness from Bronze (P50 / P95 / P99)
```

Without `make` (Windows):

```bash
python -m venv .venv && .venv\Scripts\activate        # macOS/Linux: . .venv/bin/activate
pip install -r requirements.txt
python main.py && python verify.py && python rerun_check.py && pytest
python main.py --date 2026-08-14        # one daily run on the existing warehouse
python main.py --lateness
```

Python **3.10+** for the lite path. The dbt track was tested on Python 3.13.

---

## What is in the repo

| File | Layer | What it does | Deck |
|---|---|---|---|
| `data/` | sources | What the source systems deliver each day: Debezium CDC (as Kafka records), Kafka events, transcript exports | Running example |
| `pipeline/bronze.py` | Bronze | Lands each (source, day) as **one immutable Parquet file**; re-landing is a no-op | Bronze commitment |
| `pipeline/staging.py` | Bronze→ | Reads the **Debezium envelope** correctly (`before`/`after`/`op`/`lsn`, tombstones) | Log-based CDC |
| `pipeline/quality.py` | gate | **Pydantic** checks every event; bad → `quarantine_events`, the run never halts | Data testing |
| `pipeline/silver.py` | Silver | `silver_tickets` (**MERGE** on `ticket_id`), `silver_ticket_history` (**SCD2**), `silver_events`, `silver_transcripts`, PII masking | Silver — keyed |
| `pipeline/gold.py` | Gold | `gold_feature_daily` (by **event time**, with **lookback**), `gold_training_set` (**versioned snapshots**, point-in-time), `gold_doc_chunks` (embedding cache keyed by **hash + model version**) | Gold — right shape |
| `pipeline/run.py`, `pipeline/dag.py` | orchestration | One DAG per day; backfill = **the same code path**, day by day | Safe re-runs & backfill |
| `pipeline/checksum.py` | grading | Row-order-independent checksum (plain SQL, runnable in the DuckDB CLI) | The final test |
| `rerun_check.py` | grading | Fresh build → re-run an old day 3× → compare checksums | Lab 17 |
| `dbt_project/` | dbt | The same Silver/Gold in dbt: `merge` + `merge_update_condition`, `microbatch` + `lookback`, contract, unit test | dbt, microbatch |
| `docker/` | bonus | The same daily run on real **Airflow 3** (`airflow.sdk`, `airflow backfill create`) | Airflow 2 → 3 |
| `pipeline/llm_label.py` | bonus | An **LLM labelling** step — naive; you add the hash cache | LLM as a transform |
| `extensions/` | extra | Trace → eval/DPO flywheel and knowledge graph (from the previous lab, ungraded) | — |

---

## Seed data: the planted stories

Seven days, 2026-08-10 → 2026-08-16, small enough to read by eye
(`scripts/generate_seed.py` regenerates all of it):

- **T-91** is created `low/open` on 08-10 → `high` on 08-14 → `closed/bug` on 08-16
  (the deck's Silver example). The 08-14 change is **delivered twice** by Kafka.
- **T-97** contains a name, an email and a phone number; closed on 08-12; **deleted**
  on 08-15 (erasure request). Debezium sends `op = 'd'` with `after = null`, then a tombstone.
- **u05** is offline on a train on the evening of 08-12: her clicks and a 👎 on T-88
  reach Kafka **on 08-15** — 3 days late.
- On 08-13 a consumer restart redelivers 2 events. 2 events are malformed
  (rating `meh`, missing `user_id`).

---

## The grading test: three checksums

Deck: *"Re-run an old day three times in a row, record the Gold checksum after each
run. The three numbers must be identical."* `make rerun3` does exactly that, one step stricter:

```
fresh build             C0   ← reset Silver/Gold, backfill every day from Bronze
re-run #1 of 2026-08-12 C1
re-run #2 of 2026-08-12 C2
re-run #3 of 2026-08-12 C3   PASS ⇔ C0 = C1 = C2 = C3
```

Why must they equal **C0**, not just each other? A pipeline can be "stably wrong":
the first re-run corrupts the data and every later re-run corrupts it the same way.
Three equal numbers that differ from a fresh build is still a FAIL.

---

## Hints if you are stuck (open one layer at a time)

<details><summary>Silver — <code>silver_tickets</code> has several rows per ticket</summary>

Re-read *"Silver — keyed"* and *"Four ways to write idempotent"*. One row = one
entity needs a **key**. Then: when an **old** batch is re-run after a **newer** one,
which state must win? Which column tells you which change is newer?
</details>

<details><summary>Gold — <code>gold_feature_daily</code> does not reconcile with a full recompute</summary>

Run `make lateness`. Deck *"Late data"*: set the lookback to the P99 of
`(_ingested_at − event_time)` — **measure it from Bronze, don't guess**.
</details>

<details><summary>Deletes — T-97 is still in Silver, the training set and the RAG index</summary>

Open `data/cdc/tickets/2026-08-15.jsonl` and look at the `op = "d"` record. Where is
the ticket's key when `after` is `null`? Deck *"Log-based CDC"* and *"Deletes must propagate"*.
</details>

---

## dbt track (graded)

```bash
make setup-dbt
make dbt          # land Bronze → dbt build: PASS=19 (models + data tests + unit test)
make parity       # silver_tickets + gold_feature_daily: lite vs dbt, same checksum
```

`dbt_project/` rewrites Silver/Gold the way the deck shows: `silver_tickets` is
`incremental_strategy='merge'` with a `unique_key` and an LSN `merge_update_condition`,
`gold_feature_daily` is `microbatch` (`batch_size='day'`, `lookback=3`), with a
contract, `data_tests:` and a **unit test** for the dedup + delete logic. If
`make parity` reports MISMATCH, one of the two implementations is wrong — usually
the one you have not finished fixing.

---

## Bonus (+20, optional)

- **B1 — An LLM step with a cache** (+10): `pipeline/llm_label.py` calls the LLM for
  every ticket on every run and stores whatever comes back. Make `make bonus-llm` print
  `BONUS PASS`: cache key = hash(input) + model + prompt version, a re-run makes 0
  calls, a new prompt re-labels on purpose, off-schema output → quarantine.
  Zero-key: `FakeLLM` stands in for a real model.
- **B2 — pick one** (+10): run the daily pipeline on **Airflow 3** (`make docker-up`,
  then `airflow backfill create ...`, screenshot 7 runs and the checksum), **or** the
  real-world brainstorm in [`BONUS-CHALLENGE-EN.md`](BONUS-CHALLENGE-EN.md).

## Extensions (ungraded)

`make flywheel` (agent traces → eval set + DPO pairs, decontamination, ASOF join) and
`make kg` (knowledge graph vs vector retrieval) — the parts of the previous lab worth
keeping; see [`extensions/README.md`](extensions/README.md).

---

## Submission

See [`rubric.md`](rubric.md) (100 core + 20 bonus). Submit **one public GitHub URL**
in the Day 17 LMS box — no PR. The repo must contain:

- your fixed code (the 3 fixes readable in the commit history),
- `submission/checksums.txt` — produced by `make rerun3`, must say `PASS`,
- `submission/REPORT.md` — the ≤ 1 page report,
- the output of `make verify`, `make test`, `make parity` (paste at the end of REPORT),
- optional: the bonus.

New to working with an AI coding agent? Read [`VIBE-CODING.md`](VIBE-CODING.md) first —
and remember: your REPORT must explain every line you changed.

The lakehouse table formats Bronze/Gold land in are **Day 18**; the feature store /
vector DB Gold feeds is **Day 19**; observability and lineage for this pipeline are
**Day 27**.
