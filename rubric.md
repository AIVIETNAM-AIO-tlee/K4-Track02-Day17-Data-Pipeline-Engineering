# Lab 17 — Rubric (100 pts core + 20 bonus)

Maps 1-to-1 to the Day 17 deck: Bronze/Silver/Gold commitments, MERGE on keys,
CDC deletes, late data + lookback, point-in-time & versioned snapshots, dbt
merge/microbatch, and the deck's final test — *re-run an old day three times,
three identical checksums*. Track-2 Daily Lab weight = 30%.

Everything is verifiable zero-key. Evidence = `make verify`, `make test`,
`make rerun3` (`submission/checksums.txt`), `make parity`, and `submission/REPORT.md`.

**Hard rule:** editing `verify.py`, `tests/`, `data/`, `rerun_check.py` or
`pipeline/checksum.py` to make a check pass = 0 for the affected criteria.

| # | Criterion | Evidence | Pts |
|---|---|---|---:|
| 1 | **Bug — Silver key.** `silver_tickets` holds one row per `ticket_id`, written with a keyed upsert; re-running an **older** batch never overwrites a newer state | verify Silver checks + `rerun3` | 15 |
| 2 | **Bug — late data.** `LOOKBACK_DAYS` is set from the P99 lateness **measured from Bronze**; `gold_feature_daily` reconciles with a full recompute; u05's 08-12 events land on 08-12 | verify Gold checks, `make lateness` output in REPORT | 15 |
| 3 | **Bug — CDC deletes.** A Debezium delete becomes a tombstone in Silver (no personal data left) and propagates: gone from the latest training snapshot and from the RAG index | verify Silver/Gold checks | 15 |
| 4 | **Grading test.** `make rerun3` = PASS: fresh build and 3 re-runs of 2026-08-12 give the same Gold checksum; `submission/checksums.txt` committed | `submission/checksums.txt` | 15 |
| 5 | **All contracts.** `make verify` ALL PASS (18/18) and `make test` all green | pasted output | 10 |
| 6 | **dbt track.** `make dbt` PASS (merge + microbatch + data tests + unit test) and `make parity` = PARITY | pasted output | 10 |
| 7 | **REPORT.md (≤ 1 page).** For each bug: symptom (which check / checksum), root cause, fix, deck concept. Tool choices justified (why MERGE vs overwrite-partition, why tombstone vs hard delete, why lookback = that number). Answers to the 2 reflection questions | `submission/REPORT.md` | 20 |
|   | **Core total** | | **100** |

### What a full-mark REPORT shows

- It names the **symptom you saw first** (e.g. "24 rows for 12 tickets", or the
  checksum table), not just the fix.
- It explains the fix **in terms of the deck**: key + MERGE, LSN guard, tombstone,
  lookback = ceil(P99), "xoá phải lan".
- It is honest about trade-offs (e.g. tombstones keep a row forever; old training
  snapshots still contain T-97's text — what would you do about that?).

## Bonus (optional, +20)

| Criterion | Evidence | Pts |
|---|---|---:|
| **B1 — LLM step.** `make bonus-llm` prints `BONUS PASS`: cache key = hash(input)+model+prompt version, 0 calls on re-run, re-label on prompt change, off-schema answers quarantined, cost estimated before running | output + diff of `pipeline/llm_label.py` | 10 |
| **B2 — pick one:** (a) the daily run on Airflow 3 via `make docker-up` + `airflow backfill create`, screenshot of 7 successful runs and the final checksum; **or** (b) `bonus/DESIGN.md` from [`BONUS-CHALLENGE.md`](BONUS-CHALLENGE.md) (≥ 600 words, 4–6 decisions with explicit trade-offs, one rejected alternative, an architecture sketch) | screenshots or `bonus/DESIGN.md` | 10 |
|   | **Bonus total** | | **20** |

Missing the bonus never lowers your core grade.

## Submission

**No PR. Submit a public GitHub URL into the VinUni LMS Day-17 box.**

1. Push to `<your-username>/Track02-Day17-Data-Pipeline-Engineering` (fork or fresh, **public**).
2. Include `submission/checksums.txt`, `submission/REPORT.md` (with the pasted outputs),
   and optionally `bonus/`.
3. Keep it public until grades are released. Private = 0.

## Late policy / regrade

Standard Track-2 policy applies — see `INDEX-Track2.md`.
