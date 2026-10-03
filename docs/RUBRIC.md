# K4-Track02-Day17 — Rubric (100 điểm bắt buộc, bonus tối đa 10 điểm)

Maps 1-to-1 to the Day 17 deck: Bronze/Silver/Gold commitments, MERGE on keys,
CDC deletes, late data + lookback, point-in-time & versioned snapshots, dbt
merge/microbatch, and the deck's final test — *re-run an old day three times,
three identical checksums*. This is an individual K4 Track 02 assignment.

Everything is verifiable zero-key. Evidence = `make verify`, `make test`,
`make rerun3` (`submission/checksums.txt`), `make parity`, and `submission/REPORT.md`.

**Hard rule:** editing `scripts/verify.py`, `tests/`, `data/`, `scripts/rerun_check.py` or
`pipeline/checksum.py` to make a check pass = 0 for the affected criteria.

| # | Criterion | Evidence | Pts |
|---|---|---|---:|
| 1 | **Bug — Silver key.** `silver_tickets` holds one row per `ticket_id`, written with a keyed upsert; re-running an **older** batch never overwrites a newer state | verify Silver checks + `rerun3` | 15 |
| 2 | **Bug — late data.** `LOOKBACK_DAYS` is set from the P99 lateness **measured from Bronze**; `gold_feature_daily` reconciles with a full recompute; u05's 08-12 events land on 08-12 | verify Gold checks, `make lateness` output in REPORT | 15 |
| 3 | **Bug — CDC deletes.** A Debezium delete becomes a tombstone in `silver_tickets` (`user_id`, `subject`, `body` cleared) and propagates: gone from the latest training snapshot and from the RAG index | verify Silver/Gold checks | 15 |
| 4 | **Grading test.** `make rerun3` = PASS: fresh build and 3 re-runs of 2026-08-12 give the same Gold checksum; `submission/checksums.txt` committed | `submission/checksums.txt` | 15 |
| 5 | **All contracts.** `make verify` ALL PASS (18/18) and `make test` all green | pasted output | 10 |
| 6 | **dbt track.** `make dbt` PASS (merge + microbatch + data tests + unit test) and `make parity` = PARITY | pasted output | 10 |
| 7 | **REPORT.md (analysis ≤ 1 page, excluding command output).** For each bug: symptom (which check / checksum), root cause, fix, deck concept. Tool choices justified (why MERGE vs overwrite-partition, why tombstone vs hard delete, why lookback = that number). Answers to the 2 reflection questions | `submission/REPORT.md` | 20 |
|   | **Core total** | | **100** |

### What a full-mark REPORT shows

The one-page analysis limit uses the reference format in [SUBMISSION.md](SUBMISSION.md).
Airflow B2 instructions are in [AIRFLOW.md](AIRFLOW.md).

- It names the **symptom you saw first** (e.g. "24 rows for 12 tickets", or the
  checksum table), not just the fix.
- It explains the fix **in terms of the deck**: key + MERGE, LSN guard, tombstone,
  lookback = ceil(P99), "xoá phải lan".
- It is honest about trade-offs (e.g. tombstones keep a row forever; old training
  snapshots still contain T-97's text — what would you do about that?).

## Bonus (optional, up to +10)

| Criterion | Evidence | Pts |
|---|---|---:|
| **B1 — LLM step.** `make bonus-llm` prints `BONUS PASS`: cache key = hash(input)+model+prompt version, 0 calls on re-run, re-label on prompt change, off-schema answers quarantined, cost estimated before running | output + diff of `pipeline/llm_label.py` | 5 |
| **B2 — pick one:** (a) the daily run on Airflow 3 via `make docker-up` + `airflow backfill create`, screenshot of 7 successful runs and the final checksum; **or** (b) `bonus/DESIGN.md` from [`BONUS-CHALLENGE.md`](bonus/BONUS-CHALLENGE.md) (≥ 600 words, 4–6 decisions with explicit trade-offs, one rejected alternative, an architecture sketch) | screenshots or `bonus/DESIGN.md` | 5 |
|   | **Bonus total** | | **10** |

Missing the bonus never lowers your core grade. B1 and B2 together award at most
10 bonus points; the two B2 options do not stack. These points are for the lab,
separate from participation or pitching points.

## Submission

Each learner submits their own public repository URL in the K4 / Track 02 / Day 17
LMS assignment. Naming, required files and the deadline are in
[SUBMISSION.md](SUBMISSION.md). Private or inaccessible repositories cannot be graded.

## Late policy / regrade

See [RULES.md](RULES.md). Deadline adjustments, late penalties and any regrade
window are communicated by the key coach on LMS.
