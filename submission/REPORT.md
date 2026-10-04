# K4-Track02-Day17 — Report cá nhân

Phần phân tích tối đa một trang, không tính output ở phần 5.
Định dạng tham chiếu và phạm vi tính trang: [SUBMISSION.md](../docs/SUBMISSION.md).

**Họ tên / MSSV:** Lê Quang Thành / 2A202602647
**Repo:** https://github.com/AIVIETNAM-AIO-tlee/K4-Track02-Day17-Data-Pipeline-Engineering
**Commit bài nộp:** fbf74549564cf474f0b59b1452db9dd4b07cd131
**AI đã dùng và phạm vi hỗ trợ (hoặc không dùng):** Antigravity AI — Hỗ trợ rà soát mã nguồn, xác định nguyên nhân gốc 3 lỗi pipeline, sửa logic MERGE, cấu hình lookback, parse CDC delete và kiểm chứng parity dbt.
**Nguồn tham khảo khác (nếu có):** Slide K4 Track 02 Day 17 (Data Pipeline Engineering), Debezium CDC Connector Docs, dbt Microbatch & Incremental Models Documentation.

## 1. Ba lỗi

Mỗi lỗi 4 dòng. Triệu chứng = thứ bạn *thấy* đầu tiên (check nào fail, số nào lạ,
checksum nào lệch) — không phải cách sửa.

| | Lỗi Silver | Lỗi late data | Lỗi xoá (CDC) |
|---|---|---|---|
| **Triệu chứng** | `silver_tickets has exactly one row per ticket_id` FAIL (24 rows for 12 tickets); `T-91 shows its latest state` FAIL (nhận 3 dòng trạng thái cũ và mới). | `gold_feature_daily reconciles with a full recompute` FAIL; check u05 ngày 12/08 FAIL (got `(2, 0)`, expected `(5, 1)`); `LOOKBACK_DAYS covers measured P99` FAIL (`0 < 3`). | `deleted ticket T-97 is a tombstone` FAIL; T-97 vẫn còn trong training snapshot mới nhất (1 row) và RAG index `gold_doc_chunks` (2 chunks). |
| **Nguyên nhân gốc** | `upsert_silver_tickets` trong `pipeline/silver.py` dùng `INSERT INTO` thuần, làm nhân bản dòng qua các batch và không cập nhật trạng thái theo LSN. | `pipeline/config.py` đặt `LOOKBACK_DAYS = 0`, giả định dữ liệu đến tức thì nên không recompute lại partition ngày 12/08 khi nhận event muộn vào ngày 15/08. | `ticket_changes_sql` trong `pipeline/staging.py` chỉ trích xuất `ticket_id` từ `after`. Bản ghi CDC delete (`op = 'd'`) có `after = null` nên `ticket_id` bị null và bị loại bỏ. |
| **Cách sửa** (file, vài dòng) | Sửa `pipeline/silver.py`: dùng `MERGE INTO silver_tickets AS t USING _latest_changes AS s ON t.ticket_id = s.ticket_id WHEN MATCHED AND s._lsn > t._lsn THEN UPDATE ... WHEN NOT MATCHED THEN INSERT ...`. | Sửa `pipeline/config.py`: đổi `LOOKBACK_DAYS = 3` (dựa trên đo đạc P99 lateness từ Bronze: P99 = 3.00 ngày, `ceil(P99) = 3`). | Sửa `pipeline/staging.py`: trích xuất `ticket_id` bằng `coalesce(j->'value'->'after'->>'ticket_id', j->'value'->'before'->>'ticket_id') AS ticket_id`. |
| **Khái niệm trên slide** | Silver — Có khoá; Bốn cách viết idempotent (Keyed Upsert/MERGE kết hợp LSN ordering guard). | Data về muộn; Event time vs Ingest time; Cửa sổ Lookback & Overwrite-Partition. | CDC log-based (Debezium envelope before/after); Xoá phải lan (Delete propagation & Tombstones). |

## 2. Các con số

- P99 lateness đo từ Bronze: `3.00` ngày → `LOOKBACK_DAYS = 3`
- `submission/checksums.txt`: PASS — Gold checksum: `39e115c510ecdf526800eac227158a4f`
- `make parity`: PARITY

## 3. Lựa chọn công cụ / kỹ thuật (mỗi dòng một câu "vì sao")

- MERGE theo khoá cho `silver_tickets`, overwrite-partition cho `gold_feature_daily`: `silver_tickets` là bảng thực thể có khoá duy nhất thay đổi qua CDC stream cần MERGE kèm LSN guard để idempotent; `gold_feature_daily` là bảng aggregate theo ngày nên overwrite phân vùng theo cửa sổ lookback đơn giản, hiệu quả và tự nhiên.
- Tombstone thay vì xoá hẳn hàng trong Silver: Giữ tombstone với LSN mới nhất giúp ghi nhận trạng thái đã xoá và ngăn chặn việc replay các batch CDC cũ vô tình "hồi sinh" (resurrect) lại ticket.
- Snapshot training dựng lại từ Bronze "as of" ngày đó, không sửa snapshot cũ: Đảm bảo tính bất biến (immutability) và khả năng tái lập (reproducibility) cho ML, ngăn chặn rò rỉ dữ liệu tương lai (data leakage).
- DuckDB (lite) / dbt (track dbt) cho bài toán cỡ này, chứ không phải Spark: Quy mô dữ liệu vừa vặn trên máy đơn; DuckDB là in-process engine vector hóa cực nhanh, zero-overhead, còn dbt quản lý lineage và testing chuẩn xác mà không tốn chi phí cụm/shuffle của Spark.

## 4. Hai câu hỏi suy ngẫm

1. Snapshot `v2026-08-12`..`v2026-08-14` vẫn chứa văn bản của T-97 (đã bị xoá ngày
   08-15). "Snapshot bất biến" và "quyền được xoá dữ liệu" mâu thuẫn — bạn xử lý thế nào?
   - **Xử lý:** Áp dụng kết hợp hai cơ chế: (a) *Pseudonymization / Crypto-shredding*: mã hóa PII bằng key riêng cho từng người dùng trước khi đưa vào snapshot; khi nhận yêu cầu xóa dữ liệu, chỉ cần hủy khóa mã hóa tương ứng thì văn bản trong snapshot cũ tự động trở thành dữ liệu vô danh (anonymized) không thể giải mã, vừa bảo đảm tuân thủ GDPR/quyền được xóa vừa giữ nguyên cấu trúc snapshot; (b) *Compliance scrub versioning*: khi bắt buộc phải xóa cứng văn bản thô theo yêu cầu pháp lý, chạy job tuân thủ tạo snapshot hiệu chỉnh (ví dụ `v2026-08-12-purged`) thay thế bản cũ, ghi nhận lý do vào metadata audit lineage của mô hình để phục vụ kiểm toán.
2. Regex che được email và số điện thoại, nhưng tên "Nguyễn Văn An" vẫn còn. Bạn sẽ
   đặt chốt PII nào, ở tầng nào, và đo nó ra sao?
   - **Xử lý:**
     - *Vị trí tầng:* Đặt chốt tại tầng Staging / Silver Ingestion Gate (ngay trước khi ghi dữ liệu vào Silver) để PII không bao giờ rò rỉ xuống Silver, Gold, Training snapshot hay RAG index.
     - *Phương pháp:* Tích hợp mô hình NER (Named Entity Recognition) chuyên dụng cho tiếng Việt (như PhoBERT-NER hoặc presidio-analyzer với custom Vietnamese recognizers) để nhận diện thực thể tên người (`<PERSON>`), tổ chức (`<ORG>`), địa chỉ (`<LOC>`).
     - *Cách đo:* Xây dựng bộ dữ liệu đánh giá chuẩn (Golden Evaluation Set) có gán nhãn PII; đo lường tự động qua CI/CD bằng chỉ số Recall và Precision, trong đó ưu tiên Recall đạt xấp xỉ 100% để giảm thiểu tối đa tỉ lệ rò rỉ (`PII Leakage Rate = 1 - Recall`); kết hợp quét kiểm tra ngẫu nhiên (Audit Scan) định kỳ trên Silver.

## 5. Output (dán nguyên văn)

### Lệnh chạy trên Windows PowerShell:

```powershell
# 1. verify
$env:PYTHONUTF8='1'; .\.venv\Scripts\python.exe -m scripts.verify
=== verify.py — Day 17 pipeline contracts ===
  [OK ] Bronze  every daily batch landed as Parquet (7 days x 3 sources)
  [OK ] Bronze  re-landing a batch is a no-op (append-only, no duplicate file)
  [OK ] Bronze  Bronze keeps the raw truth: Kafka tombstone + redelivered events are still there
  [OK ] Silver  silver_tickets has exactly one row per ticket_id
  [OK ] Silver  T-91 shows its latest state: high / closed / bug
  [OK ] Silver  deleted ticket T-97 is a tombstone: is_deleted and no personal data left
  [OK ] Silver  no email / phone number survives past Bronze
  [OK ] Silver  silver_events has one row per event_id (Kafka redeliveries removed)
  [OK ] Silver  2 malformed events quarantined with a reason; the run did not halt
  [OK ] Gold    gold_feature_daily reconciles with a full recompute from Silver
  [OK ] Gold    u05's offline events of 08-12 (arrived 08-15) are counted on 08-12
  [OK ] Gold    LOOKBACK_DAYS covers measured P99 lateness (p99=3.00 days)
  [OK ] Gold    training set uses point-in-time priority (T-91 created as 'low')
  [OK ] Gold    late feedback creates a NEW snapshot version; the old one is untouched
  [OK ] Gold    latest training snapshot excludes the deleted ticket T-97
  [OK ] Gold    deletes propagate to the RAG index: no chunk of T-97
  [OK ] Gold    gold_doc_chunks: one row per chunk, and a re-run embeds 0 new chunks
  [OK ] Rerun   re-run 2026-08-12 three times -> Gold checksum identical to a fresh build

RESULT: 18/18 checks — ALL PASS
re-run checksums written to submission/checksums.txt

# 2. test
$env:PYTHONUTF8='1'; .\.venv\Scripts\python.exe -m pytest
..................................                                       [100%]
34 passed in 2.45s

# 3. rerun3
$env:PYTHONUTF8='1'; .\.venv\Scripts\python.exe -m scripts.rerun_check
# Lab 17 — re-run check for 2026-08-12

run                     gold_feature_daily    gold_training_set     gold_doc_chunks       gold (combined)
fresh build             8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #1 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #2 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f
re-run #3 of 2026-08-12 8630e04a61d1          9370ca77af23          cb9ebd12fdcc          39e115c510ecdf526800eac227158a4f

RESULT: PASS — 3 re-runs, identical checksums

# 4. lateness
$env:PYTHONUTF8='1'; .\.venv\Scripts\python.exe main.py --lateness
event lateness over 43 Bronze records (calendar days): p50=0.00 p95=2.90 p99=3.00 max=3
-> lookback must be >= ceil(p99) = 3 day(s); config.LOOKBACK_DAYS = 3

# 5. dbt build
$env:PYTHONUTF8='1'; .\.venv\Scripts\python.exe main.py --land-only; $env:DO_NOT_TRACK = '1'; Push-Location dbt_project; try { ..\.venv\Scripts\dbt.exe build --profiles-dir . --event-time-start 2026-08-10 --event-time-end 2026-08-17 } finally { Pop-Location }
05:40:04  Running with dbt=1.12.5
05:40:04  Registered adapter: duckdb=1.11.0
05:40:05  Found 5 models, 13 data tests, 2 sources, 502 macros, 1 unit test
05:40:05  
05:40:05  Concurrency: 1 threads (target='dev')
05:40:05  
05:40:05  1 of 19 START sql view model main.stg_events ................................... [RUN]
05:40:05  1 of 19 OK created sql view model main.stg_events .............................. [OK in 0.07s]
05:40:05  2 of 19 START sql view model main.stg_ticket_changes ........................... [RUN]
05:40:05  2 of 19 OK created sql view model main.stg_ticket_changes ...................... [OK in 0.02s]
05:40:05  3 of 19 START sql incremental model main.silver_events ......................... [RUN]
05:40:05  3 of 19 OK created sql incremental model main.silver_events .................... [OK in 0.10s]
05:40:05  4 of 19 START unit_test silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [RUN]
05:40:05  4 of 19 PASS silver_tickets::silver_tickets_latest_change_wins_and_delete_is_tombstone  [PASS in 0.09s]
05:40:05  8 of 19 START sql incremental model main.silver_tickets ........................ [RUN]
05:40:05  8 of 19 OK created sql incremental model main.silver_tickets ................... [OK in 0.13s]
05:40:05  5 of 19 START test not_null_silver_events_event_id ............................. [RUN]
05:40:05  5 of 19 PASS not_null_silver_events_event_id ................................... [PASS in 0.03s]
05:40:05  6 of 19 START test not_null_silver_events_user_id .............................. [RUN]
05:40:05  6 of 19 PASS not_null_silver_events_user_id .................................... [PASS in 0.01s]
05:40:05  7 of 19 START test unique_silver_events_event_id ............................... [RUN]
05:40:05  7 of 19 PASS unique_silver_events_event_id ..................................... [PASS in 0.02s]
05:40:05  9 of 19 START test accepted_values_silver_tickets_category__bug__billing__other  [RUN]
05:40:05  9 of 19 PASS accepted_values_silver_tickets_category__bug__billing__other ...... [PASS in 0.03s]
05:40:05  10 of 19 START test accepted_values_silver_tickets_priority__low__medium__high . [RUN]
05:40:05  10 of 19 PASS accepted_values_silver_tickets_priority__low__medium__high ....... [PASS in 0.03s]
05:40:05  11 of 19 START test accepted_values_silver_tickets_status__open__pending__closed  [RUN]
05:40:05  11 of 19 PASS accepted_values_silver_tickets_status__open__pending__closed ..... [PASS in 0.02s]
05:40:05  12 of 19 START test not_null_silver_tickets__lsn ............................... [RUN]
05:40:05  12 of 19 PASS not_null_silver_tickets__lsn ..................................... [PASS in 0.01s]
05:40:05  13 of 19 START test not_null_silver_tickets_is_deleted ......................... [RUN]
05:40:05  13 of 19 PASS not_null_silver_tickets_is_deleted ............................... [PASS in 0.01s]
05:40:05  14 of 19 START test not_null_silver_tickets_ticket_id .......................... [RUN]
05:40:06  14 of 19 PASS not_null_silver_tickets_ticket_id ................................ [PASS in 0.01s]
05:40:06  15 of 19 START test unique_silver_tickets_ticket_id ............................ [RUN]
05:40:06  15 of 19 PASS unique_silver_tickets_ticket_id .................................. [PASS in 0.02s]
05:40:06  16 of 19 START sql microbatch model main.gold_feature_daily .................... [RUN]
05:40:06  Batch 1 of 7 START batch 2026-08-10 of main.gold_feature_daily ....................... [RUN]
05:40:06  Batch 1 of 7 OK created batch 2026-08-10 of main.gold_feature_daily .................. [OK in 0.04s]
05:40:06  Batch 2 of 7 START batch 2026-08-11 of main.gold_feature_daily ....................... [RUN]
05:40:06  Batch 2 of 7 OK created batch 2026-08-11 of main.gold_feature_daily .................. [OK in 0.02s]
05:40:06  Batch 3 of 7 START batch 2026-08-12 of main.gold_feature_daily ....................... [RUN]
05:40:06  Batch 3 of 7 OK created batch 2026-08-12 of main.gold_feature_daily .................. [OK in 0.02s]
05:40:06  Batch 4 of 7 START batch 2026-08-13 of main.gold_feature_daily ....................... [RUN]
05:40:06  Batch 4 of 7 OK created batch 2026-08-13 of main.gold_feature_daily .................. [OK in 0.02s]
05:40:06  Batch 5 of 7 START batch 2026-08-14 of main.gold_feature_daily ....................... [RUN]
05:40:06  Batch 5 of 7 OK created batch 2026-08-14 of main.gold_feature_daily .................. [OK in 0.02s]
05:40:06  Batch 6 of 7 START batch 2026-08-15 of main.gold_feature_daily ....................... [RUN]
05:40:06  Batch 6 of 7 OK created batch 2026-08-15 of main.gold_feature_daily .................. [OK in 0.02s]
05:40:06  Batch 7 of 7 START batch 2026-08-16 of main.gold_feature_daily ....................... [RUN]
05:40:06  Batch 7 of 7 OK created batch 2026-08-16 of main.gold_feature_daily .................. [OK in 0.02s]
05:40:06  16 of 19 OK created sql microbatch model main.gold_feature_daily ............... [SUCCESS in 0.18s]
05:40:06  17 of 19 START test dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [RUN]
05:40:06  17 of 19 PASS dbt_utils_free_unique_combination_gold_feature_daily_user_id__event_date  [PASS in 0.02s]
05:40:06  18 of 19 START test not_null_gold_feature_daily_event_date ..................... [RUN]
05:40:06  18 of 19 PASS not_null_gold_feature_daily_event_date ........................... [PASS in 0.01s]
05:40:06  19 of 19 START test not_null_gold_feature_daily_user_id ........................ [RUN]
05:40:06  19 of 19 PASS not_null_gold_feature_daily_user_id .............................. [PASS in 0.01s]
05:40:06  
05:40:06  Finished running 3 incremental models, 13 data tests, 1 unit test, 2 view models in 0 hours 0 minutes and 0.99 seconds (0.99s).
05:40:06  
05:40:06  Completed successfully
05:40:06  
05:40:06  Done. PASS=19 WARN=0 ERROR=0 SKIP=0 NO-OP=0 REUSED=0 TOTAL=19

# 6. parity
$env:PYTHONUTF8='1'; .\.venv\Scripts\python.exe -m scripts.parity
=== parity: lite pipeline vs dbt ===
  [OK ] silver_tickets       lite 3c15dfd43701  dbt 3c15dfd43701
  [OK ] gold_feature_daily   lite 8630e04a61d1  dbt 8630e04a61d1
RESULT: PARITY — both implementations agree
```
