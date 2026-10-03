# Lab 17 — Data Pipeline Engineering (Track 2)

> 🇬🇧 English version: [`README_en.md`](README_en.md)

Dựng đường ống dữ liệu cho **nền tảng AI hỗ trợ khách hàng** — đúng bài toán
xuyên suốt của slide Ngày 17 — rồi **sửa ba lỗi đã cài sẵn** và chứng minh
pipeline chạy lại được bằng **checksum**.

```
Postgres tickets ── Debezium CDC ──┐
Kafka support.events ──────────────┼─▶ Bronze ─────────▶ Silver ──────────────┬─▶ gold_doc_chunks    → RAG index
S3 transcripts (JSON) ─────────────┘   Parquet,          MERGE theo khoá,     ├─▶ gold_training_set  → classifier
                                       bất biến          PII đã che           └─▶ gold_feature_daily → routing agent
```

Mọi thứ chạy **zero-key, đa nền tảng** trên DuckDB + Python. Không cần Docker,
không cần cloud. Track dbt dùng chính Bronze đó.

---

## Nhiệm vụ (2,5 giờ)

Repo này **cố tình có 3 lỗi**. Clone về chạy `make verify` sẽ thấy `FAILURES`.

1. Chạy pipeline, đọc các check bị fail, **tìm 3 lỗi** trong `pipeline/`.
2. **Sửa** chúng — không sửa `verify.py`, `tests/`, `data/` hay cách tính checksum.
3. Chứng minh: `make rerun3` → chạy lại ngày **2026-08-12 ba lần**, ba checksum Gold
   **giống hệt nhau và giống bản build mới** (`submission/checksums.txt`).
4. Track dbt: `make dbt` pass, `make parity` → hai cách cài đặt cho cùng một checksum.
5. Viết `submission/REPORT.md` (≤ 1 trang): mỗi lỗi — triệu chứng, nguyên nhân gốc,
   cách sửa, khái niệm trên slide; mỗi lựa chọn công cụ — vì sao.

Gợi ý phân bổ: 20' đọc code + chạy · 75' ba lỗi · 25' dbt · 30' report.

---

## Bắt đầu nhanh

```bash
make setup        # tạo .venv + cài requirements.txt
make run          # build mới: reset Silver/Gold, backfill 08-10 .. 08-16 từ Bronze
make verify       # 18 contract — bản clone về sẽ FAIL, đó chính là bài lab
make test         # pytest
make rerun3       # BÀI KIỂM TRA CHẤM ĐIỂM: chạy lại 2026-08-12 ba lần
make lateness     # đo độ trễ của event từ Bronze (P50 / P95 / P99)
```

Không dùng `make` (Windows):

```bash
python -m venv .venv && .venv\Scripts\activate        # macOS/Linux: . .venv/bin/activate
pip install -r requirements.txt
python main.py && python verify.py && python rerun_check.py && pytest
python main.py --date 2026-08-14        # chạy một ngày trên warehouse hiện có
python main.py --lateness
```

Python **3.10+** cho đường lite. Track dbt đã test với Python 3.13.

---

## Có gì trong repo

| File | Tầng | Làm gì | Slide |
|---|---|---|---|
| `data/` | nguồn | Thứ các hệ thống nguồn giao mỗi ngày: CDC Debezium (dạng Kafka record), Kafka events, transcript export | Bài toán xuyên suốt |
| `pipeline/bronze.py` | Bronze | Land mỗi (nguồn, ngày) thành **một file Parquet bất biến**; land lại = không làm gì | Bronze — cam kết |
| `pipeline/staging.py` | Bronze→ | Đọc đúng **phong bì Debezium** (`before`/`after`/`op`/`lsn`, tombstone) | CDC log-based |
| `pipeline/quality.py` | gate | **Pydantic** kiểm từng event; sai → `quarantine_events`, không dừng run | Kiểm thử dữ liệu |
| `pipeline/silver.py` | Silver | `silver_tickets` (**MERGE** theo `ticket_id`), `silver_ticket_history` (**SCD2**), `silver_events`, `silver_transcripts`, che PII | Silver — có khoá |
| `pipeline/gold.py` | Gold | `gold_feature_daily` (theo **event time**, có **lookback**), `gold_training_set` (**snapshot có version**, point-in-time), `gold_doc_chunks` (cache embedding theo **hash + model version**) | Gold — đúng hình dạng |
| `pipeline/run.py`, `pipeline/dag.py` | điều phối | Một DAG cho một ngày; backfill = **cùng code path**, từng ngày | Chạy lại & backfill |
| `pipeline/checksum.py` | chấm | Checksum không phụ thuộc thứ tự dòng (SQL thuần, tự chạy được trong DuckDB CLI) | Bài kiểm tra cuối cùng |
| `rerun_check.py` | chấm | Build mới → chạy lại một ngày cũ 3 lần → so checksum | Lab 17 |
| `dbt_project/` | dbt | Cùng Silver/Gold viết bằng dbt: `merge` + `merge_update_condition`, `microbatch` + `lookback`, contract, unit test | dbt, Microbatch |
| `docker/` | bonus | Cùng daily run trên **Airflow 3** thật (`airflow.sdk`, `airflow backfill create`) | Airflow 2 → 3 |
| `pipeline/llm_label.py` | bonus | Bước **LLM gán nhãn** ticket — bản ngây thơ, bạn thêm cache theo hash | LLM là một bước transform |
| `extensions/` | mở rộng | Flywheel trace → eval/DPO, knowledge graph (từ lab bản cũ, không chấm) | — |

---

## Dữ liệu seed: những câu chuyện được cài sẵn

Bảy ngày 2026-08-10 → 2026-08-16, đủ nhỏ để đọc bằng mắt
(`scripts/generate_seed.py` sinh lại toàn bộ):

- **T-91** tạo `low/open` ngày 08-10 → `high` ngày 08-14 → `closed/bug` ngày 08-16
  (đúng ví dụ trên slide Silver). Thay đổi ngày 08-14 bị Kafka **giao hai lần**.
- **T-97** chứa tên, email, số điện thoại; đóng ngày 08-12; **bị xoá** ngày 08-15
  (yêu cầu xoá dữ liệu). Debezium gửi `op = 'd'` với `after = null`, rồi một tombstone.
- **u05** ở trên tàu, mất mạng tối 08-12: click và một 👎 cho T-88 tới Kafka
  **ngày 08-15** — trễ 3 ngày.
- Ngày 08-13 consumer khởi động lại: 2 event bị giao lại. Có 2 event hỏng
  (rating `meh`, thiếu `user_id`).

---

## Bài kiểm tra chấm điểm: ba checksum

Slide: *"Chạy lại một ngày cũ ba lần liên tiếp, ghi checksum bảng Gold sau mỗi lần.
Ba con số phải giống hệt nhau."* `make rerun3` làm đúng như vậy, và chặt hơn một bước:

```
fresh build             C0   ← reset Silver/Gold, backfill mọi ngày từ Bronze
re-run #1 of 2026-08-12 C1
re-run #2 of 2026-08-12 C2
re-run #3 of 2026-08-12 C3   PASS ⇔ C0 = C1 = C2 = C3
```

Vì sao phải bằng **C0** chứ không chỉ C1 = C2 = C3? Một pipeline có thể "ổn định
sai": lần chạy lại đầu tiên làm hỏng dữ liệu, các lần sau hỏng y như vậy. Ba con số
giống nhau mà khác bản build mới vẫn là FAIL.

---

## Gợi ý khi bí (mở từng tầng, đừng mở hết một lúc)

<details><summary>Lỗi ở Silver — <code>silver_tickets</code> có nhiều hàng cho một ticket</summary>

Đọc lại slide *"Silver — Có khoá"* và *"Bốn cách viết idempotent"*. Một hàng = một
thực thể cần **khoá**. Rồi hỏi tiếp: chạy lại batch **cũ** sau batch **mới** thì
trạng thái nào phải thắng? Cột nào cho bạn biết thay đổi nào mới hơn?
</details>

<details><summary>Lỗi ở Gold — <code>gold_feature_daily</code> không khớp full recompute</summary>

Chạy `make lateness`. Slide *"Data về muộn"*: lookback đặt bằng P99 của
`(_ingested_at − event_time)` — **đo từ Bronze, đừng đoán**.
</details>

<details><summary>Lỗi xoá — T-97 vẫn còn ở Silver, training set, RAG index</summary>

Mở `data/cdc/tickets/2026-08-15.jsonl` và nhìn bản ghi `op = "d"`. Khoá của ticket
nằm ở đâu khi `after` là `null`? Slide *"CDC log-based"* và *"Xoá phải lan"*.
</details>

---

## Track dbt (có chấm)

```bash
make setup-dbt
make dbt          # land Bronze → dbt build: PASS=19 (models + data tests + unit test)
make parity       # silver_tickets + gold_feature_daily: lite vs dbt cùng checksum
```

`dbt_project/` viết lại Silver/Gold theo đúng slide: `silver_tickets` là
`incremental_strategy='merge'` với `unique_key` và `merge_update_condition` theo LSN,
`gold_feature_daily` là `microbatch` (`batch_size='day'`, `lookback=3`), có contract,
`data_tests:` và một **unit test** cho logic dedup + xoá. Nếu `make parity` báo
MISMATCH thì một trong hai bản đang sai — thường là bản bạn chưa sửa xong.

---

## Bonus (+20, không bắt buộc)

- **B1 — Bước LLM có cache** (+10): `pipeline/llm_label.py` gọi LLM cho mọi ticket
  ở mọi lần chạy và ghi bất cứ thứ gì model trả về. Làm `make bonus-llm` in
  `BONUS PASS`: khoá cache = hash(input) + model + prompt version, chạy lại 0 lần
  gọi, đổi prompt thì gắn nhãn lại có chủ đích, output sai schema → quarantine.
  Zero-key: `FakeLLM` thay cho model thật.
- **B2 — chọn một** (+10): chạy daily run trên **Airflow 3** (`make docker-up`, rồi
  `airflow backfill create ...`, chụp 7 run và checksum), **hoặc** phiên brainstorm
  bài toán thật trong [`BONUS-CHALLENGE.md`](BONUS-CHALLENGE.md).

## Mở rộng (không chấm)

`make flywheel` (agent traces → eval set + cặp DPO, decontamination, ASOF join) và
`make kg` (knowledge graph vs vector retrieval) — phần còn giá trị của lab bản cũ, xem
[`extensions/README.md`](extensions/README.md).

---

## Nộp bài

Xem [`rubric.md`](rubric.md) (100 lõi + 20 bonus). Nộp **một URL GitHub public**
vào ô LMS Ngày 17 — không PR. Repo cần có:

- code đã sửa (diff của 3 lỗi đọc được trong lịch sử commit),
- `submission/checksums.txt` — sinh ra bởi `make rerun3`, phải là `PASS`,
- `submission/REPORT.md` — báo cáo ≤ 1 trang,
- output của `make verify`, `make test`, `make parity` (dán vào cuối REPORT),
- tuỳ chọn: phần bonus.

Mới làm việc cùng AI coding agent? Đọc [`VIBE-CODING.md`](VIBE-CODING.md) trước —
và nhớ: bạn phải giải thích được từng dòng mình sửa trong REPORT.

Định dạng bảng lakehouse mà Bronze/Gold sẽ hạ cánh là **Ngày 18**; feature store /
vector DB mà Gold nuôi là **Ngày 19**; observability và lineage cho pipeline này là
**Ngày 27**.
