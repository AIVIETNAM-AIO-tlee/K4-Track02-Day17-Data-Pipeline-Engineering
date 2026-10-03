# K4-Track02-Day17 — Checkpoints

Bài cá nhân, thời lượng gợi ý **150 phút**. Mỗi checkpoint cần có sản phẩm,
hiểu được logic và tự kiểm tra bằng công cụ của repo.
Lệnh PowerShell tương đương được ghi trong [SUBMISSION.md](SUBMISSION.md).

## CP1 — Đọc đề và dựng baseline (20 phút)

**Làm gì:** đọc README, RUBRIC và RULES; cài dependencies; chạy pipeline, verify
và pytest trên bản chưa sửa. Đo lateness sau khi Bronze đã được land.

**Sản phẩm:** ghi lại các check fail, triệu chứng và P50/P95/P99 lateness trong ghi chú REPORT.

**Cần hiểu:** ba nguồn dữ liệu, các tầng Bronze/Silver/Gold và việc fail trên seed là chủ đích.

**Tự kiểm tra:** `make run`, `make verify`, `make test`, `make lateness`.
Đọc được ví dụ T-91, T-97 và event đến muộn của u05 trong seed.

## CP2 — Sửa khoá Silver (25 phút)

**Làm gì:** sửa cách ghi ticket để mỗi `ticket_id` có một hàng và batch cũ
không ghi đè trạng thái mới. Dùng thông tin thứ tự thay đổi của CDC để quyết định cập nhật.

**Sản phẩm:** thay đổi trong `pipeline/`, ghi triệu chứng, nguyên nhân và giải thích trong REPORT.

**Cần hiểu:** dedup trong một batch khác với upsert giữa các batch; vai trò của khoá và LSN.

**Tự kiểm tra:** `make test`; các test về ticket duy nhất và trạng thái mới nhất phải pass.
T-91 có trạng thái cuối `high / closed / bug`. Các check late data và delete có thể còn fail.

## CP3 — Sửa dữ liệu đến muộn (25 phút)

**Làm gì:** đo P99 từ Bronze; đặt lookback phù hợp và kiểm tra feature theo event time.

**Sản phẩm:** cấu hình lookback đã sửa, số đo lateness và lý do chọn cửa sổ trong REPORT.

**Cần hiểu:** event time khác ingest time; vì sao recompute partition giúp nhận event đến muộn.

**Tự kiểm tra:** `make lateness`, `make test` và `make verify`.
Feature phải khớp full recompute; u05 ngày `2026-08-12` có 5 events, 3 clicks và 1 feedback down.

## CP4 — Sửa CDC delete và chứng minh rerun (25 phút)

**Làm gì:** xử lý delete khi `after = null`, lưu tombstone ở Silver và truyền thao tác xoá
đến training snapshot mới nhất và RAG index. Sau khi sửa đủ ba lỗi, chạy rerun ba lần.

**Sản phẩm:** thay đổi xử lý delete; `submission/checksums.txt` đạt `PASS`.

**Cần hiểu:** CDC delete khác Kafka tombstone; giữ LSN giúp chống hồi sinh ticket khi replay.
Snapshot cũ có chủ đích bất biến; bài lab yêu cầu loại ticket khỏi snapshot mới nhất và RAG.

**Tự kiểm tra:** `make verify`, `make test`, `make rerun3`.
Verify đạt 18/18; T-97 là tombstone không còn user, subject, body ở Silver;
không có T-97 trong snapshot mới nhất và chunks. C0 = C1 = C2 = C3.

## CP5 — dbt và parity (25 phút)

**Làm gì:** cài dependencies dbt, build từ cùng Bronze rồi so checksum với pipeline Python.

**Sản phẩm:** output dbt build thành công và parity trong REPORT.

**Cần hiểu:** keyed merge, LSN guard, microbatch/lookback và data contract trong dbt.
Parity chỉ so hai bảng chung: `silver_tickets` và `gold_feature_daily`.

**Tự kiểm tra:** `make setup-dbt`, `make dbt`, `make parity`; kết quả cuối là `PARITY`.

## CP6 — Hoàn thiện bài nộp (30 phút)

**Làm gì:** viết REPORT theo mẫu, trả lời hai câu hỏi suy ngẫm, kiểm tra tên repo,
commit, push và nộp URL cá nhân trên LMS.

**Sản phẩm:** repo public đúng tên, REPORT đầy đủ và checksum có thể tái lập.

**Cần hiểu:** giải thích được từng thay đổi, hạn chế của regex PII và đánh đổi của snapshot bất biến.

**Tự kiểm tra:** hoàn thành checklist trong [SUBMISSION.md](SUBMISSION.md).
Bonus nằm ngoài 150 phút gợi ý; xem [RUBRIC.md](RUBRIC.md) nếu làm thêm.
