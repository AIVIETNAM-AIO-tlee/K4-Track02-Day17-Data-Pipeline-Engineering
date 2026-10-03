# K4-Track02-Day17 — Bonus B2: Airflow

Đây là một lựa chọn của bonus B2 (+5), làm cá nhân. Cần Docker Engine chạy Linux
containers và Docker Compose. Hoàn thành ba lỗi của pipeline trước; DAG dùng chính
code trong `pipeline/`, không có lời giải riêng. B2 brainstorm là lựa chọn còn lại
trong [RUBRIC.md](RUBRIC.md).

## Khởi động

Chạy từ thư mục gốc repo:

```text
docker compose -f docker/docker-compose.yml up -d
docker compose -f docker/docker-compose.yml logs -f airflow
```

Đợi Airflow standalone khởi động xong, mở `http://localhost:8080`. Dùng thông tin
đăng nhập trong log. Nếu mật khẩu không xuất hiện, đọc file của Simple Auth Manager:

```text
docker compose -f docker/docker-compose.yml exec airflow cat /opt/airflow/simple_auth_manager_passwords.json.generated
```

File cho biết tên người dùng và mật khẩu tương ứng. Xem
[Airflow 3.3.2 Quick Start](https://airflow.apache.org/docs/apache-airflow/3.3.2/start.html).

## Backfill bảy ngày

Kiểm tra DAG đã được load và bật DAG trong giao diện (hoặc dùng lệnh `unpause`):

```text
docker compose -f docker/docker-compose.yml exec airflow airflow dags list
docker compose -f docker/docker-compose.yml exec airflow airflow dags list-import-errors
docker compose -f docker/docker-compose.yml exec airflow airflow dags unpause day17_support_pipeline
docker compose -f docker/docker-compose.yml exec airflow airflow backfill create --dag-id day17_support_pipeline --from-date 2026-08-10 --to-date 2026-08-16 --max-active-runs 1 --reprocess-behavior failed --dry-run
```

Dry-run phải liệt kê bảy ngày seed từ 10 đến 16/08/2026. Sau đó tạo backfill:

```text
docker compose -f docker/docker-compose.yml exec airflow airflow backfill create --dag-id day17_support_pipeline --from-date 2026-08-10 --to-date 2026-08-16 --max-active-runs 1 --reprocess-behavior failed
```

Chạy ngày cũ đến ngày mới; không dùng `--run-backwards`. Trong khi backfill chạy,
không trigger run thủ công, tạo thêm backfill hay chạy pipeline khác ghi vào cùng
database container. `max_active_runs=1` của DAG không giới hạn chung với backfill;
backfill có giới hạn riêng, vì vậy cần cả `--max-active-runs 1` và thao tác tuần tự.
Xem [Airflow 3.3.2 Backfill](https://airflow.apache.org/docs/apache-airflow/3.3.2/core-concepts/backfill.html).

## Bằng chứng nộp bài

Chờ bảy run và cả ba task mỗi run thành công. Lấy checksum của warehouse trong
container sau khi backfill hoàn tất (lệnh một dòng dùng được cả PowerShell và bash):

```text
docker compose -f docker/docker-compose.yml exec airflow python -c "from pipeline.run import connect; from pipeline.checksum import gold_checksums; c=connect(); print(gold_checksums(c)); c.close()"
```

Lưu ảnh bảy run vào `bonus/airflow/`, lưu output checksum trong cùng thư mục và
ghi đường dẫn bằng chứng vào `submission/REPORT.md`. Đối chiếu với checksum fresh
build của bản code đã sửa. Database container và database chạy Python trên host
là hai file riêng; lệnh trên đọc kết quả của Airflow.

Để dừng môi trường:

```text
docker compose -f docker/docker-compose.yml down
```

Đây là môi trường standalone phục vụ lab. Kiểm tra `docker compose ... config`
chỉ xác nhận cấu hình Compose; cần bảy run thực tế để xác nhận bonus hoạt động.
