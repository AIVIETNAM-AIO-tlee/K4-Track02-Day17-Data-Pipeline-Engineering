# K4-Track02-Day17 — Quy định làm bài

Đây là **bài cá nhân**, dùng chung cho K4 Track 02.
Mỗi học viên tự nộp repo và chịu trách nhiệm giải thích mã nguồn, báo cáo, bằng chứng của mình.

## Phạm vi được sửa

Sửa logic trong `pipeline/` để đáp ứng contract. Có thể điều chỉnh model dbt khi cần
và điền báo cáo, tài liệu bonus của mình. Giữ nguyên các công cụ chấm và dữ liệu seed:
`scripts/verify.py`, `tests/`, `data/`, `scripts/rerun_check.py`, `pipeline/checksum.py`.
Không thay đổi expected output, bỏ test hay làm yếu kiểm tra để tạo kết quả pass.
Vi phạm quy định này bị **0 điểm cho tiêu chí bị ảnh hưởng**, theo [RUBRIC.md](RUBRIC.md).

## Sử dụng AI

Được dùng AI để đọc code, giải thích khái niệm, đề xuất cách sửa và hỗ trợ viết code.
Bạn phải review thay đổi, chạy kiểm tra thực tế và giải thích được từng dòng sửa.
Ghi công cụ AI đã dùng và phạm vi hỗ trợ trong REPORT; nếu không dùng, ghi rõ không dùng.
Output và checksum nộp làm bằng chứng phải được sinh từ code của bạn, không tự viết
hoặc lấy kết quả của người khác. Xem [VIBE-CODING.md](VIBE-CODING.md) để tham khảo workflow.

## Hợp tác và sao chép

Được trao đổi khái niệm và cách đọc lỗi với học viên khác. Phần sửa code, lập luận
và REPORT phải do bạn thực hiện và hiểu. Không sao chép lời giải hoàn chỉnh,
báo cáo hoặc bằng chứng chạy của người khác. Ghi nguồn tham khảo bên ngoài nếu sử dụng.
Bài này không yêu cầu `TEAM.md` hay repo trưởng nhóm.
Trường hợp nghi vấn sao chép được key coach xem xét theo quy định lớp.

## Chốt bài và nộp muộn

Deadline mặc định: **23:59 ngày diễn ra lab, Asia/Ho_Chi_Minh (UTC+7)**.
Thông báo điều chỉnh của key coach trên LMS trong vòng 48 giờ sau lab được ưu tiên
theo quy ước chung. Ngày trong seed không phải deadline.

Nộp sau deadline được ghi nhận là nộp muộn và áp dụng mức trừ do key coach công bố
trên LMS. Nếu chưa có mức trừ cụ thể, liên hệ key coach để xác nhận; không dùng chính sách
hay đường dẫn của bài khác làm căn cứ cho bài này.

Commit và bằng chứng dùng để chấm phải phản ánh phiên bản đã nộp đúng hạn.
Nếu cần sửa sau deadline, giữ lại commit đã nộp, ghi rõ commit sửa và thời điểm sửa,
báo key coach qua kênh lớp. Phiên bản sửa chỉ được dùng chấm lại khi key coach cho phép;
không sửa hoặc thay bằng chứng để thể hiện như đã hoàn thành trước deadline.

## Bảo mật và dữ liệu

Lab chạy zero-key và dùng dữ liệu giả lập. Không cần tài khoản cloud hay API trả phí.
Nếu thử model thật cho bonus, để secret ngoài Git; không commit `.env`, API key,
token hoặc đưa chúng vào log, ảnh chụp, prompt gửi AI hay REPORT.
Nếu lộ key, thu hồi hoặc đổi key và xử lý phần đã lộ trong lịch sử Git.

Không đưa dữ liệu khách hàng thật hoặc tài liệu nội bộ vào repo public.
Regex hiện tại chỉ che email/số điện thoại; không coi đó là giải pháp đầy đủ để bảo vệ PII.

## Điểm và bằng chứng

Phần bắt buộc có 100 điểm; bonus cộng tối đa 10 điểm (B1: 5, B2: 5).
Bonus là điểm cộng bài lab, tách khỏi điểm phát biểu, tham gia hoặc pitching.
Không làm bonus không bị trừ điểm bắt buộc.
Nộp repo public truy cập được và output kiểm tra thực tế theo [SUBMISSION.md](SUBMISSION.md).
