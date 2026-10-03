"""Regenerate the seed data in data/ — deterministic, no randomness.

    python scripts/generate_seed.py

You do NOT need to run this for the lab: the generated files are committed.
It exists so instructors can see (and change) every planted story in one place.

What the three "source systems" deliver for 2026-08-10 .. 2026-08-16:

  data/cdc/tickets/<day>.jsonl     Debezium change events for Postgres `tickets`,
                                   as Kafka records written by an S3 sink connector
  data/events/<day>.jsonl          Kafka records from topic `support.events`
                                   (clicks + thumbs up/down feedback)
  data/transcripts/<day>.json      chat transcripts exported to S3 that day

Planted stories (each one maps to a slide):
  * T-91  created low/open 08-10 -> high 08-14 -> closed/bug 08-16 (Silver slide);
          the 08-14 change is delivered twice (at-least-once).
  * T-97  contains a name, an email and a phone (PII); it is triaged and closed
          on 08-12, then DELETED on 08-15 (erasure request). Debezium sends op='d' with `after = null` and the row in
          `before`, followed by a Kafka tombstone (value = null).
  * u05   is offline on a train on 08-12; her clicks and a thumbs-down on T-88
          only reach Kafka on 08-15 (3 days late -> late-data slide).
  * 08-13 consumer restart: two event records are delivered twice.
  * 2 malformed events (bad rating, missing user_id) -> quarantine.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "data"
DAYS = [f"2026-08-{d:02d}" for d in range(10, 17)]


def ts(s: str) -> datetime:
    """All source timestamps are UTC (timezone-aware, so output is machine-independent)."""
    return datetime.fromisoformat(s).replace(tzinfo=timezone.utc)


def ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def micros(dt: datetime) -> int:
    # Debezium's default for Postgres TIMESTAMP columns: io.debezium.time.MicroTimestamp
    return int(dt.timestamp() * 1_000_000)


# ─────────────────────────────────────────────────────────────────────────────
# 1) Tickets — CDC
# ─────────────────────────────────────────────────────────────────────────────
# (when, op, ticket_id, changes)   op: r = initial snapshot, c = insert,
# u = update, d = delete.  `changes` = columns that differ from current row.
TICKET_CHANGES = [
    ("2026-08-10 00:05", "r", "T-81", dict(user_id="u01", created_at="2026-08-08 09:00",
        subject="Bị trừ tiền 2 lần cho gói Pro tháng 8",
        body="Thẻ của tôi bị trừ 2 lần 199.000đ cho cùng một hoá đơn tháng 8.",
        priority="medium", status="closed", category="billing")),
    ("2026-08-10 00:05", "r", "T-82", dict(user_id="u02", created_at="2026-08-09 14:00",
        subject="Không đổi được ngôn ngữ giao diện",
        body="Tôi chọn tiếng Việt nhưng sau khi đăng xuất lại về tiếng Anh.",
        priority="low", status="open", category=None)),
    ("2026-08-10 09:00", "c", "T-91", dict(user_id="u03", created_at="2026-08-10 09:00",
        subject="Ứng dụng crash khi tải file PDF lớn",
        body="Tải file PDF khoảng 40MB lên là ứng dụng tự thoát, thử 3 lần đều vậy.",
        priority="low", status="open", category=None)),
    ("2026-08-10 11:20", "c", "T-84", dict(user_id="u04", created_at="2026-08-10 11:20",
        subject="Hoá đơn VAT sai mã số thuế",
        body="Hoá đơn VAT tháng 7 ghi sai mã số thuế công ty, cần xuất lại.",
        priority="medium", status="open", category="billing")),
    ("2026-08-10 16:00", "u", "T-82", dict(status="pending")),

    ("2026-08-11 10:05", "c", "T-97", dict(user_id="u06", created_at="2026-08-11 10:05",
        subject="Yêu cầu xoá tài khoản",
        body="Tôi là Nguyễn Văn An, email nguyenvanan@gmail.com, sđt 0912 345 678. "
             "Xin xoá toàn bộ dữ liệu của tôi.",
        priority="medium", status="open", category=None)),
    ("2026-08-11 13:30", "c", "T-85", dict(user_id="u05", created_at="2026-08-11 13:30",
        subject="Cách xuất báo cáo sang Excel",
        body="Tôi muốn xuất báo cáo doanh thu theo tuần ra file Excel.",
        priority="low", status="open", category="other")),
    ("2026-08-11 17:00", "u", "T-84", dict(status="closed")),

    ("2026-08-12 08:40", "c", "T-88", dict(user_id="u05", created_at="2026-08-12 08:40",
        subject="Chatbot trả lời sai chính sách hoàn tiền",
        body="Chatbot nói được hoàn tiền trong 60 ngày nhưng điều khoản ghi 30 ngày.",
        priority="high", status="open", category="bug")),
    ("2026-08-12 10:00", "u", "T-85", dict(status="closed")),
    ("2026-08-12 11:00", "u", "T-97", dict(status="closed", category="other")),
    ("2026-08-12 15:00", "c", "T-86", dict(user_id="u07", created_at="2026-08-12 15:00",
        subject="Không đăng nhập được bằng SSO",
        body="Gọi lại cho tôi qua +84 987 654 321 để hỗ trợ đăng nhập SSO công ty.",
        priority="medium", status="open", category=None)),
    ("2026-08-12 18:00", "u", "T-82", dict(status="closed", category="other")),

    ("2026-08-13 09:30", "u", "T-88", dict(status="closed")),
    ("2026-08-13 11:00", "c", "T-89", dict(user_id="u08", created_at="2026-08-13 11:00",
        subject="Muốn đổi gói từ tháng sang năm",
        body="Đổi từ gói tháng sang gói năm thì phần tiền còn lại được tính thế nào?",
        priority="low", status="open", category="billing")),
    ("2026-08-13 14:00", "u", "T-86", dict(priority="high", category="bug")),

    ("2026-08-14 11:30", "u", "T-91", dict(priority="high")),
    ("2026-08-14 16:00", "c", "T-92", dict(user_id="u02", created_at="2026-08-14 16:00",
        subject="Không nhận được email xác nhận thanh toán",
        body="Đã thanh toán qua chuyển khoản nhưng chưa nhận được email xác nhận.",
        priority="medium", status="open", category=None)),
    ("2026-08-14 17:00", "u", "T-89", dict(status="closed")),

    ("2026-08-15 09:00", "d", "T-97", {}),
    ("2026-08-15 10:00", "u", "T-86", dict(status="closed")),
    ("2026-08-15 13:00", "c", "T-93", dict(user_id="u04", created_at="2026-08-15 13:00",
        subject="Thêm thành viên vào workspace",
        body="Làm sao để mời thêm 5 thành viên vào workspace của nhóm?",
        priority="low", status="open", category="other")),

    ("2026-08-16 10:00", "u", "T-92", dict(category="billing")),
    ("2026-08-16 15:00", "u", "T-91", dict(status="closed", category="bug")),
    ("2026-08-16 16:30", "u", "T-93", dict(status="closed")),
    ("2026-08-16 17:45", "c", "T-94", dict(user_id="u01", created_at="2026-08-16 17:45",
        subject="Xuất dữ liệu cho kiểm toán",
        body="Cần xuất toàn bộ lịch sử giao dịch năm 2025 cho kiểm toán.",
        priority="medium", status="open", category=None)),
]

# at-least-once redelivery: (when the duplicate arrives, ticket_id of the change, change time)
CDC_REDELIVERED = [("2026-08-14 11:31", "T-91", "2026-08-14 11:30")]


def build_tickets() -> dict[str, list[dict]]:
    state: dict[str, dict] = {}
    by_day: dict[str, list[dict]] = {d: [] for d in DAYS}
    lsn, offset = 24_000_000, 0
    emitted: dict[tuple[str, str], dict] = {}

    def row_for(tid: str, row: dict, when: datetime) -> dict:
        return {
            "ticket_id": tid,
            "user_id": row["user_id"],
            "subject": row["subject"],
            "body": row["body"],
            "priority": row["priority"],
            "status": row["status"],
            "category": row["category"],
            "created_at": micros(ts(row["created_at"])),
            "updated_at": micros(when),
        }

    for when_s, op, tid, changes in TICKET_CHANGES:
        when = ts(when_s)
        lsn += 1_000
        before = row_for(tid, state[tid], ts(state[tid]["_updated"])) if tid in state else None
        if op == "d":
            after = None
            state.pop(tid)
        else:
            cur = dict(state.get(tid, {}))
            cur.update(changes)
            cur["_updated"] = when_s
            state[tid] = cur
            after = row_for(tid, cur, when)
        rec = {
            "topic": "support.public.tickets",
            "partition": 0,
            "offset": offset,
            "timestamp": ms(when + timedelta(seconds=2)),
            "key": {"ticket_id": tid},
            "value": {
                "before": before if op != "r" else None,
                "after": after,
                "source": {"connector": "postgresql", "db": "support", "schema": "public",
                           "table": "tickets", "lsn": lsn, "ts_ms": ms(when)},
                "op": op,
                "ts_ms": ms(when + timedelta(seconds=1)),
            },
        }
        offset += 1
        by_day[when_s[:10]].append(rec)
        emitted[(tid, when_s)] = rec
        if op == "d":
            # Kafka tombstone so log compaction can drop the key
            by_day[when_s[:10]].append({
                "topic": "support.public.tickets", "partition": 0, "offset": offset,
                "timestamp": ms(when + timedelta(seconds=3)),
                "key": {"ticket_id": tid}, "value": None,
            })
            offset += 1

    for arrive_s, tid, change_s in CDC_REDELIVERED:
        dup = json.loads(json.dumps(emitted[(tid, change_s)]))
        dup["timestamp"] = ms(ts(arrive_s))          # same record, delivered again
        by_day[arrive_s[:10]].append(dup)
    for d in by_day:
        by_day[d].sort(key=lambda r: (r["timestamp"], r["offset"]))
    return by_day


# ─────────────────────────────────────────────────────────────────────────────
# 2) Events — Kafka topic support.events (3 partitions, key = user_id)
# ─────────────────────────────────────────────────────────────────────────────
# (event_time, arrives_at or None = +2s, user, type, ticket_id, rating, page)
EVENTS = [
    # 08-10
    ("2026-08-10 09:05", None, "u03", "click", "T-91", None, "/help/upload-limits"),
    ("2026-08-10 09:12", None, "u03", "click", None, None, "/help/pdf"),
    ("2026-08-10 11:25", None, "u04", "click", "T-84", None, "/billing/invoices"),
    ("2026-08-10 14:02", None, "u01", "click", None, None, "/pricing"),
    ("2026-08-10 16:10", None, "u02", "feedback", "T-82", "down", None),
    ("2026-08-10 20:45", None, "u08", "click", None, None, "/pricing"),
    # 08-11
    ("2026-08-11 08:15", None, "u01", "click", None, None, "/help/export"),
    ("2026-08-11 10:07", None, "u06", "click", "T-97", None, "/account/delete"),
    ("2026-08-11 13:35", None, "u05", "click", "T-85", None, "/help/export"),
    ("2026-08-11 17:05", None, "u04", "feedback", "T-84", "up", None),
    ("2026-08-11 21:30", None, "u02", "click", None, None, "/settings/language"),
    ("2026-08-11 23:58", "2026-08-12 00:01", "u07", "click", None, None, "/login/sso"),
    # 08-12
    ("2026-08-12 08:45", None, "u05", "click", "T-88", None, "/help/refunds"),
    ("2026-08-12 10:05", None, "u05", "feedback", "T-85", "up", None),
    ("2026-08-12 15:03", None, "u07", "click", "T-86", None, "/login/sso"),
    ("2026-08-12 18:10", None, "u02", "feedback", "T-82", "up", None),
    # u05 on a train, offline: these reach Kafka only on 08-15 (3 days late)
    ("2026-08-12 21:05", "2026-08-15 07:30", "u05", "click", "T-88", None, "/help/refunds"),
    ("2026-08-12 21:07", "2026-08-15 07:30", "u05", "click", None, None, "/terms/refund-policy"),
    ("2026-08-12 21:10", "2026-08-15 07:31", "u05", "feedback", "T-88", "down", None),
    ("2026-08-12 22:40", None, "u03", "click", "T-91", None, "/help/upload-limits"),
    # 08-13
    ("2026-08-13 09:35", None, "u05", "feedback", "T-88", "up", None),
    ("2026-08-13 11:05", None, "u08", "click", "T-89", None, "/billing/plans"),
    ("2026-08-13 14:05", None, "u07", "click", "T-86", None, "/login/sso"),
    ("2026-08-13 19:20", None, "u01", "click", None, None, "/help/export"),
    ("2026-08-13 23:50", "2026-08-14 00:04", "u07", "click", None, None, "/status"),
    # 08-14
    ("2026-08-14 11:35", None, "u03", "click", "T-91", None, "/help/upload-limits"),
    ("2026-08-14 16:05", None, "u02", "click", "T-92", None, "/billing/payments"),
    ("2026-08-14 17:10", None, "u08", "feedback", "T-89", "up", None),
    ("2026-08-14 20:00", "2026-08-16 08:15", "u02", "click", None, None, "/billing/payments"),
    ("2026-08-14 20:02", "2026-08-16 08:15", "u02", "click", None, None, "/help/bank-transfer"),
    # 08-15
    ("2026-08-15 09:20", None, "u04", "click", None, None, "/workspace/members"),
    ("2026-08-15 10:05", None, "u07", "feedback", "T-86", "down", None),
    ("2026-08-15 13:05", None, "u04", "click", "T-93", None, "/workspace/members"),
    ("2026-08-15 18:30", None, "u01", "click", None, None, "/pricing"),
    # 08-16
    ("2026-08-16 10:05", None, "u02", "click", "T-92", None, "/billing/invoices"),
    ("2026-08-16 15:05", None, "u03", "feedback", "T-91", "up", None),
    ("2026-08-16 16:35", None, "u04", "feedback", "T-93", "up", None),
    ("2026-08-16 17:50", None, "u01", "click", "T-94", None, "/help/export"),
    ("2026-08-16 21:15", None, "u06", "click", None, None, "/pricing"),
]

# malformed records (must be quarantined, must never halt the run)
BAD_EVENTS = [
    ("2026-08-13 12:00", "u03", {"type": "feedback", "ticket_id": "T-91", "rating": "meh"}),
    ("2026-08-15 11:11", None,  {"type": "click", "ticket_id": None, "rating": None,
                                 "page": "/pricing"}),
]
# consumer restart on 08-13: these event_ids are delivered a second time
REDELIVERED_EVENT_IDS = ["e-0021", "e-0022"]


def build_events() -> dict[str, list[dict]]:
    by_day: dict[str, list[dict]] = {d: [] for d in DAYS}
    offsets = [0, 0, 0]
    by_id: dict[str, dict] = {}

    def partition(user: str | None) -> int:
        return (int(user[1:]) % 3) if user else 0

    n = 0
    for et_s, arr_s, user, typ, tid, rating, page in EVENTS:
        n += 1
        et = ts(et_s)
        arrive = ts(arr_s) if arr_s else et + timedelta(seconds=2)
        p = partition(user)
        value = {"event_id": f"e-{n:04d}", "user_id": user, "ticket_id": tid,
                 "type": typ, "rating": rating, "page": page,
                 "event_time": et.strftime("%Y-%m-%dT%H:%M:%SZ")}
        rec = {"topic": "support.events", "partition": p, "offset": offsets[p],
               "timestamp": ms(arrive), "key": user, "value": value}
        offsets[p] += 1
        by_day[arrive.date().isoformat()].append(rec)
        by_id[value["event_id"]] = rec

    for i, (et_s, user, extra) in enumerate(BAD_EVENTS, start=1):
        et = ts(et_s)
        p = partition(user)
        value = {"event_id": f"e-bad-{i}", "user_id": user,
                 "event_time": et.strftime("%Y-%m-%dT%H:%M:%SZ"), **extra}
        rec = {"topic": "support.events", "partition": p, "offset": offsets[p],
               "timestamp": ms(et + timedelta(seconds=2)), "key": user, "value": value}
        offsets[p] += 1
        by_day[et.date().isoformat()].append(rec)

    for eid in REDELIVERED_EVENT_IDS:
        dup = json.loads(json.dumps(by_id[eid]))
        dup["timestamp"] += 60_000          # same partition/offset, delivered again later
        by_day[ts("2026-08-13 00:00").date().isoformat()].append(dup)
    for d in by_day:
        by_day[d].sort(key=lambda r: (r["timestamp"], r["partition"], r["offset"]))
    return by_day


# ─────────────────────────────────────────────────────────────────────────────
# 3) Transcripts — exported to S3 (a ticket can be re-exported with more turns)
# ─────────────────────────────────────────────────────────────────────────────
TRANSCRIPTS = [
    ("2026-08-10 23:00", "T-91", [
        ("customer", "Tải file PDF khoảng 40MB lên là ứng dụng tự thoát, thử 3 lần đều vậy."),
        ("agent", "Cảm ơn anh. Giới hạn hiện tại là 25MB mỗi file, em đã ghi nhận lỗi crash để đội kỹ thuật kiểm tra."),
    ]),
    ("2026-08-11 23:00", "T-84", [
        ("customer", "Hoá đơn VAT tháng 7 ghi sai mã số thuế công ty, cần xuất lại."),
        ("agent", "Em đã xuất lại hoá đơn điện tử với mã số thuế đúng và gửi vào email kế toán của công ty."),
    ]),
    ("2026-08-11 23:00", "T-97", [
        ("customer", "Tôi là Nguyễn Văn An, email nguyenvanan@gmail.com, sđt 0912 345 678. Xin xoá toàn bộ dữ liệu của tôi."),
        ("agent", "Em đã tiếp nhận yêu cầu xoá dữ liệu, thời hạn xử lý tối đa 72 giờ."),
    ]),
    ("2026-08-12 23:00", "T-85", [
        ("customer", "Tôi muốn xuất báo cáo doanh thu theo tuần ra file Excel."),
        ("agent", "Chị vào Báo cáo, chọn khoảng thời gian, bấm Xuất và chọn định dạng XLSX."),
    ]),
    ("2026-08-13 23:00", "T-88", [
        ("customer", "Chatbot nói được hoàn tiền trong 60 ngày nhưng điều khoản ghi 30 ngày."),
        ("agent", "Chị nói đúng, chính sách hiện hành là 30 ngày. Em đã báo đội AI cập nhật tài liệu nguồn cho chatbot."),
    ]),
    ("2026-08-14 23:00", "T-91", [
        ("customer", "Tải file PDF khoảng 40MB lên là ứng dụng tự thoát, thử 3 lần đều vậy."),
        ("agent", "Cảm ơn anh. Giới hạn hiện tại là 25MB mỗi file, em đã ghi nhận lỗi crash để đội kỹ thuật kiểm tra."),
        ("agent", "Đội kỹ thuật đã tái hiện được lỗi, mức ưu tiên được nâng lên cao."),
    ]),
    ("2026-08-15 23:00", "T-86", [
        ("customer", "Gọi lại cho tôi qua +84 987 654 321 để hỗ trợ đăng nhập SSO công ty."),
        ("agent", "Lỗi do chứng chỉ SAML hết hạn phía công ty anh. Sau khi cập nhật chứng chỉ, anh đăng nhập lại được."),
    ]),
    ("2026-08-16 23:00", "T-91", [
        ("customer", "Tải file PDF khoảng 40MB lên là ứng dụng tự thoát, thử 3 lần đều vậy."),
        ("agent", "Cảm ơn anh. Giới hạn hiện tại là 25MB mỗi file, em đã ghi nhận lỗi crash để đội kỹ thuật kiểm tra."),
        ("agent", "Đội kỹ thuật đã tái hiện được lỗi, mức ưu tiên được nâng lên cao."),
        ("agent", "Bản 4.12 đã sửa lỗi crash và nâng giới hạn lên 100MB. Anh cập nhật ứng dụng giúp em."),
    ]),
    ("2026-08-16 23:00", "T-93", [
        ("customer", "Làm sao để mời thêm 5 thành viên vào workspace của nhóm?"),
        ("agent", "Anh vào Cài đặt, chọn Thành viên, bấm Mời và nhập email từng người."),
    ]),
]


def build_transcripts() -> dict[str, list[dict]]:
    by_day: dict[str, list[dict]] = {d: [] for d in DAYS}
    for exported_s, tid, turns in TRANSCRIPTS:
        by_day[exported_s[:10]].append({
            "ticket_id": tid,
            "exported_at": ts(exported_s).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "turns": [{"role": r, "text": t} for r, t in turns],
        })
    return by_day


def main() -> None:
    for sub in ["cdc/tickets", "events", "transcripts"]:
        (DATA / sub).mkdir(parents=True, exist_ok=True)
    for day, recs in build_tickets().items():
        with open(DATA / "cdc" / "tickets" / f"{day}.jsonl", "w", encoding="utf-8") as fh:
            for r in recs:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    for day, recs in build_events().items():
        with open(DATA / "events" / f"{day}.jsonl", "w", encoding="utf-8") as fh:
            for r in recs:
                fh.write(json.dumps(r, ensure_ascii=False) + "\n")
    for day, recs in build_transcripts().items():
        (DATA / "transcripts" / f"{day}.json").write_text(
            json.dumps(recs, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print("seed data written to", DATA)


if __name__ == "__main__":
    main()
