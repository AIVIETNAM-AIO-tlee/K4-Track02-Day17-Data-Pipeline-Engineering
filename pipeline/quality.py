"""Quality gate for Kafka events — record-level validation at the system boundary.

Slide "Kiểm thử dữ liệu": Pydantic validates each record as it crosses the
boundary (API payloads, Kafka messages). A bad record goes to quarantine with a
reason; it never halts the run and never reaches a feature table.
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Literal, Optional

from pydantic import BaseModel, ConfigDict, Field, ValidationError, model_validator


class SupportEvent(BaseModel):
    model_config = ConfigDict(extra="ignore")

    event_id: str = Field(min_length=1)
    user_id: str = Field(min_length=1)
    ticket_id: Optional[str] = None
    type: Literal["click", "feedback"]
    rating: Optional[Literal["up", "down"]] = None
    page: Optional[str] = None
    event_time: datetime

    @model_validator(mode="after")
    def _rating_matches_type(self) -> "SupportEvent":
        if self.type == "feedback" and self.rating is None:
            raise ValueError("feedback event needs rating up/down")
        if self.type == "click" and self.rating is not None:
            raise ValueError("click event must not carry a rating")
        return self


def _naive_utc(dt: datetime) -> datetime:
    return dt.astimezone(timezone.utc).replace(tzinfo=None) if dt.tzinfo else dt


def validate_events(records: list[tuple]) -> tuple[list[dict], list[dict]]:
    """records: (_payload, _kafka_partition, _kafka_offset, _ingested_at, _batch_id)."""
    valid, bad = [], []
    for payload, part, offset, ingested_at, batch_id in records:
        value = (json.loads(payload) or {}).get("value") or {}
        try:
            ev = SupportEvent.model_validate(value)
        except ValidationError as err:
            first = err.errors()[0]
            where = ".".join(str(x) for x in first.get("loc", ())) or "record"
            bad.append({"_batch_id": batch_id, "_kafka_partition": part,
                        "_kafka_offset": offset, "event_id": value.get("event_id"),
                        "reason": f"{where}: {first['msg']}", "_payload": payload})
            continue
        row = ev.model_dump()
        row["event_time"] = _naive_utc(ev.event_time)
        row.update({"_ingested_at": ingested_at, "_batch_id": batch_id,
                    "_kafka_partition": part, "_kafka_offset": offset})
        valid.append(row)
    return valid, bad
