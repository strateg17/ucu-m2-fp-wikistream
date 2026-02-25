from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from enum import Enum
import json
from typing import Iterable


class StrEnum(str, Enum):
    pass


class Granularity(StrEnum):
    YEAR = "year"
    MONTH = "month"
    DAY = "day"
    HOUR = "hour"


class Period(StrEnum):
    YEAR = "year"
    MONTH = "month"
    DAY = "day"


class ContributionType(StrEnum):
    TYPO_EDITING = "typo_editing"
    CONTENT_ADDITION = "content_addition"


@dataclass(slots=True)
class LengthInfo:
    old: int | None = None
    new: int | None = None


@dataclass(slots=True)
class ChangeEvent:
    timestamp: datetime
    user: str
    title: str
    comment: str = ""
    length: LengthInfo = field(default_factory=LengthInfo)
    id: int | None = None

    @classmethod
    def from_wikimedia(cls, payload: dict) -> "ChangeEvent":
        ts = datetime.fromtimestamp(payload.get("timestamp", 0), tz=timezone.utc)
        length_payload = payload.get("length") or {}
        return cls(
            id=payload.get("id"),
            timestamp=ts,
            user=payload.get("user") or "unknown",
            title=payload.get("title") or "unknown",
            comment=payload.get("comment") or "",
            length=LengthInfo(
                old=length_payload.get("old"),
                new=length_payload.get("new"),
            ),
        )

    def to_json(self) -> str:
        return json.dumps(asdict(self), default=str)


@dataclass(slots=True)
class TopicCount:
    topic: str
    count: int


@dataclass(slots=True)
class Point:
    time: datetime
    value: int


@dataclass(slots=True)
class UserStatsResponse:
    user: str
    granularity: Granularity
    contribution_series: list[Point]
    top_topics: list[TopicCount]
    contribution_types: dict[ContributionType, int]


@dataclass(slots=True)
class TrackUsersRequest:
    users: list[str]

    def normalized_users(self) -> Iterable[str]:
        return [user.strip() for user in self.users if user.strip()]


@dataclass(slots=True)
class ActiveUserResponse:
    period: Period
    user: str | None
    contributions: int


@dataclass(slots=True)
class TypoTopicResponse:
    topic: str
    typo_edits: int


@dataclass(slots=True)
class MistakenWordResponse:
    word: str
    count: int
