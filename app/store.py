from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone

from app.classifier import classify_contribution, typo_words_from_comment
from app.models import (
    ActiveUserResponse,
    ChangeEvent,
    ContributionType,
    Granularity,
    MistakenWordResponse,
    Period,
    Point,
    TopicCount,
    TypoTopicResponse,
    UserStatsResponse,
)


class InMemoryAnalyticsStore:
    def __init__(self) -> None:
        self.events: list[ChangeEvent] = []
        self.tracked_users: set[str] = set()

    def ensure_users(self, users: list[str]) -> None:
        self.tracked_users.update(users)

    def add_event(self, event: ChangeEvent) -> ContributionType:
        self.events.append(event)
        return classify_contribution(event)

    def user_stats(self, user: str, granularity: Granularity) -> UserStatsResponse:
        self.tracked_users.add(user)
        user_events = sorted((event for event in self.events if event.user == user), key=lambda e: e.timestamp)

        topic_counter: Counter[str] = Counter()
        type_counter: Counter[ContributionType] = Counter()
        bucket_counts: defaultdict[datetime, int] = defaultdict(int)

        for event in user_events:
            topic_counter[event.title] += 1
            contribution_type = classify_contribution(event)
            type_counter[contribution_type] += 1
            bucket_counts[self._truncate(event.timestamp, granularity)] += 1

        cumulative = 0
        points: list[Point] = []
        for bucket_time in sorted(bucket_counts):
            cumulative += bucket_counts[bucket_time]
            points.append(Point(time=bucket_time, value=cumulative))

        return UserStatsResponse(
            user=user,
            granularity=granularity,
            contribution_series=points,
            top_topics=[TopicCount(topic=t, count=c) for t, c in topic_counter.most_common(10)],
            contribution_types={
                ContributionType.TYPO_EDITING: type_counter[ContributionType.TYPO_EDITING],
                ContributionType.CONTENT_ADDITION: type_counter[ContributionType.CONTENT_ADDITION],
            },
        )

    def most_active_user(self, period: Period) -> ActiveUserResponse:
        now = datetime.now(timezone.utc)
        since = {
            Period.DAY: now - timedelta(days=1),
            Period.MONTH: now - timedelta(days=30),
            Period.YEAR: now - timedelta(days=365),
        }[period]

        counter: Counter[str] = Counter(
            event.user for event in self.events if event.timestamp >= since
        )
        if not counter:
            return ActiveUserResponse(period=period, user=None, contributions=0)
        user, contributions = counter.most_common(1)[0]
        return ActiveUserResponse(period=period, user=user, contributions=contributions)

    def top_typo_topics(self) -> list[TypoTopicResponse]:
        counter: Counter[str] = Counter(
            event.title
            for event in self.events
            if classify_contribution(event) == ContributionType.TYPO_EDITING
        )
        return [TypoTopicResponse(topic=t, typo_edits=c) for t, c in counter.most_common(10)]

    def common_mistakes(self) -> list[MistakenWordResponse]:
        word_counter: Counter[str] = Counter()
        for event in self.events:
            for word in typo_words_from_comment(event.comment):
                word_counter[word] += 1
        return [MistakenWordResponse(word=w, count=c) for w, c in word_counter.most_common(20)]

    @staticmethod
    def _truncate(dt: datetime, granularity: Granularity) -> datetime:
        if granularity == Granularity.HOUR:
            return dt.replace(minute=0, second=0, microsecond=0)
        if granularity == Granularity.DAY:
            return dt.replace(hour=0, minute=0, second=0, microsecond=0)
        if granularity == Granularity.MONTH:
            return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
        return dt.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
