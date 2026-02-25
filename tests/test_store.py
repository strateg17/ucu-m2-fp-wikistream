from datetime import datetime, timedelta, timezone

from app.models import ChangeEvent, Granularity, LengthInfo, Period
from app.store import InMemoryAnalyticsStore


def make_event(user: str, title: str, comment: str, minutes_ago: int, old: int = 100, new: int = 100):
    return ChangeEvent(
        timestamp=datetime.now(timezone.utc) - timedelta(minutes=minutes_ago),
        user=user,
        title=title,
        comment=comment,
        length=LengthInfo(old=old, new=new),
    )


def test_user_stats_and_series() -> None:
    store = InMemoryAnalyticsStore()
    store.add_event(make_event("alice", "Python", "fix typo in sentence", 120, 100, 102))
    store.add_event(make_event("alice", "Python", "added new section", 60, 102, 150))
    store.add_event(make_event("alice", "Django", "spelling", 10, 100, 99))

    stats = store.user_stats("alice", Granularity.DAY)

    assert stats.user == "alice"
    assert len(stats.contribution_series) == 1
    assert stats.contribution_series[0].value == 3
    assert stats.top_topics[0].topic == "Python"
    assert stats.contribution_types["typo_editing"] == 2
    assert stats.contribution_types["content_addition"] == 1


def test_active_user_and_typo_topics() -> None:
    store = InMemoryAnalyticsStore()
    store.add_event(make_event("alice", "Python", "fix typo", 20, 100, 101))
    store.add_event(make_event("bob", "FastAPI", "added content", 10, 100, 170))
    store.add_event(make_event("alice", "Python", "spelling", 5, 100, 101))

    active = store.most_active_user(Period.DAY)
    assert active.user == "alice"
    assert active.contributions == 2

    typo_topics = store.top_typo_topics()
    assert typo_topics[0].topic == "Python"
    assert typo_topics[0].typo_edits == 2
