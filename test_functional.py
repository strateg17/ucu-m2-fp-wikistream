"""
Unit Tests for Functional Programming Primitives (Pure Functional)

Tests the Maybe, Either, IO monads and functional stream operators.
"""

import pytest
import asyncio
import aiostream
from datetime import datetime, timedelta

from functional_utils import (
    Maybe, Some, Nothing,
    Either, Left, Right,
    IO, io_pure,
    compose, pipe, curry2
)

from event_processor import (
    parse_event, ParsedEvent, EditType,
    classify_edit_type, parse_stream
)

from user_statistics import (
    StatisticsStore, UserStatistics,
    TimeGranularity, TimePeriod,
)

from query_api import (
    get_user_contribution_series,
    get_user_top_topics,
    get_user_contribution_types,
    get_most_active_users,
    get_top_typo_topics,
    get_statistics_summary
)


# ============================================================================
# Test Helpers
# ============================================================================

def make_raw_event(
    user="TestUser",
    title="Test Page",
    timestamp=1711200000,
    bot=False,
    minor=False,
    old_length=100,
    new_length=200,
    comment="",
    wiki="enwiki",
    namespace=0,
) -> dict:
    """Build a minimal raw WikiMedia event dict."""
    return {
        'user': user,
        'title': title,
        'timestamp': timestamp,
        'bot': bot,
        'minor': minor,
        'length': {'old': old_length, 'new': new_length},
        'comment': comment,
        'wiki': wiki,
        'namespace': namespace,
    }


def make_parsed_event(
    user="TestUser",
    title="Test Page",
    timestamp: datetime = None,
    edit_type: EditType = EditType.CONTENT_ADDITION,
    diff_size: int = 100,
    is_bot: bool = False,
    is_minor: bool = False,
    comment: str = "",
    wiki: str = "enwiki",
    namespace: int = 0
) -> ParsedEvent:
    """Directly construct a ParsedEvent for unit testing."""
    return ParsedEvent(
        user=user,
        title=title,
        timestamp=timestamp or datetime(2024, 3, 23, 12, 0, 0),
        edit_type=edit_type,
        diff_size=diff_size,
        is_bot=is_bot,
        is_minor=is_minor,
        wiki=wiki,
        namespace=namespace,
        comment=comment,
    )


# ============================================================================
# 1. Maybe Monad
# ============================================================================

class TestMaybeMonad:

    def test_some_holds_value(self):
        m = Some(42)
        assert m.is_some()
        assert m.get_or_else(0) == 42

    def test_nothing_returns_default(self):
        m = Nothing()
        assert not m.is_some()
        assert m.get_or_else(99) == 99

    def test_some_map_transforms_value(self):
        assert Some(5).map(lambda x: x * 2).get_or_else(0) == 10

    def test_nothing_map_stays_nothing(self):
        assert not Nothing().map(lambda x: x * 2).is_some()

    def test_some_flat_map_chains(self):
        result = Some(5).flat_map(lambda x: Some(x + 1))
        assert result.get_or_else(0) == 6

    def test_some_flat_map_to_nothing(self):
        result = Some(5).flat_map(lambda x: Nothing())
        assert not result.is_some()

    def test_nothing_flat_map_stays_nothing(self):
        result = Nothing().flat_map(lambda x: Some(x))
        assert not result.is_some()

    def test_chaining_multiple_maps(self):
        result = Some(2).map(lambda x: x + 1).map(lambda x: x * 3)
        assert result.get_or_else(0) == 9


# ============================================================================
# 2. Either Monad
# ============================================================================

class TestEitherMonad:

    def test_right_is_right(self):
        e = Right(10)
        assert e.is_right()
        assert not e.is_left()
        assert e.get_right() == 10

    def test_left_is_left(self):
        e = Left("error")
        assert e.is_left()
        assert not e.is_right()
        assert e.get_left() == "error"

    def test_right_map(self):
        result = Right(5).map(lambda x: x * 2)
        assert result.get_right() == 10

    def test_left_map_is_noop(self):
        result = Left("err").map(lambda x: x * 2)
        assert result.is_left()
        assert result.get_left() == "err"

    def test_right_flat_map(self):
        result = Right(5).flat_map(lambda x: Right(x + 1))
        assert result.get_right() == 6

    def test_right_flat_map_to_left(self):
        result = Right(5).flat_map(lambda x: Left("oops"))
        assert result.is_left()

    def test_left_flat_map_short_circuits(self):
        called = []
        result = Left("err").flat_map(lambda x: called.append(x) or Right(x))
        assert result.is_left()
        assert called == []

    def test_get_or_else_on_right(self):
        assert Right(42).get_or_else(0) == 42

    def test_get_or_else_on_left(self):
        assert Left("err").get_or_else(99) == 99


# ============================================================================
# 3. IO Monad
# ============================================================================

class TestIOMonad:

    def test_io_runs_effect(self):
        io = IO(lambda: 42)
        assert io.run() == 42

    def test_io_pure_wraps_value(self):
        io = io_pure(7)
        assert io.run() == 7

    def test_io_map(self):
        result = io_pure(3).map(lambda x: x * 4).run()
        assert result == 12

    def test_io_flat_map(self):
        result = io_pure(5).flat_map(lambda x: io_pure(x + 10)).run()
        assert result == 15

    def test_io_delays_execution(self):
        effects = []
        io = IO(lambda: effects.append("run"))
        assert effects == []
        io.run()
        assert effects == ["run"]


# ============================================================================
# 4. Function Composition
# ============================================================================

class TestFunctionComposition:

    def test_compose_right_to_left(self):
        f = compose(lambda x: x + 1, lambda x: x * 2)
        assert f(3) == 7  # (3 * 2) + 1

    def test_pipe_left_to_right(self):
        f = pipe(lambda x: x * 2, lambda x: x + 1)
        assert f(3) == 7  # (3 * 2) + 1

    def test_curry2(self):
        add = curry2(lambda a, b: a + b)
        add5 = add(5)
        assert add5(3) == 8
        assert add5(10) == 15


# ============================================================================
# 5. Event Parsing
# ============================================================================

class TestEventProcessing:

    def test_parse_event_success(self):
        raw = make_raw_event()
        maybe = parse_event(raw)
        assert maybe.is_some()
        event = maybe.get_or_else(None)
        assert event.user == "TestUser"
        assert event.diff_size == 100  # 200 - 100

    def test_parse_event_missing_user_returns_nothing(self):
        raw = make_raw_event()
        del raw['user']
        assert not parse_event(raw).is_some()

    def test_parse_event_missing_title_returns_nothing(self):
        raw = make_raw_event()
        del raw['title']
        assert not parse_event(raw).is_some()

    def test_parse_event_missing_timestamp_returns_nothing(self):
        raw = make_raw_event()
        del raw['timestamp']
        assert not parse_event(raw).is_some()

    def test_classify_typo_edit(self):
        assert classify_edit_type(10, False, False, "") == EditType.TYPO_EDITING

    def test_classify_content_addition(self):
        assert classify_edit_type(200, False, False, "") == EditType.CONTENT_ADDITION

    def test_classify_bot_edit(self):
        assert classify_edit_type(10, True, False, "") == EditType.BOT_EDIT

    def test_classify_minor_edit(self):
        # diff >= 50 AND minor → MINOR_EDIT
        assert classify_edit_type(60, False, True, "") == EditType.MINOR_EDIT

    def test_parse_event_stores_comment(self):
        raw = make_raw_event(comment="Fixed typo: 'teh' → 'the'")
        event = parse_event(raw).get_or_else(None)
        assert event.comment == "Fixed typo: 'teh' → 'the'"

    def test_parse_stream_operator(self):
        async def run():
            source = aiostream.stream.iterate([
                make_raw_event(user="U1"),
                make_raw_event(user="U2"),
            ])
            results = []
            async with (source | parse_stream.pipe()).stream() as s:
                async for event in s:
                    results.append(event)
            return results

        results = asyncio.run(run())
        assert len(results) == 2
        assert results[0].user == "U1"
        assert results[1].user == "U2"

    def test_parse_stream_filters_invalid(self):
        """Events missing required fields should be silently dropped."""
        async def run():
            source = aiostream.stream.iterate([
                make_raw_event(user="Good"),
                {'bad': 'event'},  # no user/title/timestamp
            ])
            results = []
            async with (source | parse_stream.pipe()).stream() as s:
                async for event in s:
                    results.append(event)
            return results

        results = asyncio.run(run())
        assert len(results) == 1
        assert results[0].user == "Good"




# ============================================================================
# 7. UserStatistics (immutability + aggregation)
# ============================================================================

class TestUserStatistics:

    def test_empty_stats_defaults(self):
        s = UserStatistics()
        assert s.total_contributions == 0
        assert s.typo_edits == 0
        assert s.content_additions == 0
        assert s.minor_edits == 0
        assert s.bot_edits == 0
        assert s.contribution_points == []
        assert s.timestamps == []

    def test_add_contribution_returns_new_instance(self):
        """Core immutability guarantee."""
        s1 = UserStatistics()
        event = make_parsed_event()
        s2 = s1.add_contribution(event)
        assert s1 is not s2
        assert s1.total_contributions == 0
        assert s2.total_contributions == 1

    def test_add_contribution_increments_total(self):
        s = UserStatistics()
        for i in range(5):
            s = s.add_contribution(make_parsed_event())
        assert s.total_contributions == 5

    def test_add_contribution_counts_typo_edit(self):
        s = UserStatistics()
        s = s.add_contribution(make_parsed_event(edit_type=EditType.TYPO_EDITING))
        assert s.typo_edits == 1
        assert s.content_additions == 0

    def test_add_contribution_counts_content_addition(self):
        s = UserStatistics()
        s = s.add_contribution(make_parsed_event(edit_type=EditType.CONTENT_ADDITION))
        assert s.content_additions == 1
        assert s.typo_edits == 0

    def test_add_contribution_counts_minor_edit(self):
        s = UserStatistics()
        s = s.add_contribution(make_parsed_event(is_minor=True))
        assert s.minor_edits == 1

    def test_add_contribution_counts_bot_edit(self):
        s = UserStatistics()
        s = s.add_contribution(make_parsed_event(is_bot=True))
        assert s.bot_edits == 1

    def test_add_contribution_tracks_topic(self):
        s = UserStatistics()
        s = s.add_contribution(make_parsed_event(title="Python"))
        s = s.add_contribution(make_parsed_event(title="Python"))
        s = s.add_contribution(make_parsed_event(title="Java"))
        assert s.topic_contributions["Python"] == 2
        assert s.topic_contributions["Java"] == 1

    def test_get_top_topics_sorted(self):
        s = UserStatistics()
        for _ in range(3):
            s = s.add_contribution(make_parsed_event(title="Python"))
        for _ in range(1):
            s = s.add_contribution(make_parsed_event(title="Java"))
        topics = s.get_top_topics(limit=5)
        assert topics[0] == ("Python", 3)
        assert topics[1] == ("Java", 1)

    def test_get_top_topics_respects_limit(self):
        s = UserStatistics()
        for title in ["A", "B", "C", "D", "E"]:
            s = s.add_contribution(make_parsed_event(title=title))
        topics = s.get_top_topics(limit=2)
        assert len(topics) == 2

    def test_get_contribution_series_empty(self):
        s = UserStatistics()
        assert s.get_contribution_series(TimeGranularity.HOUR) == []

    def test_get_contribution_series_single_event(self):
        s = UserStatistics()
        ts = datetime(2024, 3, 23, 10, 5, 0)
        s = s.add_contribution(make_parsed_event(timestamp=ts))
        series = s.get_contribution_series(TimeGranularity.HOUR)
        assert len(series) == 1
        assert series[0][0] == datetime(2024, 3, 23, 10, 0, 0)
        assert series[0][1] == 1

    def test_get_contribution_series_aggregates_same_hour_bucket(self):
        """
        BUG FIX: Multiple events in the same time bucket must produce
        the MAX cumulative total, not an arbitrary overwrite.
        """
        s = UserStatistics()
        s = s.add_contribution(make_parsed_event(timestamp=datetime(2024, 3, 23, 10, 5, 0)))   # total=1
        s = s.add_contribution(make_parsed_event(timestamp=datetime(2024, 3, 23, 10, 35, 0)))  # total=2
        s = s.add_contribution(make_parsed_event(timestamp=datetime(2024, 3, 23, 11, 5, 0)))   # total=3

        series = s.get_contribution_series(TimeGranularity.HOUR)

        assert len(series) == 2
        bucket_10 = datetime(2024, 3, 23, 10, 0, 0)
        bucket_11 = datetime(2024, 3, 23, 11, 0, 0)
        series_dict = dict(series)
        assert series_dict[bucket_10] == 2  # highest total in the 10:xx bucket
        assert series_dict[bucket_11] == 3  # highest total in the 11:xx bucket

    def test_get_contribution_series_day_granularity(self):
        s = UserStatistics()
        s = s.add_contribution(make_parsed_event(timestamp=datetime(2024, 3, 23, 8, 0, 0)))
        s = s.add_contribution(make_parsed_event(timestamp=datetime(2024, 3, 23, 20, 0, 0)))
        s = s.add_contribution(make_parsed_event(timestamp=datetime(2024, 3, 24, 10, 0, 0)))

        series = s.get_contribution_series(TimeGranularity.DAY)
        assert len(series) == 2  # 2 distinct days

    def test_get_contribution_series_is_sorted(self):
        s = UserStatistics()
        s = s.add_contribution(make_parsed_event(timestamp=datetime(2024, 3, 25, 12, 0, 0)))
        s = s.add_contribution(make_parsed_event(timestamp=datetime(2024, 3, 23, 12, 0, 0)))
        s = s.add_contribution(make_parsed_event(timestamp=datetime(2024, 3, 24, 12, 0, 0)))

        series = s.get_contribution_series(TimeGranularity.DAY)
        timestamps = [t for t, _ in series]
        assert timestamps == sorted(timestamps)


# ============================================================================
# 8. StatisticsStore (immutability + accumulation)
# ============================================================================

class TestStatisticsStore:

    def test_empty_store_defaults(self):
        store = StatisticsStore()
        assert store.users == {}
        assert len(store.all_events) == 0
        assert len(store.topic_typo_counts) == 0
        assert len(store.word_mistake_counts) == 0

    def test_update_returns_new_instance(self):
        store = StatisticsStore()
        event = make_parsed_event()
        new_store = store.update(event)
        assert store is not new_store

    def test_update_does_not_mutate_original(self):
        store = StatisticsStore()
        event = make_parsed_event()
        _ = store.update(event)
        assert len(store.users) == 0
        assert len(store.all_events) == 0

    def test_update_creates_user_on_first_event(self):
        store = StatisticsStore()
        event = make_parsed_event(user="Alice")
        store = store.update(event)
        assert "Alice" in store.users
        assert store.users["Alice"].total_contributions == 1

    def test_update_accumulates_existing_user(self):
        store = StatisticsStore()
        for _ in range(3):
            store = store.update(make_parsed_event(user="Bob"))
        assert store.users["Bob"].total_contributions == 3

    def test_update_tracks_typo_topic(self):
        store = StatisticsStore()
        store = store.update(
            make_parsed_event(title="Python", edit_type=EditType.TYPO_EDITING)
        )
        assert store.topic_typo_counts["Python"] == 1

    def test_update_does_not_track_non_typo_topic(self):
        store = StatisticsStore()
        store = store.update(
            make_parsed_event(title="Python", edit_type=EditType.CONTENT_ADDITION)
        )
        assert store.topic_typo_counts["Python"] == 0

    def test_update_appends_event_to_all_events(self):
        store = StatisticsStore()
        for i in range(4):
            store = store.update(make_parsed_event())
        assert len(store.all_events) == 4

    def test_get_summary_contains_expected_keys(self):
        store = StatisticsStore()
        summary = store.get_summary()
        assert "total_users" in summary
        assert "total_events" in summary
        assert "total_topics" in summary

    def test_get_summary_values(self):
        store = StatisticsStore()
        store = store.update(make_parsed_event(user="Alice", title="P1", edit_type=EditType.TYPO_EDITING))
        store = store.update(make_parsed_event(user="Bob",   title="P2", edit_type=EditType.CONTENT_ADDITION))
        s = store.get_summary()
        assert s["total_users"] == 2
        assert s["total_events"] == 2
        assert s["total_topics"] == 1  # only P1 was a typo


# ============================================================================
# 9. Query API — all functions, Left and Right paths
# ============================================================================

class TestQueryAPI:

    # --- get_user_contribution_series ---

    def test_series_user_not_found_returns_left(self):
        result = get_user_contribution_series("Ghost", StatisticsStore())
        assert result.is_left()

    def test_series_no_data_returns_left(self):
        """Store has user but somehow no contribution_points (edge case)."""
        store = StatisticsStore(users={"Alice": UserStatistics()})
        result = get_user_contribution_series("Alice", store)
        assert result.is_left()

    def test_series_returns_right_with_data(self):
        store = StatisticsStore()
        event = make_parsed_event(user="Alice", timestamp=datetime(2024, 3, 23, 10, 0, 0))
        store = store.update(event)
        result = get_user_contribution_series("Alice", store, TimeGranularity.HOUR)
        assert result.is_right()
        data = result.get_right()
        assert len(data) == 1
        assert data[0][1] == 1  # one contribution

    def test_series_respects_granularity(self):
        store = StatisticsStore()
        for h in [8, 10, 12]:
            store = store.update(
                make_parsed_event(user="Alice", timestamp=datetime(2024, 3, 23, h, 0, 0))
            )
        result_hour = get_user_contribution_series("Alice", store, TimeGranularity.HOUR)
        result_day = get_user_contribution_series("Alice", store, TimeGranularity.DAY)
        assert len(result_hour.get_right()) == 3  # 3 different hours
        assert len(result_day.get_right()) == 1   # all in same day

    # --- get_user_top_topics ---

    def test_topics_user_not_found(self):
        result = get_user_top_topics("Ghost", StatisticsStore())
        assert result.is_left()

    def test_topics_no_topics_returns_left(self):
        store = StatisticsStore(users={"Alice": UserStatistics()})
        result = get_user_top_topics("Alice", store)
        assert result.is_left()

    def test_topics_returns_sorted_by_count(self):
        store = StatisticsStore()
        for _ in range(3):
            store = store.update(make_parsed_event(user="Alice", title="Python"))
        store = store.update(make_parsed_event(user="Alice", title="Java"))
        result = get_user_top_topics("Alice", store, limit=5)
        assert result.is_right()
        topics = result.get_right()
        assert topics[0] == ("Python", 3)
        assert topics[1] == ("Java", 1)

    def test_topics_limit_is_respected(self):
        store = StatisticsStore()
        for title in ["A", "B", "C", "D", "E"]:
            store = store.update(make_parsed_event(user="Alice", title=title))
        result = get_user_top_topics("Alice", store, limit=2)
        assert len(result.get_right()) == 2

    # --- get_user_contribution_types ---

    def test_types_user_not_found(self):
        result = get_user_contribution_types("Ghost", StatisticsStore())
        assert result.is_left()

    def test_types_returns_absolute_numbers(self):
        store = StatisticsStore()
        store = store.update(make_parsed_event(user="Alice", edit_type=EditType.TYPO_EDITING))
        store = store.update(make_parsed_event(user="Alice", edit_type=EditType.CONTENT_ADDITION))
        store = store.update(make_parsed_event(user="Alice", edit_type=EditType.CONTENT_ADDITION))

        result = get_user_contribution_types("Alice", store)
        assert result.is_right()
        types = result.get_right()
        assert types["typo_edits"] == 1
        assert types["content_additions"] == 2
        assert types["total"] == 3

    def test_types_contains_all_keys(self):
        store = StatisticsStore()
        store = store.update(make_parsed_event(user="Alice"))
        types = get_user_contribution_types("Alice", store).get_right()
        for key in ("typo_edits", "content_additions", "minor_edits", "bot_edits", "total"):
            assert key in types

    # --- get_most_active_users ---

    def test_active_users_empty_store_returns_left(self):
        result = get_most_active_users(StatisticsStore(), TimePeriod.DAY)
        assert result.is_left()

    def test_active_users_returns_sorted_by_count(self):
        store = StatisticsStore()
        recent = datetime.now() - timedelta(hours=1)
        for _ in range(3):
            store = store.update(make_parsed_event(user="Alice", timestamp=recent))
        store = store.update(make_parsed_event(user="Bob", timestamp=recent))

        result = get_most_active_users(store, TimePeriod.YEAR)
        assert result.is_right()
        users = result.get_right()
        assert users[0][0] == "Alice"
        assert users[0][1] == 3

    def test_active_users_period_filter_excludes_old_events(self):
        """Events older than the period must not be counted."""
        store = StatisticsStore()

        old_ts = datetime.now() - timedelta(days=400)
        recent_ts = datetime.now() - timedelta(hours=1)

        old_event = make_parsed_event(user="OldUser", timestamp=old_ts)
        recent_event = make_parsed_event(user="RecentUser", timestamp=recent_ts)

        store = store.update(old_event).update(recent_event)

        result = get_most_active_users(store, TimePeriod.YEAR)  # last 365 days
        assert result.is_right()
        users_dict = dict(result.get_right())
        assert "RecentUser" in users_dict
        assert "OldUser" not in users_dict

    def test_active_users_limit_respected(self):
        store = StatisticsStore()
        recent = datetime.now() - timedelta(hours=1)
        for user in ["A", "B", "C", "D", "E"]:
            store = store.update(make_parsed_event(user=user, timestamp=recent))
        result = get_most_active_users(store, TimePeriod.YEAR, limit=3)
        assert len(result.get_right()) == 3

    # --- get_top_typo_topics ---

    def test_typo_topics_empty_returns_left(self):
        result = get_top_typo_topics(StatisticsStore())
        assert result.is_left()

    def test_typo_topics_returns_right(self):
        store = StatisticsStore()
        store = store.update(make_parsed_event(title="Python", edit_type=EditType.TYPO_EDITING))
        store = store.update(make_parsed_event(title="Python", edit_type=EditType.TYPO_EDITING))
        store = store.update(make_parsed_event(title="Java",   edit_type=EditType.TYPO_EDITING))
        result = get_top_typo_topics(store, limit=10)
        assert result.is_right()
        topics = dict(result.get_right())
        assert topics["Python"] == 2
        assert topics["Java"] == 1

    def test_typo_topics_non_typo_not_counted(self):
        store = StatisticsStore()
        store = store.update(make_parsed_event(title="Python", edit_type=EditType.CONTENT_ADDITION))
        result = get_top_typo_topics(store)
        assert result.is_left()  # no typos → Left

    def test_typo_topics_limit(self):
        store = StatisticsStore()
        for title in ["A", "B", "C", "D", "E", "F"]:
            store = store.update(make_parsed_event(title=title, edit_type=EditType.TYPO_EDITING))
        result = get_top_typo_topics(store, limit=3)
        assert len(result.get_right()) == 3

    # --- get_statistics_summary ---

    def test_summary_always_returns_right(self):
        result = get_statistics_summary(StatisticsStore())
        assert result.is_right()

    def test_summary_reflects_current_store(self):
        store = StatisticsStore()
        store = store.update(make_parsed_event(user="Alice"))
        store = store.update(make_parsed_event(user="Bob"))
        summary = get_statistics_summary(store).get_right()
        assert summary["total_users"] == 2
        assert summary["total_events"] == 2




if __name__ == '__main__':
    pytest.main([__file__, '-v'])
