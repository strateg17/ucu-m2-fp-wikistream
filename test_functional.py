"""
Unit Tests for Functional Programming Primitives (Pure Functional)

Tests the Maybe, Either, IO monads and functional stream operators.
"""

import pytest
import asyncio
import aiostream
from datetime import datetime
from typing import Dict, Any

from functional_utils import (
    Maybe, Some, Nothing,
    Either, Left, Right,
    IO, io_pure,
    compose, pipe
)

from event_processor import (
    parse_event, ParsedEvent, EditType,
    classify_edit_type, parse_stream
)

from user_statistics import StatisticsStore, UserStatistics
from query_api import (
    get_statistics_summary,
    get_most_active_users,
    get_top_typo_topics
)


class TestMaybeMonad:
    def test_some_creation(self):
        maybe = Some(42)
        assert maybe.is_some()
        assert maybe.get_or_else(0) == 42
    
    def test_nothing_creation(self):
        maybe = Nothing()
        assert not maybe.is_some()
        assert maybe.get_or_else(0) == 0
    
    def test_functor_map(self):
        maybe = Some(5)
        result = maybe.map(lambda x: x * 2)
        assert result.get_or_else(0) == 10


class TestEventProcessing:
    def create_sample_event(self, user="TestUser"):
        return {
            'user': user,
            'title': 'Test Page',
            'timestamp': 1711200000,
            'bot': False,
            'minor': False,
            'length': {'old': 100, 'new': 200}
        }

    def test_parse_event_success(self):
        raw_event = self.create_sample_event()
        maybe_parsed = parse_event(raw_event)
        assert maybe_parsed.is_some()
        parsed = maybe_parsed.get_or_else(None)
        assert parsed.user == "TestUser"
        assert parsed.diff_size == 100

    def test_classify_edit_type(self):
        assert classify_edit_type(10, False, False, "") == EditType.TYPO_EDITING
        assert classify_edit_type(100, False, False, "") == EditType.CONTENT_ADDITION


class TestFunctionalStoreAndOperators:
    def create_sample_event(self, user="TestUser"):
        return {
            'user': user,
            'title': 'Test Page',
            'timestamp': 1711200000,
            'bot': False,
            'minor': False,
            'length': {'old': 100, 'new': 200}
        }

    def test_store_immutability(self):
        store1 = StatisticsStore()
        event = parse_event(self.create_sample_event()).get_or_else(None)
        store2 = store1.update(event)
        
        assert len(store1.users) == 0
        assert len(store2.users) == 1
        assert "TestUser" in store2.users

    def test_parse_stream_operator(self):
        async def run_test():
            raw_events = [self.create_sample_event("U1"), self.create_sample_event("U2")]
            source = aiostream.stream.iterate(raw_events)
            pipeline = source | parse_stream.pipe()
            results = []
            async with pipeline.stream() as streamer:
                async for event in streamer:
                    results.append(event)
            assert len(results) == 2
        asyncio.run(run_test())

    def test_accumulation_pipeline(self):
        async def run_test():
            raw_events = [self.create_sample_event("U1"), self.create_sample_event("U1")]
            source = aiostream.stream.iterate(raw_events)
            pipeline = (
                source 
                | parse_stream.pipe()
                | aiostream.pipe.accumulate(
                    lambda state, event: (state[0].update(event), event), 
                    initializer=(StatisticsStore(), None)
                )
            )
            final_store = None
            async with pipeline.stream() as streamer:
                async for store, event in streamer:
                    final_store = store
            assert final_store.users["U1"].total_contributions == 2
        asyncio.run(run_test())


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
