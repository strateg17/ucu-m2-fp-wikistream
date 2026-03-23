"""
Unit Tests for Functional Programming Primitives and Core Logic

Tests the Maybe, Either, IO monads and event processing logic.
"""

import pytest
from datetime import datetime
from typing import Dict, Any

# Import functional primitives
from functional_utils import (
    Maybe, Some, Nothing,
    Either, Left, Right,
    IO, io_pure,
    Applicative,
    compose, pipe, curry2
)

# Import event processing
from event_processor import (
    parse_event, ParsedEvent, EditType,
    classify_edit_type, extract_user_info,
    parse_and_extract_user, parse_events_batch
)


# ============================================================================
# Tests for Maybe Monad
# ============================================================================

class TestMaybeMonad:
    """Test Maybe monad implementation"""
    
    def test_some_creation(self):
        """Test creating Some with a value"""
        maybe = Some(42)
        assert maybe.is_some()
        assert maybe.get_or_else(0) == 42
    
    def test_nothing_creation(self):
        """Test creating Nothing"""
        maybe = Nothing()
        assert not maybe.is_some()
        assert maybe.get_or_else(0) == 0
    
    def test_functor_map_some(self):
        """Test FUNCTOR: mapping over Some"""
        maybe = Some(5)
        result = maybe.map(lambda x: x * 2)
        assert result.is_some()
        assert result.get_or_else(0) == 10
    
    def test_functor_map_nothing(self):
        """Test FUNCTOR: mapping over Nothing returns Nothing"""
        maybe = Nothing()
        result = maybe.map(lambda x: x * 2)
        assert not result.is_some()
    
    def test_monad_flat_map_some(self):
        """Test MONAD: flat_map with Some"""
        maybe = Some(5)
        result = maybe.flat_map(lambda x: Some(x * 2))
        assert result.is_some()
        assert result.get_or_else(0) == 10
    
    def test_monad_flat_map_nothing(self):
        """Test MONAD: flat_map with Nothing returns Nothing"""
        maybe = Nothing()
        result = maybe.flat_map(lambda x: Some(x * 2))
        assert not result.is_some()
    
    def test_monad_chaining(self):
        """Test MONAD: chaining multiple operations"""
        result = (Some(10)
                  .map(lambda x: x + 5)
                  .flat_map(lambda x: Some(x * 2))
                  .map(lambda x: x - 10))
        assert result.is_some()
        assert result.get_or_else(0) == 20  # (10 + 5) * 2 - 10 = 20


# ============================================================================
# Tests for Either Monad
# ============================================================================

class TestEitherMonad:
    """Test Either monad implementation"""
    
    def test_right_creation(self):
        """Test creating Right"""
        either = Right(42)
        assert either.is_right()
        assert not either.is_left()
        assert either.get_or_else(0) == 42
    
    def test_left_creation(self):
        """Test creating Left"""
        either = Left("error")
        assert either.is_left()
        assert not either.is_right()
        assert either.get_or_else(0) == 0
    
    def test_functor_map_right(self):
        """Test FUNCTOR: mapping over Right"""
        either = Right(5)
        result = either.map(lambda x: x * 2)
        assert result.is_right()
        assert result.get_or_else(0) == 10
    
    def test_functor_map_left(self):
        """Test FUNCTOR: mapping over Left returns Left"""
        either = Left("error")
        result = either.map(lambda x: x * 2)
        assert result.is_left()
    
    def test_monad_flat_map_right(self):
        """Test MONAD: flat_map with Right"""
        either = Right(5)
        result = either.flat_map(lambda x: Right(x * 2))
        assert result.is_right()
        assert result.get_or_else(0) == 10
    
    def test_monad_flat_map_left(self):
        """Test MONAD: flat_map with Left returns Left"""
        either = Left("error")
        result = either.flat_map(lambda x: Right(x * 2))
        assert result.is_left()
    
    def test_error_handling(self):
        """Test Either for error handling"""
        def divide(a: int, b: int) -> Either[str, float]:
            if b == 0:
                return Left("Division by zero")
            return Right(a / b)
        
        success = divide(10, 2)
        assert success.is_right()
        assert success.get_or_else(0) == 5.0
        
        failure = divide(10, 0)
        assert failure.is_left()


# ============================================================================
# Tests for IO Monad
# ============================================================================

class TestIOMonad:
    """Test IO monad implementation"""
    
    def test_io_creation(self):
        """Test creating IO action"""
        io = IO(lambda: 42)
        result = io.run()
        assert result == 42
    
    def test_io_pure(self):
        """Test io_pure helper"""
        io = io_pure(42)
        result = io.run()
        assert result == 42
    
    def test_functor_map(self):
        """Test FUNCTOR: mapping over IO"""
        io = io_pure(5)
        result = io.map(lambda x: x * 2).run()
        assert result == 10
    
    def test_monad_flat_map(self):
        """Test MONAD: flat_map with IO"""
        io = io_pure(5)
        result = io.flat_map(lambda x: io_pure(x * 2)).run()
        assert result == 10
    
    def test_side_effect_delay(self):
        """Test that IO delays side effects"""
        counter = [0]
        
        def increment():
            counter[0] += 1
            return counter[0]
        
        io = IO(increment)
        # Side effect hasn't happened yet
        assert counter[0] == 0
        
        # Run triggers the side effect
        result1 = io.run()
        assert counter[0] == 1
        assert result1 == 1
        
        # Running again triggers again
        result2 = io.run()
        assert counter[0] == 2
        assert result2 == 2


# ============================================================================
# Tests for Applicative
# ============================================================================

class TestApplicative:
    """Test Applicative operations"""
    
    def test_lift2_maybe_success(self):
        """Test APPLICATIVE: lift2 with two Some values"""
        result = Applicative.lift2_maybe(
            lambda a, b: a + b,
            Some(5),
            Some(3)
        )
        assert result.is_some()
        assert result.get_or_else(0) == 8
    
    def test_lift2_maybe_failure(self):
        """Test APPLICATIVE: lift2 with Nothing"""
        result = Applicative.lift2_maybe(
            lambda a, b: a + b,
            Some(5),
            Nothing()
        )
        assert not result.is_some()
    
    def test_sequence_maybe_success(self):
        """Test APPLICATIVE: sequence with all Some"""
        maybes = [Some(1), Some(2), Some(3)]
        result = Applicative.sequence_maybe(maybes)
        assert result.is_some()
        assert result.get_or_else([]) == [1, 2, 3]
    
    def test_sequence_maybe_failure(self):
        """Test APPLICATIVE: sequence with Nothing"""
        maybes = [Some(1), Nothing(), Some(3)]
        result = Applicative.sequence_maybe(maybes)
        assert not result.is_some()


# ============================================================================
# Tests for Function Composition
# ============================================================================

class TestFunctionComposition:
    """Test function composition utilities"""
    
    def test_compose(self):
        """Test compose function"""
        add_one = lambda x: x + 1
        multiply_two = lambda x: x * 2
        
        composed = compose(add_one, multiply_two)
        result = composed(5)  # add_one(multiply_two(5)) = add_one(10) = 11
        assert result == 11
    
    def test_pipe(self):
        """Test pipe function"""
        add_one = lambda x: x + 1
        multiply_two = lambda x: x * 2
        
        piped = pipe(add_one, multiply_two)
        result = piped(5)  # multiply_two(add_one(5)) = multiply_two(6) = 12
        assert result == 12
    
    def test_curry2(self):
        """Test currying 2-arg function"""
        add = lambda a, b: a + b
        curried_add = curry2(add)
        
        add_five = curried_add(5)
        result = add_five(3)
        assert result == 8


# ============================================================================
# Tests for Event Processing
# ============================================================================

class TestEventProcessing:
    """Test event parsing and processing"""
    
    def create_sample_event(
        self,
        user: str = "TestUser",
        title: str = "Test Page",
        timestamp: int = 1234567890,
        diff_size: int = 100
    ) -> Dict[str, Any]:
        """Helper to create sample event"""
        return {
            'user': user,
            'title': title,
            'timestamp': timestamp,
            'length': {'old': 1000, 'new': 1000 + diff_size},
            'bot': False,
            'minor': False,
            'wiki': 'enwiki',
            'namespace': 0,
            'comment': 'Test edit'
        }
    
    def test_parse_event_success(self):
        """Test parsing valid event"""
        raw_event = self.create_sample_event()
        maybe_parsed = parse_event(raw_event)
        
        assert maybe_parsed.is_some()
        parsed = maybe_parsed.get_or_else(None)
        assert parsed.user == "TestUser"
        assert parsed.title == "Test Page"
        assert parsed.diff_size == 100
    
    def test_parse_event_missing_user(self):
        """Test parsing event with missing user (should return Nothing)"""
        raw_event = self.create_sample_event()
        del raw_event['user']
        
        maybe_parsed = parse_event(raw_event)
        assert not maybe_parsed.is_some()
    
    def test_parse_event_missing_title(self):
        """Test parsing event with missing title"""
        raw_event = self.create_sample_event()
        del raw_event['title']
        
        maybe_parsed = parse_event(raw_event)
        assert not maybe_parsed.is_some()
    
    def test_classify_edit_type_typo(self):
        """Test classification of typo edits"""
        edit_type = classify_edit_type(
            diff_size=30,
            is_bot=False,
            is_minor=False,
            comment="Fixed typo"
        )
        assert edit_type == EditType.TYPO_EDITING
    
    def test_classify_edit_type_content(self):
        """Test classification of content additions"""
        edit_type = classify_edit_type(
            diff_size=200,
            is_bot=False,
            is_minor=False,
            comment="Added new section"
        )
        assert edit_type == EditType.CONTENT_ADDITION
    
    def test_classify_edit_type_bot(self):
        """Test classification of bot edits"""
        edit_type = classify_edit_type(
            diff_size=100,
            is_bot=True,
            is_minor=False,
            comment="Bot edit"
        )
        assert edit_type == EditType.BOT_EDIT
    
    def test_extract_user_info(self):
        """Test FUNCTOR: extracting user info from parsed event"""
        raw_event = self.create_sample_event()
        maybe_parsed = parse_event(raw_event)
        
        # Use map (functor) to extract user info
        maybe_user_info = maybe_parsed.map(extract_user_info)
        
        assert maybe_user_info.is_some()
        user_info = maybe_user_info.get_or_else({})
        assert user_info['user'] == "TestUser"
        assert 'timestamp' in user_info
    
    def test_parse_and_extract_user(self):
        """Test composed function using FUNCTOR"""
        raw_event = self.create_sample_event()
        maybe_user_info = parse_and_extract_user(raw_event)
        
        assert maybe_user_info.is_some()
        user_info = maybe_user_info.get_or_else({})
        assert user_info['user'] == "TestUser"
    
    def test_parse_events_batch(self):
        """Test batch parsing with FUNCTOR"""
        events = [
            self.create_sample_event(user="User1"),
            self.create_sample_event(user="User2"),
            {'invalid': 'event'},  # This should be filtered out
            self.create_sample_event(user="User3")
        ]
        
        parsed = parse_events_batch(events)
        assert len(parsed) == 3
        assert parsed[0].user == "User1"
        assert parsed[1].user == "User2"
        assert parsed[2].user == "User3"


# ============================================================================
# Functional Store & Stream Operators Tests
# ============================================================================

from user_statistics import FunctionalStore
from event_processor import parse_stream
import asyncio
import aiostream

class TestFunctionalStoreAndOperators:
    """Tests for pure functional state management and reactive operators."""

    def create_sample_event(self, user="TestUser"):
        return {
            'user': user,
            'title': 'Test Page',
            'timestamp': 1711200000,  # Example timestamp
            'bot': False,
            'minor': False,
            'length': {'old': 100, 'new': 200},
            'wiki': 'enwiki',
            'namespace': 0,
            'comment': 'Test comment'
        }

    def test_functional_store_immutability(self):
        """Test that FunctionalStore is immutable and returns new instances."""
        store1 = FunctionalStore()
        raw_event = self.create_sample_event()
        maybe_parsed = parse_event(raw_event)
        event = maybe_parsed.get_or_else(None)
        
        store2 = store1.update(event)
        
        # Original store should be empty
        assert len(store1.users) == 0
        assert len(store1.all_events) == 0
        
        # New store should have the data
        assert len(store2.users) == 1
        assert len(store2.all_events) == 1
        assert "TestUser" in store2.users
        assert store2.get_summary()['total_events'] == 1

    def test_parse_stream_operator(self):
        """Test the parse_stream pipable operator."""
        async def run_test():
            raw_events = [
                self.create_sample_event(user="User1"),
                {'invalid': 'event'},
                self.create_sample_event(user="User2")
            ]
            
            # Create a stream from the list
            source = aiostream.stream.iterate(raw_events)
            
            # Apply the operator
            pipeline = source | parse_stream.pipe()
            
            # Collect results
            results = []
            async with pipeline.stream() as streamer:
                async for event in streamer:
                    results.append(event)
                    
            assert len(results) == 2
            assert results[0].user == "User1"
            assert results[1].user == "User2"
            
        asyncio.run(run_test())

    def test_functional_accumulation_pipeline(self):
        """Test the full functional accumulation pipeline."""
        async def run_test():
            raw_events = [
                self.create_sample_event(user="User1"),
                self.create_sample_event(user="User2"),
                self.create_sample_event(user="User1")
            ]
            
            source = aiostream.stream.iterate(raw_events)
            
            pipeline = (
                source 
                | parse_stream.pipe()
                | aiostream.pipe.accumulate(
                    lambda store, event: store.update(event), 
                    initializer=FunctionalStore()
                )
            )
            
            # Collect final state
            final_state = None
            async with pipeline.stream() as streamer:
                async for state in streamer:
                    final_state = state
                    
            assert final_state is not None
            summary = final_state.get_summary()
            assert summary['total_events'] == 3
            assert summary['total_users'] == 2
            assert "User1" in final_state.users
            assert "User2" in final_state.users
            assert final_state.users["User1"].total_contributions == 2
            
        asyncio.run(run_test())


# ============================================================================
# Run Tests
# ============================================================================

if __name__ == '__main__':
    pytest.main([__file__, '-v'])
