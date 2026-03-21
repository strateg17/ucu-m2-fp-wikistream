"""
Wikipedia/Wikimedia Streaming Integration

Connects to WikiMedia EventStream API to receive real-time Wikipedia changes.
Uses aiostream operators for functional reactive programming (following professor's example).

WikiMedia EventStream documentation:
https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams

NOTE: WikiMedia EventStream API does NOT require an API key - it's freely accessible!
"""

import asyncio
import aiohttp
from typing import AsyncIterator, Dict, Any, TypeVar, Callable
import json
import logging
from datetime import datetime, timedelta

import aiostream
from aiostream import operator, pipable_operator

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

T = TypeVar("T")
TKey = TypeVar("TKey")

# WikiMedia EventStream endpoints
WIKIMEDIA_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

# Data limiting: process only events from last 2 months (prevents overwhelming data volume)
DATA_CUTOFF_DAYS = 60  # Last 2 months of data


@operator
async def fetch_wikipedia_changes() -> AsyncIterator[Dict[str, Any]]:
    """
    Fetch Wikipedia recent changes using aiostream operator pattern.
    
    Following professor's example from lesson14.py - using @operator decorator
    to create a functional reactive stream operator.
    
    OPERATOR PATTERN: Creates a stream source using aiostream decorators
    NO API KEY REQUIRED: WikiMedia EventStream is freely accessible
    
    Yields:
        Dict[str, Any]: Raw event data from WikiMedia
    """
    headers = {
        "Accept": "text/event-stream",  # Correct for SSE
        "User-Agent": "WikiStreamAnalyzer/1.0 (Educational project; dizervf@gmail.com) Python/aiohttp",
    }
    
    logger.info("Connecting to WikiMedia EventStream...")
    
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(WIKIMEDIA_STREAM_URL) as response:
                logger.info(f"Connected to WikiMedia stream (status: {response.status})")
                
                # Read SSE stream line by line
                async for line in response.content:
                    line = line.decode('utf-8').strip()
                    
                    # SSE format: lines starting with "data: " contain JSON
                    if line.startswith('data: '):
                        json_data = line[6:]  # Remove "data: " prefix
                        
                        try:
                            event = json.loads(json_data)
                            yield event
                        except json.JSONDecodeError:
                            logger.warning(f"Failed to parse JSON")
                            continue
    
    except aiohttp.ClientError as e:
        logger.error(f"Connection error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")


@pipable_operator
async def filter_by_date(
    source: AsyncIterator[Dict[str, Any]],
    cutoff_days: int = DATA_CUTOFF_DAYS
) -> AsyncIterator[Dict[str, Any]]:
    """
    Filter events to only include recent data (last N days).
    
    Following professor's pattern - using @pipable_operator for transformations.
    This prevents getting banned by limiting data volume (like professor did with GitHub).
    
    PIPABLE OPERATOR: Can be piped with | operator
    
    Args:
        source: Source stream
        cutoff_days: Only process events from last N days
        
    Yields:
        Recent events only
    """
    cutoff_time = datetime.now() - timedelta(days=cutoff_days)
    cutoff_timestamp = int(cutoff_time.timestamp())
    
    async with aiostream.streamcontext(source) as streamer:
        async for event in streamer:
            # Check timestamp (WikiMedia uses Unix timestamp)
            event_timestamp = event.get('timestamp', 0)
            
            if event_timestamp >= cutoff_timestamp:
                yield event
            else:
                # Log when we skip old events (shouldn't happen with live stream)
                logger.debug(f"Skipping old event: {event_timestamp}")


@pipable_operator
async def deduplicator(
    source: AsyncIterator[Dict[str, Any]],
    key_extractor: Callable[[Dict[str, Any]], TKey],
) -> AsyncIterator[Dict[str, Any]]:
    """
    Remove duplicate events based on a key.
    
    Following professor's deduplicator pattern from lesson14.py.
    Prevents processing the same event twice.
    
    PIPABLE OPERATOR: Stateful transformation operator
    
    Args:
        source: Source stream
        key_extractor: Function to extract unique key from event
        
    Yields:
        Deduplicated events
    """
    async with aiostream.streamcontext(source) as streamer:
        seen_keys: set[TKey] = set()
        
        async for event in streamer:
            key = key_extractor(event)
            
            if key in seen_keys:
                logger.debug(f"Duplicate event skipped: {key}")
                continue
            
            seen_keys.add(key)
            yield event


@pipable_operator
async def filter_by_user(
    source: AsyncIterator[Dict[str, Any]],
    username: str
) -> AsyncIterator[Dict[str, Any]]:
    """
    Filter events for a specific user.
    
    PIPABLE OPERATOR: Filter transformation
    
    Args:
        source: Source stream
        username: Username to filter for
        
    Yields:
        Events from the specified user
    """
    async with aiostream.streamcontext(source) as streamer:
        async for event in streamer:
            if event.get('user') == username:
                yield event


@pipable_operator
async def filter_by_users(
    source: AsyncIterator[Dict[str, Any]],
    usernames: set[str]
) -> AsyncIterator[Dict[str, Any]]:
    """
    Filter events for multiple users.
    
    PIPABLE OPERATOR: Filter transformation
    
    Args:
        source: Source stream
        usernames: Set of usernames to filter for
        
    Yields:
        Events from any of the specified users
    """
    async with aiostream.streamcontext(source) as streamer:
        async for event in streamer:
            if event.get('user') in usernames:
                yield event


# ============================================================================
# Pipeline Builder Functions - Following Professor's Pattern
# ============================================================================

def create_basic_pipeline(limit: int = None):
    """
    Create a basic Wikipedia stream pipeline.
    
    Following professor's pattern: pipe operators together with |
    
    Args:
        limit: Optional limit on number of events (for testing)
        
    Returns:
        Pipeline that can be streamed
    """
    pipeline = (
        fetch_wikipedia_changes()
        | aiostream.pipe.delay(0.1)  # Rate limiting - prevent overwhelming
        | deduplicator.pipe(
            key_extractor=lambda event: event.get('id', 0),
        )
    )
    
    if limit:
        pipeline = pipeline | aiostream.pipe.take(limit)
    
    return pipeline


def create_user_tracking_pipeline(username: str, limit: int = None):
    """
    Create a pipeline to track specific user.
    
    Args:
        username: Username to track
        limit: Optional limit on number of events
        
    Returns:
        Pipeline filtered for specific user
    """
    pipeline = (
        fetch_wikipedia_changes()
        | aiostream.pipe.delay(0.1)
        | deduplicator.pipe(
            key_extractor=lambda event: event.get('id', 0),
        )
        | filter_by_user.pipe(username)
    )
    
    if limit:
        pipeline = pipeline | aiostream.pipe.take(limit)
    
    return pipeline


def create_multi_user_pipeline(usernames: list[str], limit: int = None):
    """
    Create a pipeline to track multiple users.
    
    Args:
        usernames: List of usernames to track
        limit: Optional limit on number of events
        
    Returns:
        Pipeline filtered for multiple users
    """
    username_set = set(usernames)
    
    pipeline = (
        fetch_wikipedia_changes()
        | aiostream.pipe.delay(0.1)
        | deduplicator.pipe(
            key_extractor=lambda event: event.get('id', 0),
        )
        | filter_by_users.pipe(username_set)
    )
    
    if limit:
        pipeline = pipeline | aiostream.pipe.take(limit)
    
    return pipeline


# ============================================================================
# Backward Compatibility - Keep old async generator interface
# ============================================================================

async def stream_recent_changes():
    """
    Backward compatible async generator interface.
    
    Yields raw events using the new aiostream pipeline.
    """
    pipeline = create_basic_pipeline()
    
    async with pipeline.stream() as streamer:
        async for event in streamer:
            yield event


async def stream_user_changes(username: str):
    """
    Track changes from a specific user (backward compatible).
    
    Args:
        username: Wikipedia username to track
        
    Yields:
        Events from the specified user
    """
    pipeline = create_user_tracking_pipeline(username)
    
    async with pipeline.stream() as streamer:
        async for event in streamer:
            yield event


async def stream_multiple_users(usernames: list[str]):
    """
    Track changes from multiple users (backward compatible).
    
    Args:
        usernames: List of Wikipedia usernames to track
        
    Yields:
        Events from any of the specified users
    """
    pipeline = create_multi_user_pipeline(usernames)
    
    async with pipeline.stream() as streamer:
        async for event in streamer:
            yield event


# ============================================================================
# Helper Functions
# ============================================================================

async def take_stream(stream, n: int):
    """
    Take first n items from a stream (backward compatible).
    
    Args:
        stream: Source async generator
        n: Number of items to take
        
    Yields:
        First n items from stream
    """
    count = 0
    async for item in stream:
        if count >= n:
            break
        yield item
        count += 1
