"""
Wikipedia/Wikimedia Streaming Integration

This module provides the core streaming functionality for the Wikipedia Real-Time 
Stream Analyzer. It utilizes the `aiostream` library to implement functional 
reactive programming patterns, specifically focusing on composable operators.

WikiMedia EventStream documentation:
https://wikitech.wikimedia.org/wiki/Event_Platform/EventStreams

NOTE: WikiMedia EventStream API does NOT require an API key and is freely accessible.
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

from functional_utils import Maybe, Some, Nothing

T = TypeVar("T")
TKey = TypeVar("TKey")
U = TypeVar("U")

# WikiMedia EventStream endpoint
WIKIMEDIA_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"

# Default data limiting: process only events from last 60 days
DATA_CUTOFF_DAYS = 60


@pipable_operator
async def map_maybe(
    source: AsyncIterator[T],
    f: Callable[[T], Maybe[U]]
) -> AsyncIterator[U]:
    """
    A pipable operator that applies a Maybe-returning function to each item 
    in the stream and yields only the successful results (Some values).
    
    Args:
        source: The input async iterator.
        f: A function that returns a Maybe instance.
        
    Yields:
        Extracted values from Some instances, skipping Nothing.
    """
    async with aiostream.streamcontext(source) as streamer:
        async for item in streamer:
            maybe_val = f(item)
            if maybe_val.is_some():
                yield maybe_val.get_or_else(None)


@operator
async def fetch_wikipedia_changes() -> AsyncIterator[Dict[str, Any]]:
    """
    An aiostream source operator that fetches real-time Wikipedia recent changes.
    
    This operator implements a reactive stream source using Server-Sent Events (SSE).
    It maintains a connection to the Wikimedia EventStream and yields raw JSON 
    events as they arrive.
    
    Yields:
        Dict[str, Any]: A dictionary containing raw event data.
    """
    headers = {
        "Accept": "text/event-stream",
        "User-Agent": "WikiStreamAnalyzer/1.0 (Educational project; dizervf@gmail.com) Python/aiohttp",
    }
    
    logger.info("Connecting to WikiMedia EventStream...")
    
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(WIKIMEDIA_STREAM_URL) as response:
                logger.info(f"Connected to WikiMedia stream (status: {response.status})")
                
                async for line in response.content:
                    line = line.decode('utf-8').strip()
                    
                    # SSE data lines start with the "data: " prefix
                    if line.startswith('data: '):
                        json_data = line[6:]
                        
                        try:
                            event = json.loads(json_data)
                            yield event
                        except json.JSONDecodeError:
                            logger.warning("Failed to parse event JSON")
                            continue
    
    except aiohttp.ClientError as e:
        logger.error(f"HTTP connection error: {e}")
    except Exception as e:
        logger.error(f"Unexpected error in stream source: {e}")


@pipable_operator
async def filter_by_date(
    source: AsyncIterator[Dict[str, Any]],
    cutoff_days: int = DATA_CUTOFF_DAYS
) -> AsyncIterator[Dict[str, Any]]:
    """
    A pipable operator that filters events based on their timestamp.
    
    Args:
        source: The input async iterator (stream source).
        cutoff_days: Only include events occurring within this many days from now.
        
    Yields:
        Filtered event dictionaries.
    """
    cutoff_time = datetime.now() - timedelta(days=cutoff_days)
    cutoff_timestamp = int(cutoff_time.timestamp())
    
    async with aiostream.streamcontext(source) as streamer:
        async for event in streamer:
            event_timestamp = event.get('timestamp', 0)
            
            if event_timestamp >= cutoff_timestamp:
                yield event
            else:
                logger.debug(f"Filtering out old event: {event_timestamp}")


@pipable_operator
async def deduplicator(
    source: AsyncIterator[Dict[str, Any]],
    key_extractor: Callable[[Dict[str, Any]], TKey],
) -> AsyncIterator[Dict[str, Any]]:
    """
    A pipable operator that removes duplicate events from the stream.
    
    This operator maintains a local state of seen keys to ensure each unique 
    event is only processed once.
    
    Args:
        source: The input async iterator.
        key_extractor: A function that returns a unique identifier for an event.
        
    Yields:
        Unique event dictionaries.
    """
    async with aiostream.streamcontext(source) as streamer:
        seen_keys: set[TKey] = set()
        
        async for event in streamer:
            key = key_extractor(event)
            
            if key in seen_keys:
                logger.debug(f"Skipping duplicate event: {key}")
                continue
            
            seen_keys.add(key)
            yield event


@pipable_operator
async def filter_by_user(
    source: AsyncIterator[Dict[str, Any]],
    username: str
) -> AsyncIterator[Dict[str, Any]]:
    """
    A pipable operator that filters the stream for a specific Wikipedia user.
    
    Args:
        source: The input async iterator.
        username: The Wikipedia username to track.
        
    Yields:
        Events initiated by the specified user.
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
    A pipable operator that filters the stream for a set of Wikipedia users.
    
    Args:
        source: The input async iterator.
        usernames: A set of Wikipedia usernames to track.
        
    Yields:
        Events initiated by any of the specified users.
    """
    async with aiostream.streamcontext(source) as streamer:
        async for event in streamer:
            if event.get('user') in usernames:
                yield event


# ============================================================================
# Pipeline Builder Functions
# ============================================================================

def create_basic_pipeline(limit: int = None):
    """
    Constructs a basic Wikipedia event stream pipeline with deduplication.
    
    The pipeline includes:
    1. Fetching raw changes from Wikimedia.
    2. Applying a small delay (0.1s) to respect API throughput.
    3. Deduplicating events based on their unique 'id'.
    
    Args:
        limit: An optional number of events to take before stopping.
        
    Returns:
        An aiostream pipeline object.
    """
    pipeline = (
        fetch_wikipedia_changes()
        | aiostream.pipe.delay(0.1)
        | deduplicator.pipe(
            key_extractor=lambda event: event.get('id', 0),
        )
    )
    
    if limit:
        pipeline = pipeline | aiostream.pipe.take(limit)
    
    return pipeline


def create_user_tracking_pipeline(username: str, limit: int = None):
    """
    Constructs a pipeline tailored for tracking a specific Wikipedia user.
    
    Args:
        username: The user to monitor.
        limit: An optional limit on the number of events to process.
        
    Returns:
        An aiostream pipeline object filtered by user.
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
    Constructs a pipeline tailored for tracking a set of Wikipedia users.
    
    Args:
        usernames: A list of users to monitor.
        limit: An optional limit on the number of events to process.
        
    Returns:
        An aiostream pipeline object filtered by multiple users.
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
# Legacy Async Generator Interfaces
# ============================================================================

async def stream_recent_changes():
    """
    Provides an asynchronous generator interface for the basic pipeline.
    
    Yields:
        Raw event dictionaries.
    """
    pipeline = create_basic_pipeline()
    
    async with pipeline.stream() as streamer:
        async for event in streamer:
            yield event


async def stream_user_changes(username: str):
    """
    Provides an asynchronous generator interface for tracking a single user.
    
    Args:
        username: The user to monitor.
        
    Yields:
        Event dictionaries associated with the user.
    """
    pipeline = create_user_tracking_pipeline(username)
    
    async with pipeline.stream() as streamer:
        async for event in streamer:
            yield event


async def stream_multiple_users(usernames: list[str]):
    """
    Provides an asynchronous generator interface for tracking multiple users.
    
    Args:
        usernames: The users to monitor.
        
    Yields:
        Event dictionaries associated with any of the specified users.
    """
    pipeline = create_multi_user_pipeline(usernames)
    
    async with pipeline.stream() as streamer:
        async for event in streamer:
            yield event


# ============================================================================
# Stream Utility Functions
# ============================================================================

async def take_stream(stream, n: int):
    """
    Consumes and yields only the first n items from an asynchronous stream.
    
    Args:
        stream: The source async generator or stream.
        n: The number of items to take.
        
    Yields:
        Items from the stream until the limit is reached.
    """
    count = 0
    async for item in stream:
        if count >= n:
            break
        yield item
        count += 1
