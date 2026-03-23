"""
Wikipedia/Wikimedia Streaming Integration (Reactive)

Uses `aiostream` to implement functional reactive programming patterns.
"""

import asyncio
import aiohttp
from typing import AsyncIterator, Dict, Any, TypeVar, Callable, Optional
import json
import logging
from datetime import datetime, timedelta

import aiostream
from aiostream import operator, pipable_operator
from functional_utils import Maybe, Some, Nothing

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

T = TypeVar("T")
TKey = TypeVar("TKey")
U = TypeVar("U")

WIKIMEDIA_STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange"


@operator
async def fetch_wikipedia_changes() -> AsyncIterator[Dict[str, Any]]:
    """Source operator for WikiMedia EventStream."""
    headers = {
        "Accept": "text/event-stream",
        "User-Agent": "WikiStreamAnalyzer/1.0 (Educational; pure-functional)",
    }
    
    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(WIKIMEDIA_STREAM_URL) as response:
                async for line in response.content:
                    line = line.decode('utf-8').strip()
                    if line.startswith('data: '):
                        try:
                            yield json.loads(line[6:])
                        except json.JSONDecodeError:
                            continue
    except Exception as e:
        logger.error(f"Stream error: {e}")


@pipable_operator
async def map_maybe(
    source: AsyncIterator[T],
    f: Callable[[T], Maybe[U]]
) -> AsyncIterator[U]:
    """Applies Maybe-returning function and filters Nothing."""
    async with aiostream.streamcontext(source) as streamer:
        async for item in streamer:
            maybe_val = f(item)
            if maybe_val.is_some():
                yield maybe_val.get_or_else(None)


@pipable_operator
async def deduplicator(
    source: AsyncIterator[Dict[str, Any]],
    key_extractor: Callable[[Dict[str, Any]], TKey],
) -> AsyncIterator[Dict[str, Any]]:
    """Stateful but isolated deduplication operator."""
    async with aiostream.streamcontext(source) as streamer:
        seen_keys: set[TKey] = set()
        async for event in streamer:
            key = key_extractor(event)
            if key not in seen_keys:
                seen_keys.add(key)
                yield event


@pipable_operator
async def filter_by_user(
    source: AsyncIterator[Dict[str, Any]],
    username: str
) -> AsyncIterator[Dict[str, Any]]:
    async with aiostream.streamcontext(source) as streamer:
        async for event in streamer:
            if event.get('user') == username:
                yield event


def create_basic_pipeline(limit: Optional[int] = None):
    """Constructs the standard functional pipeline."""
    pipeline = (
        fetch_wikipedia_changes()
        | aiostream.pipe.delay(0.1)
        | deduplicator.pipe(key_extractor=lambda e: e.get('id', 0))
    )
    if limit:
        pipeline = pipeline | aiostream.pipe.take(limit)
    return pipeline
