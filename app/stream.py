from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator

import aiohttp
import reactivex as rx
from reactivex.subject import Subject

from app.models import ChangeEvent

WIKIMEDIA_RECENT_CHANGE_URL = "https://stream.wikimedia.org/v2/stream/recentchange"


class ReactiveChangeStream:
    def __init__(self) -> None:
        self.subject: Subject[ChangeEvent] = Subject()
        self._subscribers: set[asyncio.Queue[ChangeEvent]] = set()

    def publish(self, event: ChangeEvent) -> None:
        self.subject.on_next(event)
        for queue in list(self._subscribers):
            queue.put_nowait(event)

    async def subscribe(self) -> AsyncIterator[ChangeEvent]:
        queue: asyncio.Queue[ChangeEvent] = asyncio.Queue()
        self._subscribers.add(queue)
        try:
            while True:
                yield await queue.get()
        finally:
            self._subscribers.discard(queue)


async def wikimedia_event_source() -> AsyncIterator[dict]:
    timeout = aiohttp.ClientTimeout(total=None, sock_connect=30, sock_read=None)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        async with session.get(WIKIMEDIA_RECENT_CHANGE_URL, headers={"Accept": "text/event-stream"}) as response:
            response.raise_for_status()
            async for raw in response.content:
                line = raw.decode("utf-8", errors="ignore").strip()
                if not line.startswith("data:"):
                    continue
                payload = line.removeprefix("data:").strip()
                if payload:
                    yield json.loads(payload)
