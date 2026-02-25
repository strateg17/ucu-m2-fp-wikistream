from __future__ import annotations

import asyncio
import contextlib
import json
from collections.abc import AsyncIterator

from fastapi import FastAPI, Query
from fastapi.responses import StreamingResponse

from app.models import Granularity, Period, TrackUsersRequest
from app.store import InMemoryAnalyticsStore
from app.stream import ReactiveChangeStream, wikimedia_event_source
from app.models import ChangeEvent


def create_app() -> FastAPI:
    app = FastAPI(title="WikiStream API", version="0.1.0")
    store = InMemoryAnalyticsStore()
    stream = ReactiveChangeStream()
    app.state.store = store
    app.state.stream = stream
    app.state.ingestion_task = None

    @app.on_event("startup")
    async def startup() -> None:
        app.state.ingestion_task = asyncio.create_task(_ingest_forever(store, stream))

    @app.on_event("shutdown")
    async def shutdown() -> None:
        task = app.state.ingestion_task
        if task:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task

    @app.get("/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/stream/recent-changes")
    async def recent_changes() -> StreamingResponse:
        async def generator() -> AsyncIterator[str]:
            async for event in stream.subscribe():
                yield f"data: {event.to_json()}\n\n"

        return StreamingResponse(generator(), media_type="text/event-stream")

    @app.get("/stream/users")
    async def user_changes(users: str = Query(..., description="Comma-separated users")) -> StreamingResponse:
        user_set = {u.strip() for u in users.split(",") if u.strip()}
        store.ensure_users(list(user_set))

        async def generator() -> AsyncIterator[str]:
            async for event in stream.subscribe():
                if event.user in user_set:
                    yield f"data: {event.to_json()}\n\n"

        return StreamingResponse(generator(), media_type="text/event-stream")

    @app.post("/users/track")
    async def track_users(req: TrackUsersRequest) -> dict[str, list[str]]:
        users = list(req.normalized_users())
        store.ensure_users(users)
        return {"tracked_users": sorted(store.tracked_users)}

    @app.get("/users/{user}/stats")
    async def user_stats(user: str, granularity: Granularity = Granularity.DAY):
        return store.user_stats(user=user, granularity=granularity)

    @app.get("/analytics/active-user")
    async def active_user(period: Period = Period.DAY):
        return store.most_active_user(period)

    @app.get("/analytics/top-typo-topics")
    async def top_typo_topics():
        return store.top_typo_topics()

    @app.get("/analytics/common-mistakes")
    async def common_mistakes():
        return store.common_mistakes()

    @app.post("/internal/events")
    async def inject_event(payload: dict) -> dict[str, str]:
        event = ChangeEvent.from_wikimedia(payload)
        store.add_event(event)
        stream.publish(event)
        return {"status": "accepted"}

    return app


async def _ingest_forever(store: InMemoryAnalyticsStore, stream: ReactiveChangeStream) -> None:
    while True:
        try:
            async for raw_event in wikimedia_event_source():
                event = ChangeEvent.from_wikimedia(raw_event)
                store.add_event(event)
                stream.publish(event)
        except Exception:
            await asyncio.sleep(2)
