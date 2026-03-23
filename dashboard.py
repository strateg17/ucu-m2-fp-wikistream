"""
Wikipedia Stream Analyzer Dashboard

A web-based dashboard using aiohttp.web to visualize real-time Wikipedia changes.
Reuses existing project modules: wiki_stream, event_processor, user_statistics, query_api.
"""

import asyncio
import json
import logging
from pathlib import Path
from datetime import datetime
from aiohttp import web, WSCloseCode

from wiki_stream import fetch_wikipedia_changes, deduplicator
from event_processor import parse_stream
from user_statistics import FunctionalStore, TimePeriod, TimeGranularity
from query_api import (
    get_statistics_summary,
    get_most_active_users,
    get_top_typo_topics,
    get_user_contribution_types,
    get_user_top_topics,
    get_user_contribution_series
)
import aiostream

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Global set of connected WebSockets
connected_websockets = set()

# Global state for the pure functional pipeline
current_store = FunctionalStore()


# ============================================================================
# Background Processing Task
# ============================================================================

async def wikipedia_stream_task(app):
    """
    Background task that runs the Wikipedia stream pipeline.
    Updates the global FunctionalStore and broadcasts events to all connected
    clients via WebSockets.
    """
    global current_store
    logger.info("Starting Wikipedia stream pure functional background task...")
    
    # Create the pure functional pipeline
    pipeline = (
        fetch_wikipedia_changes()
        | aiostream.pipe.delay(0.1)
        | deduplicator.pipe(key_extractor=lambda event: event.get('id', 0))
        | parse_stream.pipe()
        | aiostream.pipe.accumulate(
            lambda state, event: (state[0].update(event), event),
            initializer=(current_store, None)
        )
    )
    
    try:
        async with pipeline.stream() as streamer:
            async for store, last_event in streamer:
                # Update global state (pure functional accumulation)
                current_store = store
                
                if last_event:
                    # Prepare message for broadcasting
                    message = {
                        'type': 'live_event',
                        'data': {
                            'user': last_event.user,
                            'title': last_event.title,
                            'edit_type': last_event.edit_type.value,
                            'diff': last_event.diff_size,
                            'wiki': last_event.wiki,
                            'timestamp': last_event.timestamp.isoformat()
                        }
                    }
                    
                    # Broadcast to all connected clients
                    if connected_websockets:
                        # Create list of tasks to avoid sequential blocking
                        tasks = [ws.send_json(message) for ws in connected_websockets]
                        await asyncio.gather(*tasks, return_exceptions=True)
                        
    except asyncio.CancelledError:
        logger.info("Wikipedia stream task cancelled.")
    except Exception as e:
        logger.error(f"Error in Wikipedia stream task: {e}")
        # Restart after a short delay
        await asyncio.sleep(5)
        asyncio.create_task(wikipedia_stream_task(app))


# ============================================================================
# WebSocket Handler
# ============================================================================

async def websocket_handler(request):
    """
    Handles incoming WebSocket connections for real-time updates.
    """
    ws = web.WebSocketResponse()
    await ws.prepare(request)
    
    logger.info("New WebSocket connection established")
    connected_websockets.add(ws)
    
    try:
        # Keep connection open until closed by client
        async for msg in ws:
            if msg.type == web.WSMsgType.TEXT:
                if msg.data == 'close':
                    await ws.close()
            elif msg.type == web.WSMsgType.ERROR:
                logger.error(f'WS connection closed with exception {ws.exception()}')
    finally:
        connected_websockets.remove(ws)
        logger.info("WebSocket connection closed")
        
    return ws


# ============================================================================
# API Endpoints (Requirement #3: Statistics retrieval)
# ============================================================================

async def api_summary(request):
    """Returns global statistics summary (Either monad)."""
    result = get_statistics_summary(store=current_store)
    if result.is_right():
        return web.json_response(result.get_right())
    return web.json_response({'error': result.get_left()}, status=500)


async def api_active_users(request):
    """Returns top active users (Requirement #3.4)."""
    period_str = request.query.get('period', 'hour').upper()
    try:
        period = TimePeriod[period_str]
    except KeyError:
        period = TimePeriod.HOUR
        
    result = get_most_active_users(period, limit=10, store=current_store)
    if result.is_right():
        return web.json_response(result.get_right())
    return web.json_response({'error': result.get_left()}, status=404)


async def api_typo_topics(request):
    """Returns top typo topics (Requirement #3.5)."""
    result = get_top_typo_topics(limit=10, store=current_store)
    if result.is_right():
        return web.json_response(result.get_right())
    return web.json_response({'error': result.get_left()}, status=404)


async def api_user_stats(request):
    """
    Returns comprehensive stats for a specific user (Requirements #3.1 - 3.3).
    """
    username = request.match_info.get('username')
    if not username:
        return web.json_response({'error': 'Username required'}, status=400)
    
    # Granularity for time series
    gran_str = request.query.get('granularity', 'hour').upper()
    try:
        gran = TimeGranularity[gran_str]
    except KeyError:
        gran = TimeGranularity.HOUR
        
    # Combine results (Requirement #3.1, #3.2, #3.3)
    types_res = get_user_contribution_types(username, store=current_store)
    topics_res = get_user_top_topics(username, limit=5, store=current_store)
    series_res = get_user_contribution_series(username, gran, store=current_store)
    
    if types_res.is_left():
        return web.json_response({'error': types_res.get_left()}, status=404)
        
    # Aggregate data
    data = {
        'username': username,
        'types': types_res.get_right(),
        'top_topics': topics_res.get_or_else([]),
        'series': [[t.isoformat(), c] for t, c in series_res.get_or_else([])]
    }
    
    return web.json_response(data)


# ============================================================================
# Frontend Serving
# ============================================================================

async def index_handler(request):
    """Serves the main dashboard HTML file."""
    return web.FileResponse(Path('static/index.html'))


# ============================================================================
# App Setup
# ============================================================================

async def start_background_tasks(app):
    """Triggers the Wikipedia stream processing task on startup."""
    app['wiki_stream_task'] = asyncio.create_task(wikipedia_stream_task(app))


async def cleanup_background_tasks(app):
    """Ensures proper shutdown of the stream task."""
    app['wiki_stream_task'].cancel()
    await app['wiki_stream_task']


def create_app():
    """Configures and returns the aiohttp web application."""
    app = web.Application()
    
    # API Routes
    app.router.add_get('/api/summary', api_summary)
    app.router.add_get('/api/active-users', api_active_users)
    app.router.add_get('/api/typo-topics', api_typo_topics)
    app.router.add_get('/api/user/{username}', api_user_stats)
    
    # WebSocket Route
    app.router.add_get('/ws', websocket_handler)
    
    # Static Files & Index
    app.router.add_get('/', index_handler)
    app.router.add_static('/static', Path('static'))
    
    # Lifecycle Hooks
    app.on_startup.append(start_background_tasks)
    app.on_cleanup.append(cleanup_background_tasks)
    
    return app


if __name__ == '__main__':
    # Ensure static directory exists
    Path('static').mkdir(exist_ok=True)
    
    app = create_app()
    logger.info("Starting dashboard at http://localhost:8080")
    web.run_app(app, port=8080)
