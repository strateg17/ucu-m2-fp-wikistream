"""
Wikipedia Real-Time Stream Analyzer (Pure Functional Reactive)

Demonstrates FRP with aiostream and immutable state snapshots.
"""

import asyncio
import signal
from typing import Set
from datetime import datetime
from pathlib import Path

import aiostream
from wiki_stream import fetch_wikipedia_changes, create_basic_pipeline
from event_processor import parse_stream, EditType
from user_statistics import StatisticsStore, TimePeriod
from query_api import (
    get_statistics_summary,
    get_most_active_users,
    get_top_typo_topics,
    print_either_result
)

# Shutdown handler
shutdown_requested = False
def signal_handler(sig, frame):
    global shutdown_requested
    print("\nShutdown requested...")
    shutdown_requested = True
signal.signal(signal.SIGINT, signal_handler)


async def demo_reactive_pipeline():
    """Pure functional pipeline with infinite state accumulation."""
    print("\n" + "=" * 80)
    print("RUNNING: Infinite Pure Functional Reactive Pipeline (CLI)")
    print("=" * 80)
    
    pipeline = (
        fetch_wikipedia_changes()
        | aiostream.pipe.delay(0.1)
        | parse_stream.pipe()
        | aiostream.pipe.accumulate(
            lambda state, event: (state[0].update(event), event), 
            initializer=(StatisticsStore(), None)
        )
    )
    
    final_store = StatisticsStore()
    event_count = 0
    async with pipeline.stream() as streamer:
        async for store, event in streamer:
            if shutdown_requested: break
            final_store = store
            event_count += 1
            
            if event:
                print(f"[{event_count:04d}] [{event.user}] edited '{event.title}' ({event.edit_type.value})")

            # Periodic summary every 10 events
            if event_count % 10 == 0:
                print("\n" + "-" * 40)
                print(f"Periodic Statistics Summary (at {event_count} events):")
                print_either_result(get_statistics_summary(final_store), "Global Stats")
                print("-" * 40 + "\n")

    print("\nFinal Statistics Summary:")
    print_either_result(get_statistics_summary(final_store), "Global Stats")
    
    print("\nTop 5 Active Users:")
    print_either_result(get_most_active_users(final_store, TimePeriod.HOUR, limit=5), "Active Users")


async def main():
    print("\n" + "█" * 80)
    print("  WIKIPEDIA REAL-TIME STREAM ANALYZER (FRP)")
    print("█" * 80)
    
    try:
        await demo_reactive_pipeline()
    except KeyboardInterrupt:
        print("\nInterrupted.")
    except Exception as e:
        print(f"\nError: {e}")


if __name__ == '__main__':
    asyncio.run(main())
