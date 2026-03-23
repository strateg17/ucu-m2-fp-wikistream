"""
Wikipedia Real-Time Stream Analyzer - Main Demo

This application demonstrates a functional reactive programming (FRP) approach 
to analyzing real-time Wikipedia change streams. It uses the `aiostream` library 
to build composable data pipelines and custom monads for safe data processing.

Key features implemented:
1. Real-time Wikipedia Recent Changes streaming.
2. Targeted tracking of specific users or sets of users.
3. Automated user profile creation upon first observation.
4. Comprehensive user statistics, including:
   - Cumulative contribution time series (X=time, Y=total).
   - Top topics of contribution.
   - Classification of contribution types (typos vs. content).
5. Global statistics:
   - Most active users over specified periods (Year/Month/Day).
   - Top topics with the highest frequency of typo corrections.
6. Data visualization using Matplotlib.

NO API KEY REQUIRED: The WikiMedia EventStream is publicly accessible.
"""

import asyncio
import signal
import sys
from typing import Set
from datetime import datetime
from pathlib import Path

import aiostream
import matplotlib.pyplot as plt
import matplotlib.dates as mdates

# Import functional programming modules
from wiki_stream import (
    create_basic_pipeline,
    create_user_tracking_pipeline,
    create_multi_user_pipeline,
    stream_recent_changes,
    take_stream
)
from event_processor import parse_event, ParsedEvent
from user_statistics import get_statistics_store, TimeGranularity, TimePeriod
from query_api import (
    get_user_contribution_series,
    get_user_top_topics,
    get_user_contribution_types,
    get_most_active_users,
    get_top_typo_topics,
    get_statistics_summary,
    print_either_result
)
from functional_utils import Maybe, Some, Nothing


# Global flag for graceful shutdown
shutdown_requested = False


def signal_handler(sig, frame):
    """
    Handles SIGINT (Ctrl+C) to trigger a graceful shutdown of the 
    streaming pipelines.
    """
    global shutdown_requested
    print("\n\nShutdown requested. Finishing current processing...")
    shutdown_requested = True


# Register the signal handler
signal.signal(signal.SIGINT, signal_handler)


# ============================================================================
# Visualization Functions
# ============================================================================

def create_contribution_visualizations(active_users_result, store):
    """
    Generates visual analytics for user contributions.
    
    Creates a two-part visualization:
    1. A line chart showing cumulative contributions over time for top users.
    2. A bar chart showing total contribution counts.
    
    Charts are saved to the 'visualizations/' directory.
    
    Args:
        active_users_result: An Either monad containing the list of active users.
        store: The StatisticsStore instance containing the data.
    """
    
    if not active_users_result.is_right():
        print("\n⚠️  No users to visualize")
        return
    
    users = active_users_result.get_right()
    if not users:
        print("\n⚠️  No users found")
        return
    
    # Ensure the output directory exists
    output_dir = Path("visualizations")
    output_dir.mkdir(exist_ok=True)
    
    # Focus on the top 5 users for clarity in the charts
    top_users = users[:5]
    
    print(f"\n📈 Creating analytics for top {len(top_users)} users...")
    
    # Setup the matplotlib figure
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
    fig.suptitle('Wikipedia User Contribution Analysis', fontsize=16, fontweight='bold')
    
    # Define a color palette
    colors = plt.cm.Set3(range(len(top_users)))
    
    # Plot 1: Cumulative contribution series (Line Chart)
    for idx, (username, total_count) in enumerate(top_users):
        user_stats = store.get_user_statistics(username)
        
        if user_stats and user_stats.contribution_points:
            # Extract raw points for time-series plotting
            timestamps, counts = zip(*user_stats.contribution_points)
            
            ax1.plot(timestamps, counts, marker='o', label=username, 
                    color=colors[idx], linewidth=2, markersize=4, alpha=0.8)
    
    ax1.set_xlabel('Time', fontsize=12)
    ax1.set_ylabel('Cumulative Contributions', fontsize=12)
    ax1.set_title('User Contribution Growth', fontsize=14, fontweight='bold')
    ax1.legend(loc='upper left', fontsize=10)
    ax1.grid(True, alpha=0.3)
    
    # Format the time axis
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # Plot 2: Absolute contribution counts (Bar Chart)
    usernames = [u[0] for u in top_users]
    counts = [u[1] for u in top_users]
    
    bars = ax2.bar(range(len(usernames)), counts, color=colors[:len(usernames)], 
                    edgecolor='black', linewidth=1.5)
    ax2.set_xlabel('User', fontsize=12)
    ax2.set_ylabel('Total Contributions', fontsize=12)
    ax2.set_title('Total Contributions by User', fontsize=14, fontweight='bold')
    ax2.set_xticks(range(len(usernames)))
    ax2.set_xticklabels(usernames, rotation=45, ha='right')
    ax2.grid(True, axis='y', alpha=0.3)
    
    # Annotate bars with values
    for bar in bars:
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}',
                ha='center', va='bottom', fontweight='bold')
    
    plt.tight_layout()
    
    # Export the visualization
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"user_contributions_{timestamp}.png"
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    
    print(f"✅ Visualization saved to: {output_file}")
    
    try:
        plt.show(block=False)
        print("📊 Chart displayed (close window to continue)")
    except Exception:
        print("📊 Chart saved (graphical display not available)")
        plt.close()


# ============================================================================
# Core Processing Logic
# ============================================================================

async def process_stream_with_statistics(
    duration_seconds: int = None,
    tracked_users: Set[str] = None
):
    """
    The main processing loop implementing the functional reactive paradigm.
    
    Flow:
    1. Ingest raw events from the reactive stream.
    2. Transform raw events into ParsedEvent objects using the Maybe monad.
    3. If valid, trigger an IO monad to update the in-memory statistics store.
    
    Args:
        duration_seconds: Maximum time to run.
        tracked_users: Specific users to highlight in console output.
    """
    store = get_statistics_store()
    event_count = 0
    start_time = datetime.now()
    
    print("\n" + "=" * 80)
    print("Live Stream Processing Active")
    print("=" * 80)
    print("Press Ctrl+C to stop and view final analytics.\n")
    
    try:
        async for raw_event in stream_recent_changes():
            if shutdown_requested:
                break
            
            if duration_seconds:
                elapsed = (datetime.now() - start_time).total_seconds()
                if elapsed > duration_seconds:
                    break
            
            # Use Maybe monad for safe parsing
            maybe_parsed: Maybe[ParsedEvent] = parse_event(raw_event)
            
            if maybe_parsed.is_some():
                parsed_event = maybe_parsed.get_or_else(None)
                
                # Encapsulate state update in IO monad
                io_update = store.update_user_statistics(parsed_event)
                io_update.run()
                
                event_count += 1
                
                if event_count % 10 == 0:
                    print(f"Processed {event_count} events...")
                
                # Check for tracked users
                if tracked_users and parsed_event.user in tracked_users:
                    print(f"\n[ALERT] Tracked user {parsed_event.user} edited '{parsed_event.title}'")
                    print(f"  Action: {parsed_event.edit_type.value}, Δ: {parsed_event.diff_size} chars")
    
    except Exception as e:
        print(f"\nStreaming error encountered: {e}")
    
    finally:
        print(f"\nProcessing stopped. Total events ingested: {event_count}")


async def demo_basic_streaming(max_events: int = 50):
    """
    Demonstrates basic real-time streaming of Wikipedia changes.
    (Satisfies Requirement #1)
    """
    print("\n" + "=" * 80)
    print("DEMO 1: Real-Time Stream Ingestion")
    print("=" * 80)
    
    count = 0
    async for event in take_stream(stream_recent_changes(), max_events):
        maybe_parsed = parse_event(event)
        
        if maybe_parsed.is_some():
            parsed = maybe_parsed.get_or_else(None)
            count += 1
            print(f"{count:02d}. [{parsed.user}] -> '{parsed.title}' "
                  f"({parsed.edit_type.value}, {parsed.diff_size:+d} chars)")


async def demo_user_tracking(username: str, max_events: int = 100):
    """
    Demonstrates targeted tracking of a specific user.
    (Satisfies Requirement #2)
    """
    print("\n" + "=" * 80)
    print(f"DEMO 2: Target User Tracking")
    print("=" * 80)
    print(f"Monitoring: {username}\n")
    
    store = get_statistics_store()
    found_count = 0
    total_checked = 0
    
    async for event in take_stream(stream_recent_changes(), max_events):
        total_checked += 1
        maybe_parsed = parse_event(event)
        
        if maybe_parsed.is_some():
            parsed = maybe_parsed.get_or_else(None)
            
            # Maintain stats for comparison
            store.update_user_statistics(parsed).run()
            
            if parsed.user == username:
                found_count += 1
                print(f"MATCH [{found_count}]: {parsed.user} edited '{parsed.title}'")
    
    print(f"\nScan complete. Found {found_count} events for {username} out of {total_checked} checked.")


async def demo_statistics_queries():
    """
    Demonstrates statistical retrieval and visualization using functional patterns.
    (Satisfies Requirement #3)
    """
    print("\n" + "=" * 80)
    print("DEMO 3: Statistical Analysis & Visualization")
    print("=" * 80)
    
    print("\nCollecting sample data (100 events)...")
    store = get_statistics_store()
    
    event_count = 0
    async for event in take_stream(stream_recent_changes(), 100):
        maybe_parsed = parse_event(event)
        if maybe_parsed.is_some():
            parsed = maybe_parsed.get_or_else(None)
            store.update_user_statistics(parsed).run()
            event_count += 1
            if event_count % 25 == 0:
                print(f"  Collected {event_count}...")
    
    print("\n--- Summary Analytics ---")
    print_either_result(get_statistics_summary(store), "Global Stats")
    
    # Requirement 3.4
    print("\n--- Active Users (Last Hour) ---")
    active_users_result = get_most_active_users(TimePeriod.HOUR, limit=5, store=store)
    if active_users_result.is_right():
        for i, (name, count) in enumerate(active_users_result.get_right(), 1):
            print(f"{i}. {name}: {count} edits")
    
    # Requirement 3.5
    print("\n--- Typo Correction Hotspots ---")
    typo_topics_result = get_top_typo_topics(limit=5, store=store)
    if typo_topics_result.is_right():
        for i, (topic, count) in enumerate(typo_topics_result.get_right(), 1):
            print(f"{i}. {topic}: {count} corrections")
    
    # Requirement 3.1 - 3.3 for the top user
    if active_users_result.is_right() and active_users_result.get_right():
        top_user = active_users_result.get_right()[0][0]
        print(f"\n--- Deep Dive: User '{top_user}' ---")
        
        # Contribution types
        types_res = get_user_contribution_types(top_user, store)
        if types_res.is_right():
            t = types_res.get_right()
            print(f"  Typo Fixes: {t['typo_edits']} | Content: {t['content_additions']}")
            
        # Top topics
        topics_res = get_user_top_topics(top_user, limit=3, store=store)
        if topics_res.is_right():
            print(f"  Primary Topics: {', '.join([item[0] for item in topics_res.get_right()])}")
    
    # Requirement: Visualization
    create_contribution_visualizations(active_users_result, store)


async def demo_reactive_pipeline(max_events: int = 50):
    """
    Demonstrates the construction and execution of a reactive pipeline.
    
    Showcases:
    - Composable operators.
    - Declarative data flow.
    - Deduplication and rate limiting.
    """
    print("\n" + "=" * 80)
    print("DEMO 4: Reactive Pipeline Composition")
    print("=" * 80)
    
    store = get_statistics_store()
    pipeline = create_basic_pipeline(limit=max_events)
    
    count = 0
    async with pipeline.stream() as streamer:
        async for raw_event in streamer:
            maybe_parsed = parse_event(raw_event)
            if maybe_parsed.is_some():
                parsed = maybe_parsed.get_or_else(None)
                store.update_user_statistics(parsed).run()
                count += 1
                print(f"Pipeline -> Event {count:02d}: {parsed.user}")

    print("\nPipeline execution finished.")


async def demo_pure_functional_pipeline(max_events: int = 20):
    """
    Demonstrates a pure functional pipeline without state modifications.
    Uses aiostream.pipe.accumulate with FunctionalStore.
    """
    from user_statistics import FunctionalStore
    from event_processor import parse_stream
    
    print("\n" + "=" * 80)
    print("DEMO 6: Pure Functional Reactive Pipeline")
    print("=" * 80)
    
    # Constructing the pure functional pipeline
    pipeline = (
        fetch_wikipedia_changes()
        | aiostream.pipe.delay(0.1)
        | parse_stream.pipe()
        | aiostream.pipe.take(max_events)
        | aiostream.pipe.accumulate(
            lambda store, event: store.update(event), 
            initializer=FunctionalStore()
        )
    )
    
    print(f"Processing {max_events} events using immutable state accumulation...")
    
    # The stream now yields FunctionalStore instances (each being a new state)
    last_store = None
    async with pipeline.stream() as streamer:
        async for store in streamer:
            last_store = store
            summary = store.get_summary()
            if summary['total_events'] % 5 == 0:
                print(f"  - State updated: {summary['total_events']} events, {summary['total_users']} users")
            
    if last_store:
        summary = last_store.get_summary()
        print("\nFinal Statistics Summary (Pure Functional State):")
        print(f"  - Total Events: {summary['total_events']}")
        print(f"  - Total Users: {summary['total_users']}")
        print(f"  - Total Topics: {summary['total_topics']}")


# ============================================================================
# Entry Point
# ============================================================================

async def main():
    """Application CLI interface."""
    
    print("\n" + "█" * 80)
    print("  WIKIPEDIA REAL-TIME STREAM ANALYZER")
    print("  Functional Reactive Programming Project")
    print("█" * 80)
    
    print("\nMenu:")
    print("1. Basic Stream Ingestion (50 events)")
    print("2. Targeted User Tracking")
    print("3. Analytics & Growth Visualizations")
    print("4. Live Continuous Monitor")
    print("5. Reactive Pipeline Architecture Demo")
    print("6. Pure Functional Pipeline (Immutable State)")
    print()
    
    mode = input("Select mode (1-6): ").strip()
    
    try:
        if mode == "1":
            await demo_basic_streaming(50)
        elif mode == "2":
            user = input("Username to track (e.g., 'ClueBot NG'): ").strip()
            await demo_user_tracking(user or "ClueBot NG", 100)
        elif mode == "3":
            await demo_statistics_queries()
        elif mode == "4":
            await process_stream_with_statistics(duration_seconds=60)
            await demo_statistics_queries()
        elif mode == "6":
            await demo_pure_functional_pipeline(30)
        else:
            await demo_reactive_pipeline(50)
    
    except KeyboardInterrupt:
        print("\nProcess interrupted.")
    except Exception as e:
        print(f"\nExecution error: {e}")
    
    print("\n" + "=" * 80)
    print("Session Ended")
    print("=" * 80 + "\n")


if __name__ == '__main__':
    asyncio.run(main())
