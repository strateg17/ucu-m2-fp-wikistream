"""
Wikipedia Real-Time Stream Analyzer - Main Demo

Demonstrates the functional reactive programming approach for Wikipedia stream analysis.
NOW USING AIOSTREAM following professor's example from lesson14.py

This implements all requirements from the task:
1. Get all Recent Changes as a real-time stream
2. Track in real-time the activity of particular users
3. Retrieve statistics for users

NO API KEY REQUIRED: WikiMedia EventStream is freely accessible!
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

# Import our functional programming modules
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
    """Handle Ctrl+C gracefully"""
    global shutdown_requested
    print("\n\nShutdown requested. Finishing current processing...")
    shutdown_requested = True


# Register signal handler
signal.signal(signal.SIGINT, signal_handler)


# ============================================================================
# Visualization Functions
# ============================================================================

def create_contribution_visualizations(active_users_result, store):
    """
    Create line chart visualizations of user contributions over time.
    
    Shows cumulative contributions for top users.
    Saves charts to 'visualizations/' directory.
    """
    
    if not active_users_result.is_right():
        print("\n⚠️  No users to visualize")
        return
    
    users = active_users_result.get_right()
    if not users:
        print("\n⚠️  No users found")
        return
    
    # Create output directory
    output_dir = Path("visualizations")
    output_dir.mkdir(exist_ok=True)
    
    # Take top 5 users for visualization
    top_users = users[:5]
    
    print(f"\n📈 Creating line chart for top {len(top_users)} users...")
    
    # Create figure with subplots
    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10))
    fig.suptitle('Wikipedia User Contribution Analysis', fontsize=16, fontweight='bold')
    
    # Plot 1: Individual user contribution series (line chart)
    colors = plt.cm.Set3(range(len(top_users)))
    
    for idx, (username, total_count) in enumerate(top_users):
        # Get user statistics directly to access raw timestamps
        user_stats = store.get_user_statistics(username)
        
        if user_stats and user_stats.contribution_points:
            # Use raw contribution points (timestamp, cumulative_count)
            # This shows the actual progression over time
            timestamps, counts = zip(*user_stats.contribution_points)
            
            # Plot the line
            ax1.plot(timestamps, counts, marker='o', label=username, 
                    color=colors[idx], linewidth=2, markersize=4, alpha=0.8)
    
    ax1.set_xlabel('Time', fontsize=12)
    ax1.set_ylabel('Cumulative Contributions', fontsize=12)
    ax1.set_title('User Contributions Over Time (Cumulative)', fontsize=14, fontweight='bold')
    ax1.legend(loc='upper left', fontsize=10)
    ax1.grid(True, alpha=0.3)
    
    # Format time axis based on data range
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45, ha='right')
    
    # Plot 2: Total contributions bar chart
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
    
    # Add value labels on bars
    for bar in bars:
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{int(height)}',
                ha='center', va='bottom', fontweight='bold')
    
    plt.tight_layout()
    
    # Save figure
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = output_dir / f"user_contributions_{timestamp}.png"
    plt.savefig(output_file, dpi=300, bbox_inches='tight')
    
    print(f"✅ Visualization saved to: {output_file}")
    
    # Try to display (will work in some environments)
    try:
        plt.show(block=False)
        print("📊 Chart displayed (close window to continue)")
    except:
        print("📊 Chart saved (display not available in this environment)")
        plt.close()


# ============================================================================
# Main Processing Loop - Functional Reactive Programming
# ============================================================================

async def process_stream_with_statistics(
    duration_seconds: int = None,
    tracked_users: Set[str] = None
):
    """
    Main processing loop demonstrating functional reactive programming.
    
    This follows the pattern from your professor's example:
    - Async stream as reactive data source
    - Functor operations (map) for transformations
    - Monad operations (flat_map) for error handling
    - IO monad for side effects
    
    Args:
        duration_seconds: How long to run (None = indefinitely)
        tracked_users: Set of users to track specifically
    """
    store = get_statistics_store()
    event_count = 0
    start_time = datetime.now()
    
    print("=" * 80)
    print("Wikipedia Real-Time Stream Analyzer")
    print("Functional Reactive Programming Demo")
    print("=" * 80)
    print("\nConnecting to WikiMedia EventStream API...")
    print("Press Ctrl+C to stop and see statistics.\n")
    
    try:
        # Stream recent changes (requirement #1)
        async for raw_event in stream_recent_changes():
            
            if shutdown_requested:
                break
            
            # Check duration limit
            if duration_seconds:
                elapsed = (datetime.now() - start_time).total_seconds()
                if elapsed > duration_seconds:
                    break
            
            # FUNCTOR: Map raw event to parsed event using Maybe monad
            maybe_parsed: Maybe[ParsedEvent] = parse_event(raw_event)
            
            # MONAD: Chain operations with Maybe
            if maybe_parsed.is_some():
                parsed_event = maybe_parsed.get_or_else(None)
                
                # IO MONAD: Execute side effect of updating statistics
                io_update = store.update_user_statistics(parsed_event)
                io_update.run()
                
                event_count += 1
                
                # Print progress every 10 events
                if event_count % 10 == 0:
                    print(f"Processed {event_count} events...")
                
                # Track specific users (requirement #2)
                if tracked_users and parsed_event.user in tracked_users:
                    print(f"\n[TRACKED USER] {parsed_event.user} edited '{parsed_event.title}'")
                    print(f"  Type: {parsed_event.edit_type.value}, Diff: {parsed_event.diff_size} chars")
    
    except Exception as e:
        print(f"\nError during streaming: {e}")
    
    finally:
        print(f"\n{'=' * 80}")
        print(f"Stream processing complete. Processed {event_count} events.")
        print(f"{'=' * 80}\n")


async def demo_basic_streaming(max_events: int = 50):
    """
    Simple demo: stream and parse events.
    
    Demonstrates requirement #1: Get all Recent Changes as real-time stream.
    """
    print("\n" + "=" * 80)
    print("DEMO 1: Basic Streaming (Requirement #1)")
    print("=" * 80)
    print(f"Streaming first {max_events} events...\n")
    
    count = 0
    async for event in take_stream(stream_recent_changes(), max_events):
        # Parse with Maybe monad
        maybe_parsed = parse_event(event)
        
        if maybe_parsed.is_some():
            parsed = maybe_parsed.get_or_else(None)
            count += 1
            print(f"{count}. [{parsed.user}] edited '{parsed.title}' "
                  f"({parsed.edit_type.value}, {parsed.diff_size:+d} chars)")


async def demo_user_tracking(username: str, max_events: int = 100):
    """
    Track a specific user's contributions.
    
    Demonstrates requirement #2: Track activity of particular user.
    """
    print("\n" + "=" * 80)
    print(f"DEMO 2: User Tracking (Requirement #2)")
    print("=" * 80)
    print(f"Tracking user: {username}")
    print(f"Will monitor up to {max_events} events...\n")
    
    store = get_statistics_store()
    found_count = 0
    total_checked = 0
    
    async for event in take_stream(stream_recent_changes(), max_events):
        total_checked += 1
        maybe_parsed = parse_event(event)
        
        if maybe_parsed.is_some():
            parsed = maybe_parsed.get_or_else(None)
            
            # Update statistics
            io_update = store.update_user_statistics(parsed)
            io_update.run()
            
            # Check if this is our tracked user
            if parsed.user == username:
                found_count += 1
                print(f"\n[{found_count}] {parsed.user} edited '{parsed.title}'")
                print(f"    Type: {parsed.edit_type.value}")
                print(f"    Diff: {parsed.diff_size:+d} chars")
                print(f"    Time: {parsed.timestamp}")
    
    print(f"\n\nFound {found_count} edits by {username} in {total_checked} events.")


async def demo_statistics_queries():
    """
    Demonstrate statistical queries using Either monad.
    
    Demonstrates requirement #3: Retrieve user statistics.
    NOW WITH VISUALIZATIONS!
    """
    print("\n" + "=" * 80)
    print("DEMO 3: Statistical Queries (Requirement #3)")
    print("=" * 80)
    
    # First collect some data
    print("\nCollecting data from stream (100 events)...\n")
    store = get_statistics_store()
    
    event_count = 0
    async for event in take_stream(stream_recent_changes(), 100):
        maybe_parsed = parse_event(event)
        if maybe_parsed.is_some():
            parsed = maybe_parsed.get_or_else(None)
            io_update = store.update_user_statistics(parsed)
            io_update.run()
            event_count += 1
            
            if event_count % 20 == 0:
                print(f"  Processed {event_count} events...")
    
    print(f"\nData collection complete! Processed {event_count} events.\n")
    
    # Query 1: Overall summary
    print("\n--- Overall Summary ---")
    summary_result = get_statistics_summary(store)
    print_either_result(summary_result, "Summary")
    
    # Query 2: Most active users (requirement #3.4)
    print("\n--- Most Active Users (Last Hour) ---")
    active_users_result = get_most_active_users(TimePeriod.HOUR, limit=5, store=store)
    if active_users_result.is_right():
        users = active_users_result.get_right()
        for i, (username, count) in enumerate(users, 1):
            print(f"{i}. {username}: {count} contributions")
    else:
        print(f"Error: {active_users_result.get_left()}")
    
    # Query 3: Top typo topics (requirement #3.5)
    print("\n--- Top Topics with Typo Edits ---")
    typo_topics_result = get_top_typo_topics(limit=5, store=store)
    if typo_topics_result.is_right():
        topics = typo_topics_result.get_right()
        for i, (topic, count) in enumerate(topics, 1):
            print(f"{i}. {topic}: {count} typo edits")
    else:
        print(f"Error: {typo_topics_result.get_left()}")
    
    # Query 4: User-specific statistics (if we have users)
    if active_users_result.is_right():
        users = active_users_result.get_right()
        if users:
            username = users[0][0]  # Most active user
            
            print(f"\n--- Statistics for User: {username} ---")
            
            # Contribution types (requirement #3.3)
            types_result = get_user_contribution_types(username, store)
            if types_result.is_right():
                types = types_result.get_right()
                print(f"\nContribution Types:")
                print(f"  Typo edits: {types['typo_edits']}")
                print(f"  Content additions: {types['content_additions']}")
                print(f"  Minor edits: {types['minor_edits']}")
                print(f"  Total: {types['total']}")
            
            # Top topics (requirement #3.2)
            topics_result = get_user_top_topics(username, limit=3, store=store)
            if topics_result.is_right():
                topics = topics_result.get_right()
                print(f"\nTop Topics:")
                for i, (topic, count) in enumerate(topics, 1):
                    print(f"  {i}. {topic}: {count} edits")
            
            # Contribution series (requirement #3.1)
            series_result = get_user_contribution_series(
                username, 
                TimeGranularity.HOUR, 
                store
            )
            if series_result.is_right():
                series = series_result.get_right()
                print(f"\nContribution Series (by hour): {len(series)} data points")
                # Show first few points
                for timestamp, count in series[:3]:
                    print(f"  {timestamp}: {count} total contributions")
    
    # NEW: Create visualizations
    print("\n" + "=" * 80)
    print("📊 CREATING VISUALIZATIONS")
    print("=" * 80)
    
    create_contribution_visualizations(active_users_result, store)


# ============================================================================
# New Demo Using AIOSTREAM Pipeline Pattern (Professor's Style)
# ============================================================================

async def demo_aiostream_pipeline(max_events: int = 50):
    """
    Demo using aiostream pipeline pattern like professor's lesson14.py.
    
    This demonstrates the functional reactive programming approach with:
    - @operator and @pipable_operator decorators
    - Pipeline composition with | operator
    - Functional transformations
    """
    print("\n" + "=" * 80)
    print("DEMO: AIOSTREAM PIPELINE (Professor's Pattern from lesson14.py)")
    print("=" * 80)
    print(f"Processing {max_events} events using functional pipelines...\n")
    
    store = get_statistics_store()
    count = 0
    
    # Create pipeline following professor's pattern
    pipeline = create_basic_pipeline(limit=max_events)
    
    print("Pipeline created with:")
    print("  1. fetch_wikipedia_changes() - stream source")
    print("  2. delay(0.1) - rate limiting")
    print("  3. deduplicator - remove duplicates")
    print("  4. take(50) - limit events\n")
    
    # Execute pipeline
    async with pipeline.stream() as streamer:
        async for raw_event in streamer:
            # Parse with Maybe monad
            maybe_parsed = parse_event(raw_event)
            
            if maybe_parsed.is_some():
                parsed = maybe_parsed.get_or_else(None)
                
                # Update statistics with IO monad
                io_update = store.update_user_statistics(parsed)
                io_update.run()
                
                count += 1
                print(f"{count}. [{parsed.user}] edited '{parsed.title}' "
                      f"({parsed.edit_type.value}, {parsed.diff_size:+d} chars)")
    
    print(f"\n\nProcessed {count} events using aiostream pipeline!")
    print("This demonstrates functional reactive programming with:")
    print("  ✓ @operator decorator for stream sources")
    print("  ✓ @pipable_operator for transformations")
    print("  ✓ Pipeline composition with | operator")
    print("  ✓ Functional programming patterns")


# ============================================================================
# Main Entry Point
# ============================================================================

async def main():
    """Main entry point with menu"""
    
    print("\n" + "=" * 80)
    print("Wikipedia Real-Time Stream Analyzer")
    print("Functional Programming Course Project")
    print("Implementation using AIOSTREAM (Professor's Pattern from lesson14.py)")
    print("=" * 80)
    
    print("\n📢 NOTE: WikiMedia EventStream API does NOT require an API key!")
    print("         It's freely accessible - just start streaming! 🎉\n")
    
    print("\nSelect demo mode:")
    print("1. Basic streaming (50 events)")
    print("2. User tracking (monitor specific user)")
    print("3. Statistical queries (collect 100 events and query with visualizations)")
    print("4. Full demo (continuous streaming with live stats)")
    print("5. AIOSTREAM Pipeline Demo (Professor's pattern - default)")
    print()
    
    mode = input("Enter mode (1-5) or press Enter for mode 5: ").strip()
    
    if not mode:
        mode = "5"
    
    try:
        if mode == "1":
            await demo_basic_streaming(max_events=50)
        
        elif mode == "2":
            username = input("Enter username to track: ").strip()
            if not username:
                username = "ClueBot NG"  # A bot that's usually active
            await demo_user_tracking(username, max_events=100)
        
        elif mode == "3":
            await demo_statistics_queries()
        
        elif mode == "4":
            duration = input("Duration in seconds (press Enter for 60): ").strip()
            duration = int(duration) if duration else 60
            
            users_input = input("Users to track (comma-separated, or Enter for none): ").strip()
            tracked_users = set(u.strip() for u in users_input.split(",")) if users_input else None
            
            await process_stream_with_statistics(
                duration_seconds=duration,
                tracked_users=tracked_users
            )
            
            # Show statistics after streaming
            print("\n\nFinal Statistics:")
            await demo_statistics_queries()
        
        elif mode == "5":
            await demo_aiostream_pipeline(max_events=50)
            
            # Show some statistics
            print("\n\nShowing statistics...")
            await demo_statistics_queries()
        
        else:
            print("Invalid mode. Running default demo (mode 5)...")
            await demo_aiostream_pipeline(max_events=50)
    
    except KeyboardInterrupt:
        print("\n\nInterrupted by user.")
    except Exception as e:
        print(f"\n\nError: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "=" * 80)
    print("Demo complete!")
    print("=" * 80 + "\n")


if __name__ == '__main__':
    # Run the async main function
    asyncio.run(main())
