"""
User Statistics Storage and Aggregation

In-memory storage for user contribution statistics.
Provides time-series data with configurable granularity.
"""

from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime, timedelta
from collections import defaultdict, Counter
from enum import Enum
import threading
from event_processor import ParsedEvent, EditType
from functional_utils import IO, io_pure


class TimeGranularity(Enum):
    """Time granularity for contribution series"""
    HOUR = "hour"
    DAY = "day"
    MONTH = "month"
    YEAR = "year"


class TimePeriod(Enum):
    """Time period for querying most active users"""
    HOUR = "hour"
    DAY = "day"
    MONTH = "month"
    YEAR = "year"


class UserStatistics:
    """
    Statistics for a single user.
    Immutable operations - creates new instances rather than mutating.
    """
    
    def __init__(self):
        # List of (timestamp, cumulative_count) for contribution series
        self.contribution_points: List[Tuple[datetime, int]] = []
        
        # Counter for topics the user has edited
        self.topic_contributions: Counter = Counter()
        
        # Counters for edit types
        self.typo_edits: int = 0
        self.content_additions: int = 0
        self.minor_edits: int = 0
        self.bot_edits: int = 0
        
        # Raw timestamps for all contributions
        self.timestamps: List[datetime] = []
        
        # Total contribution count
        self.total_contributions: int = 0
    
    def add_contribution(self, event: ParsedEvent) -> 'UserStatistics':
        """
        Add a contribution and return updated statistics.
        
        Functional approach: returns new instance rather than mutating.
        
        Args:
            event: Parsed event to add
            
        Returns:
            New UserStatistics instance with updated data
        """
        new_stats = UserStatistics()
        
        # Copy existing data
        new_stats.contribution_points = self.contribution_points.copy()
        new_stats.topic_contributions = self.topic_contributions.copy()
        new_stats.typo_edits = self.typo_edits
        new_stats.content_additions = self.content_additions
        new_stats.minor_edits = self.minor_edits
        new_stats.bot_edits = self.bot_edits
        new_stats.timestamps = self.timestamps.copy()
        new_stats.total_contributions = self.total_contributions
        
        # Add new contribution
        new_stats.total_contributions += 1
        new_stats.timestamps.append(event.timestamp)
        new_stats.contribution_points.append((event.timestamp, new_stats.total_contributions))
        
        # Update topic counter
        new_stats.topic_contributions[event.title] += 1
        
        # Update edit type counters
        if event.edit_type == EditType.TYPO_EDITING:
            new_stats.typo_edits += 1
        elif event.edit_type == EditType.CONTENT_ADDITION:
            new_stats.content_additions += 1
        elif event.edit_type == EditType.MINOR_EDIT:
            new_stats.minor_edits += 1
        elif event.edit_type == EditType.BOT_EDIT:
            new_stats.bot_edits += 1
        
        return new_stats
    
    def get_contribution_series(
        self,
        granularity: TimeGranularity
    ) -> List[Tuple[datetime, int]]:
        """
        Get contribution series with specified time granularity.
        
        Returns data points where X=time and Y=total contributions at that time.
        
        Args:
            granularity: Time granularity (HOUR, DAY, MONTH, YEAR)
            
        Returns:
            List of (timestamp, cumulative_count) tuples
        """
        if not self.contribution_points:
            return []
        
        # Aggregate based on granularity
        aggregated = {}
        
        for timestamp, count in self.contribution_points:
            bucket_time = _round_to_granularity(timestamp, granularity)
            aggregated[bucket_time] = count
        
        # Sort by time and return
        return sorted(aggregated.items(), key=lambda x: x[0])
    
    def get_top_topics(self, limit: int = 10) -> List[Tuple[str, int]]:
        """
        Get top N topics user has contributed to.
        
        Args:
            limit: Maximum number of topics to return
            
        Returns:
            List of (topic, count) tuples
        """
        return self.topic_contributions.most_common(limit)


class FunctionalStore:
    """
    A pure functional, immutable store for user statistics.
    Every update returns a new instance of the store.
    """
    
    def __init__(
        self, 
        users: Optional[Dict[str, UserStatistics]] = None, 
        topic_typo_counts: Optional[Counter] = None, 
        all_events: Optional[List[ParsedEvent]] = None
    ):
        self.users = users or {}
        self.topic_typo_counts = topic_typo_counts or Counter()
        self.all_events = all_events or []
    
    def update(self, event: ParsedEvent) -> 'FunctionalStore':
        """
        Pure functional update: returns a new FunctionalStore.
        """
        # Update user statistics
        new_users = self.users.copy()
        user_stats = new_users.get(event.user, UserStatistics())
        new_users[event.user] = user_stats.add_contribution(event)
        
        # Update typo counts
        new_topic_typo_counts = self.topic_typo_counts.copy()
        if event.edit_type == EditType.TYPO_EDITING:
            new_topic_typo_counts[event.title] += 1
            
        # Add to event list (using a copy)
        new_all_events = self.all_events + [event]
        
        return FunctionalStore(new_users, new_topic_typo_counts, new_all_events)
    
    def get_summary(self) -> Dict[str, Any]:
        return {
            'total_users': len(self.users),
            'total_events': len(self.all_events),
            'total_topics': len(self.topic_typo_counts),
        }


class StatisticsStore:

    """
    Thread-safe in-memory store for all user statistics.
    
    Uses IO monad pattern for side effects.
    """
    
    def __init__(self):
        self._users: Dict[str, UserStatistics] = {}
        self._lock = threading.Lock()
        
        # Global statistics for cross-user queries
        self._topic_typo_counts: Counter = Counter()  # (topic -> typo_edit_count)
        self._all_events: List[ParsedEvent] = []  # For time-based queries
    
    def update_user_statistics(self, event: ParsedEvent) -> IO[None]:
        """
        Update statistics for a user based on an event.
        
        IO MONAD: Wraps the side effect of updating storage.
        
        Args:
            event: Parsed event
            
        Returns:
            IO[None]: IO monad wrapping the update operation
        """
        def update():
            with self._lock:
                # Get or create user statistics
                if event.user not in self._users:
                    self._users[event.user] = UserStatistics()
                
                # Update user statistics (functional style - returns new instance)
                self._users[event.user] = self._users[event.user].add_contribution(event)
                
                # Update global statistics
                if event.edit_type == EditType.TYPO_EDITING:
                    self._topic_typo_counts[event.title] += 1
                
                self._all_events.append(event)
        
        return IO(update)
    
    def get_user_statistics(self, username: str) -> Optional[UserStatistics]:
        """
        Get statistics for a specific user.
        
        Args:
            username: Username to look up
            
        Returns:
            UserStatistics or None if user not found
        """
        with self._lock:
            return self._users.get(username)
    
    def user_exists(self, username: str) -> bool:
        """
        Check if user has been observed.
        
        Args:
            username: Username to check
            
        Returns:
            True if user has contributions
        """
        with self._lock:
            return username in self._users
    
    def get_all_users(self) -> List[str]:
        """
        Get list of all observed users.
        
        Returns:
            List of usernames
        """
        with self._lock:
            return list(self._users.keys())
    
    def get_most_active_users(
        self,
        period: TimePeriod,
        limit: int = 10
    ) -> List[Tuple[str, int]]:
        """
        Get most active users during a time period.
        
        Args:
            period: Time period (HOUR, DAY, MONTH, YEAR)
            limit: Maximum number of users to return
            
        Returns:
            List of (username, contribution_count) tuples
        """
        now = datetime.now()
        cutoff = _get_period_cutoff(now, period)
        
        with self._lock:
            user_counts = Counter()
            
            # Count contributions in the time period
            for event in self._all_events:
                if event.timestamp >= cutoff:
                    user_counts[event.user] += 1
            
            return user_counts.most_common(limit)
    
    def get_top_typo_topics(self, limit: int = 10) -> List[Tuple[str, int]]:
        """
        Get topics with most typo edits.
        
        Args:
            limit: Maximum number of topics to return
            
        Returns:
            List of (topic, typo_edit_count) tuples
        """
        with self._lock:
            return self._topic_typo_counts.most_common(limit)
    
    def get_statistics_summary(self) -> Dict[str, any]:
        """
        Get overall statistics summary.
        
        Returns:
            Dict with summary statistics
        """
        with self._lock:
            return {
                'total_users': len(self._users),
                'total_events': len(self._all_events),
                'total_topics': len(self._topic_typo_counts),
            }


# ============================================================================
# Helper Functions
# ============================================================================

def _round_to_granularity(dt: datetime, granularity: TimeGranularity) -> datetime:
    """
    Round datetime to specified granularity.
    
    Args:
        dt: Datetime to round
        granularity: Target granularity
        
    Returns:
        Rounded datetime
    """
    if granularity == TimeGranularity.HOUR:
        return dt.replace(minute=0, second=0, microsecond=0)
    elif granularity == TimeGranularity.DAY:
        return dt.replace(hour=0, minute=0, second=0, microsecond=0)
    elif granularity == TimeGranularity.MONTH:
        return dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    elif granularity == TimeGranularity.YEAR:
        return dt.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    
    return dt


def _get_period_cutoff(now: datetime, period: TimePeriod) -> datetime:
    """
    Get cutoff datetime for a time period.
    
    Args:
        now: Current datetime
        period: Time period
        
    Returns:
        Cutoff datetime (events after this are included)
    """
    if period == TimePeriod.HOUR:
        return now - timedelta(hours=1)
    elif period == TimePeriod.DAY:
        return now - timedelta(days=1)
    elif period == TimePeriod.MONTH:
        return now - timedelta(days=30)
    elif period == TimePeriod.YEAR:
        return now - timedelta(days=365)
    
    return now


# Global instance (singleton pattern)
_global_store = None


def get_statistics_store() -> StatisticsStore:
    """
    Get the global statistics store instance.
    
    Returns:
        StatisticsStore: Global store
    """
    global _global_store
    if _global_store is None:
        _global_store = StatisticsStore()
    return _global_store


def reset_statistics_store():
    """Reset the global store (useful for testing)"""
    global _global_store
    _global_store = StatisticsStore()
