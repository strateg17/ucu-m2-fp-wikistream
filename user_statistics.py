"""
User Statistics Storage and Aggregation (Pure Functional)

In-memory storage for user contribution statistics using immutable patterns.
Every update returns a new state instance.
"""

from typing import Dict, List, Tuple, Optional, Any
from datetime import datetime, timedelta
from collections import Counter
from enum import Enum
from event_processor import ParsedEvent, EditType


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
    Immutable: add_contribution returns a NEW instance.
    """
    
    def __init__(
        self,
        contribution_points: List[Tuple[datetime, int]] = None,
        topic_contributions: Counter = None,
        typo_edits: int = 0,
        content_additions: int = 0,
        minor_edits: int = 0,
        bot_edits: int = 0,
        timestamps: List[datetime] = None,
        total_contributions: int = 0
    ):
        self.contribution_points = contribution_points or []
        self.topic_contributions = topic_contributions or Counter()
        self.typo_edits = typo_edits
        self.content_additions = content_additions
        self.minor_edits = minor_edits
        self.bot_edits = bot_edits
        self.timestamps = timestamps or []
        self.total_contributions = total_contributions
    
    def add_contribution(self, event: ParsedEvent) -> 'UserStatistics':
        """Pure functional update."""
        new_total = self.total_contributions + 1
        new_timestamps = self.timestamps + [event.timestamp]
        new_points = self.contribution_points + [(event.timestamp, new_total)]
        
        new_topics = self.topic_contributions.copy()
        new_topics[event.title] += 1
        
        # Incremental counters
        typo = self.typo_edits + (1 if event.edit_type == EditType.TYPO_EDITING else 0)
        content = self.content_additions + (1 if event.edit_type == EditType.CONTENT_ADDITION else 0)
        minor = self.minor_edits + (1 if event.is_minor else 0)
        bot = self.bot_edits + (1 if event.is_bot else 0)
        
        return UserStatistics(
            contribution_points=new_points,
            topic_contributions=new_topics,
            typo_edits=typo,
            content_additions=content,
            minor_edits=minor,
            bot_edits=bot,
            timestamps=new_timestamps,
            total_contributions=new_total
        )
    
    def get_contribution_series(self, granularity: TimeGranularity) -> List[Tuple[datetime, int]]:
        if not self.contribution_points:
            return []
        aggregated = {}
        for timestamp, count in self.contribution_points:
            bucket_time = _round_to_granularity(timestamp, granularity)
            aggregated[bucket_time] = count
        return sorted(aggregated.items(), key=lambda x: x[0])

    def get_top_topics(self, limit: int = 10) -> List[Tuple[str, int]]:
        return self.topic_contributions.most_common(limit)


class StatisticsStore:
    """
    Pure functional, immutable store for all user statistics.
    """
    
    def __init__(
        self, 
        users: Dict[str, UserStatistics] = None, 
        topic_typo_counts: Counter = None, 
        all_events: List[ParsedEvent] = None
    ):
        self.users = users or {}
        self.topic_typo_counts = topic_typo_counts or Counter()
        self.all_events = all_events or []
    
    def update(self, event: ParsedEvent) -> 'StatisticsStore':
        """Returns a new StatisticsStore instance."""
        new_users = self.users.copy()
        user_stats = new_users.get(event.user, UserStatistics())
        new_users[event.user] = user_stats.add_contribution(event)
        
        new_typos = self.topic_typo_counts.copy()
        if event.edit_type == EditType.TYPO_EDITING:
            new_typos[event.title] += 1
            
        new_events = self.all_events + [event]
        
        return StatisticsStore(new_users, new_typos, new_events)
    
    def get_summary(self) -> Dict[str, Any]:
        return {
            'total_users': len(self.users),
            'total_events': len(self.all_events),
            'total_topics': len(self.topic_typo_counts),
        }


def _round_to_granularity(dt: datetime, granularity: TimeGranularity) -> datetime:
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
    if period == TimePeriod.HOUR:
        return now - timedelta(hours=1)
    elif period == TimePeriod.DAY:
        return now - timedelta(days=1)
    elif period == TimePeriod.MONTH:
        return now - timedelta(days=30)
    elif period == TimePeriod.YEAR:
        return now - timedelta(days=365)
    return now
