"""
Query API for User Statistics (Functional)

Provides a functional interface for querying the immutable StatisticsStore.
Uses Either monad for error handling.
"""

from typing import List, Tuple, Dict, Any, Optional
from collections import Counter
from datetime import datetime
from functional_utils import Either, Left, Right
from user_statistics import (
    StatisticsStore, 
    TimeGranularity,
    TimePeriod,
    _get_period_cutoff
)


# ============================================================================
# Query Functions - All return Either monad for error handling
# ============================================================================

def get_user_contribution_series(
    username: str,
    store: StatisticsStore,
    granularity: TimeGranularity = TimeGranularity.DAY
) -> Either[str, List[Tuple[Any, int]]]:
    """
    Get user contribution time series from immutable store.
    """
    if username not in store.users:
        return Left(f"User '{username}' not found.")
    
    user_stats = store.users.get(username)
    series = user_stats.get_contribution_series(granularity)
    
    if not series:
        return Left(f"User '{username}' has no contribution data.")
    
    return Right(series)


def get_user_top_topics(
    username: str,
    store: StatisticsStore,
    limit: int = 10
) -> Either[str, List[Tuple[str, int]]]:
    """
    Get top topics for a user.
    """
    if username not in store.users:
        return Left(f"User '{username}' not found.")
    
    user_stats = store.users.get(username)
    topics = user_stats.get_top_topics(limit)
    
    if not topics:
        return Left(f"User '{username}' has no topic data.")
    
    return Right(topics)


def get_user_contribution_types(
    username: str,
    store: StatisticsStore
) -> Either[str, Dict[str, int]]:
    """
    Get type of contribution from immutable store.
    """
    if username not in store.users:
        return Left(f"User '{username}' not found.")
    
    user_stats = store.users.get(username)
    types = {
        'typo_edits': user_stats.typo_edits,
        'content_additions': user_stats.content_additions,
        'minor_edits': user_stats.minor_edits,
        'bot_edits': user_stats.bot_edits,
        'total': user_stats.total_contributions
    }
    
    return Right(types)


def get_most_active_users(
    store: StatisticsStore,
    period: TimePeriod,
    limit: int = 10
) -> Either[str, List[Tuple[str, int]]]:
    """
    Retrieve most active users using event list in store.
    """
    cutoff = _get_period_cutoff(datetime.now(), period)
    user_counts = Counter()
    for event in store.all_events:
        if event.timestamp >= cutoff:
            user_counts[event.user] += 1
    
    users = user_counts.most_common(limit)
    
    if not users:
        return Left(f"No active users found in the last {period.value}.")
    
    return Right(users)


def get_top_typo_topics(
    store: StatisticsStore,
    limit: int = 10
) -> Either[str, List[Tuple[str, int]]]:
    """
    Retrieve top typo topics from store.
    """
    topics = store.topic_typo_counts.most_common(limit)
    if not topics:
        return Left("No typo edits found yet.")
    return Right(topics)


def get_statistics_summary(
    store: StatisticsStore
) -> Either[str, Dict[str, Any]]:
    """
    Get overall statistics summary from store.
    """
    return Right(store.get_summary())


def get_most_mistaken_words(
    store: StatisticsStore,
    limit: int = 15,
) -> Either[str, List[Tuple[str, int]]]:
    """
    Retrieve the most frequently corrected word pairs from the store.
    Returns Either[error_msg, list_of_(correction_pair_str, count)].
    """
    words = store.word_mistake_counts.most_common(limit)
    if not words:
        return Left("No word corrections detected yet.")
    return Right(words)


# ============================================================================
# Convenience Functions
# ============================================================================

def print_either_result(result: Either[str, Any], success_msg: str = "Result"):
    if result.is_right():
        print(f"{success_msg}: {result.get_right()}")
    else:
        print(f"Error: {result.get_left()}")
