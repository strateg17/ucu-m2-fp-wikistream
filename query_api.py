"""
Query API for User Statistics

Provides a functional interface for querying user statistics.
Uses Either monad for error handling.
"""

from typing import List, Tuple, Dict, Any, Union
from functional_utils import Either, Left, Right
from user_statistics import (
    StatisticsStore, 
    FunctionalStore,
    get_statistics_store,
    TimeGranularity,
    TimePeriod
)

StoreType = Union[StatisticsStore, FunctionalStore]

# ============================================================================
# Query Functions - All return Either monad for error handling
# ============================================================================

def get_user_contribution_series(
    username: str,
    granularity: TimeGranularity = TimeGranularity.DAY,
    store: StoreType = None
) -> Either[str, List[Tuple[Any, int]]]:
    """
    Get user contribution time series.
    
    Requirement #3.1: Information about user contribution as series of points
    over time (X=time, Y=total contributions).
    
    EITHER MONAD: Returns Right(data) on success, Left(error) on failure.
    
    Args:
        username: Username to query
        granularity: Time granularity (HOUR, DAY, MONTH, YEAR)
        store: StatisticsStore or FunctionalStore instance (uses global if None)
        
    Returns:
        Either[str, List[Tuple[datetime, int]]]: 
            Right([(timestamp, count), ...]) or Left(error_message)
    """
    if store is None:
        store = get_statistics_store()
    
    # Check if user exists
    if isinstance(store, StatisticsStore):
        exists = store.user_exists(username)
        user_stats = store.get_user_statistics(username)
    else:
        exists = username in store.users
        user_stats = store.users.get(username)

    if not exists:
        return Left(f"User '{username}' not found. No contributions observed yet.")
    
    if user_stats is None:
        return Left(f"Failed to retrieve statistics for user '{username}'.")
    
    # Get contribution series
    series = user_stats.get_contribution_series(granularity)
    
    if not series:
        return Left(f"User '{username}' has no contribution data.")
    
    return Right(series)


def get_user_top_topics(
    username: str,
    limit: int = 10,
    store: StoreType = None
) -> Either[str, List[Tuple[str, int]]]:
    """
    Get topics to which user has contributed most.
    
    Requirement #3.2: Topics to which user has contributed most.
    
    EITHER MONAD: Returns Right(topics) on success, Left(error) on failure.
    
    Args:
        username: Username to query
        limit: Maximum number of topics to return
        store: StatisticsStore or FunctionalStore instance (uses global if None)
        
    Returns:
        Either[str, List[Tuple[str, int]]]: 
            Right([(topic, count), ...]) or Left(error_message)
    """
    if store is None:
        store = get_statistics_store()
    
    # Check if user exists
    if isinstance(store, StatisticsStore):
        exists = store.user_exists(username)
        user_stats = store.get_user_statistics(username)
    else:
        exists = username in store.users
        user_stats = store.users.get(username)

    if not exists:
        return Left(f"User '{username}' not found. No contributions observed yet.")
    
    if user_stats is None:
        return Left(f"Failed to retrieve statistics for user '{username}'.")
    
    # Get top topics
    topics = user_stats.get_top_topics(limit)
    
    if not topics:
        return Left(f"User '{username}' has not contributed to any topics.")
    
    return Right(topics)


def get_user_contribution_types(
    username: str,
    store: StoreType = None
) -> Either[str, Dict[str, int]]:
    """
    Get type of contribution (typo editing vs content addition).
    
    Requirement #3.3: Type of contribution as absolute numbers.
    
    EITHER MONAD: Returns Right(types) on success, Left(error) on failure.
    
    Args:
        username: Username to query
        store: StatisticsStore or FunctionalStore instance (uses global if None)
        
    Returns:
        Either[str, Dict[str, int]]: 
            Right({
                'typo_edits': count,
                'content_additions': count,
                'minor_edits': count,
                'bot_edits': count,
                'total': count
            }) or Left(error_message)
    """
    if store is None:
        store = get_statistics_store()
    
    # Check if user exists
    if isinstance(store, StatisticsStore):
        exists = store.user_exists(username)
        user_stats = store.get_user_statistics(username)
    else:
        exists = username in store.users
        user_stats = store.users.get(username)

    if not exists:
        return Left(f"User '{username}' not found. No contributions observed yet.")
    
    if user_stats is None:
        return Left(f"Failed to retrieve statistics for user '{username}'.")
    
    # Build contribution types dictionary
    types = {
        'typo_edits': user_stats.typo_edits,
        'content_additions': user_stats.content_additions,
        'minor_edits': user_stats.minor_edits,
        'bot_edits': user_stats.bot_edits,
        'total': user_stats.total_contributions
    }
    
    return Right(types)


def get_most_active_users(
    period: TimePeriod,
    limit: int = 10,
    store: StoreType = None
) -> Either[str, List[Tuple[str, int]]]:
    """
    Retrieve most active users during a time period.
    
    Requirement #3.4: Retrieve most active user during (YEAR|MONTH|DAY).
    
    EITHER MONAD: Returns Right(users) on success, Left(error) on failure.
    
    Args:
        period: Time period (HOUR, DAY, MONTH, YEAR)
        limit: Maximum number of users to return
        store: StatisticsStore or FunctionalStore instance (uses global if None)
        
    Returns:
        Either[str, List[Tuple[str, int]]]: 
            Right([(username, count), ...]) or Left(error_message)
    """
    if store is None:
        store = get_statistics_store()
    
    if isinstance(store, StatisticsStore):
        # Get most active users
        users = store.get_most_active_users(period, limit)
    else:
        # Re-implement activity check for FunctionalStore if needed
        from collections import Counter
        from user_statistics import _get_period_cutoff
        from datetime import datetime
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
    limit: int = 10,
    store: StoreType = None
) -> Either[str, List[Tuple[str, int]]]:
    """
    Retrieve top topics with most typo edits.
    
    Requirement #3.5: Retrieve the top 10 topics which have the most 
    number of typo editings.
    
    EITHER MONAD: Returns Right(topics) on success, Left(error) on failure.
    
    Args:
        limit: Maximum number of topics to return
        store: StatisticsStore or FunctionalStore instance (uses global if None)
        
    Returns:
        Either[str, List[Tuple[str, int]]]: 
            Right([(topic, typo_count), ...]) or Left(error_message)
    """
    if store is None:
        store = get_statistics_store()
    
    if isinstance(store, StatisticsStore):
        topics = store.get_top_typo_topics(limit)
    else:
        topics = store.topic_typo_counts.most_common(limit)
    
    if not topics:
        return Left("No typo edits found yet.")
    
    return Right(topics)


def get_all_users(
    store: StoreType = None
) -> Either[str, List[str]]:
    """
    Get list of all observed users.
    
    EITHER MONAD: Returns Right(users) on success, Left(error) on failure.
    
    Args:
        store: StatisticsStore or FunctionalStore instance (uses global if None)
        
    Returns:
        Either[str, List[str]]: Right([usernames, ...]) or Left(error_message)
    """
    if store is None:
        store = get_statistics_store()
    
    if isinstance(store, StatisticsStore):
        users = store.get_all_users()
    else:
        users = list(store.users.keys())
    
    if not users:
        return Left("No users observed yet.")
    
    return Right(users)


def get_statistics_summary(
    store: StoreType = None
) -> Either[str, Dict[str, Any]]:
    """
    Get overall statistics summary.
    
    EITHER MONAD: Returns Right(summary) on success, Left(error) on failure.
    
    Args:
        store: StatisticsStore or FunctionalStore instance (uses global if None)
        
    Returns:
        Either[str, Dict[str, Any]]: Right(summary_dict) or Left(error_message)
    """
    if store is None:
        store = get_statistics_store()
    
    if isinstance(store, StatisticsStore):
        summary = store.get_statistics_summary()
    else:
        summary = store.get_summary()
    
    return Right(summary)


# ============================================================================
# Convenience Functions for Working with Either Results
# ============================================================================

def print_either_result(result: Either[str, Any], success_msg: str = "Result"):
    """
    Print Either result in a user-friendly way.
    
    Args:
        result: Either result to print
        success_msg: Message prefix for successful results
    """
    if result.is_right():
        print(f"{success_msg}: {result.get_right()}")
    else:
        print(f"Error: {result.get_left()}")


def unwrap_or_default(result: Either[str, Any], default: Any) -> Any:
    """
    Unwrap Either result or return default on error.
    
    Args:
        result: Either result
        default: Default value if Left
        
    Returns:
        Right value or default
    """
    return result.get_or_else(default)


def combine_either_results(
    results: List[Either[str, Any]]
) -> Either[str, List[Any]]:
    """
    Combine multiple Either results into one.
    
    APPLICATIVE pattern: Combines multiple Either values.
    Returns Left if any result is Left, otherwise Right with all values.
    
    Args:
        results: List of Either results
        
    Returns:
        Either[str, List[Any]]: Combined result
    """
    values = []
    for result in results:
        if result.is_left():
            return Left(result.get_left())
        values.append(result.get_right())
    
    return Right(values)
