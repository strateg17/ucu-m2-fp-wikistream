"""
Event Processing Pipeline

Processes WikiMedia events using functional programming patterns.
Uses Maybe monad for safe parsing and functors for transformations.
"""

from typing import Dict, Any, Optional, Tuple, AsyncIterator
from datetime import datetime
from enum import Enum
import aiostream
from aiostream import pipable_operator
from functional_utils import Maybe, Some, Nothing, compose, pipe


class EditType(Enum):
    """Classification of edit types"""
    TYPO_EDITING = "typo_editing"      # Small edits (< 50 chars)
    CONTENT_ADDITION = "content_addition"  # Larger content changes
    MINOR_EDIT = "minor_edit"           # Marked as minor by editor
    BOT_EDIT = "bot_edit"               # Automated edits
    UNKNOWN = "unknown"


class ParsedEvent:
    """
    Structured representation of a parsed Wikipedia event.
    Immutable data class (functional programming principle).
    """
    
    def __init__(
        self,
        user: str,
        title: str,
        timestamp: datetime,
        edit_type: EditType,
        diff_size: int,
        is_bot: bool,
        is_minor: bool,
        wiki: str,
        namespace: int,
        comment: Optional[str] = None
    ):
        self.user = user
        self.title = title
        self.timestamp = timestamp
        self.edit_type = edit_type
        self.diff_size = diff_size
        self.is_bot = is_bot
        self.is_minor = is_minor
        self.wiki = wiki
        self.namespace = namespace
        self.comment = comment or ""
    
    def __repr__(self):
        return (f"ParsedEvent(user={self.user}, title={self.title}, "
                f"edit_type={self.edit_type.value}, diff_size={self.diff_size})")


# ============================================================================
# Event Parsing with Maybe Monad
# ============================================================================

def parse_event(raw_event: Dict[str, Any]) -> Maybe[ParsedEvent]:
    """
    Parse raw WikiMedia event into structured ParsedEvent.
    
    MONAD: Uses Maybe to handle missing/malformed data gracefully.
    Returns Nothing if required fields are missing.
    
    Args:
        raw_event: Raw event dictionary from WikiMedia stream
        
    Returns:
        Maybe[ParsedEvent]: Some(parsed_event) or Nothing()
    """
    
    try:
        # Extract required fields with Maybe monad
        user = raw_event.get('user')
        title = raw_event.get('title')
        timestamp_val = raw_event.get('timestamp')
        
        # Return Nothing if any required field is missing
        if not user or not title or not timestamp_val:
            return Nothing()
        
        # Parse timestamp
        try:
            timestamp = datetime.fromtimestamp(timestamp_val)
        except (ValueError, TypeError, OSError):
            return Nothing()
        
        # Calculate diff size
        length = raw_event.get('length', {})
        old_length = length.get('old', 0) if isinstance(length, dict) else 0
        new_length = length.get('new', 0) if isinstance(length, dict) else 0
        diff_size = new_length - old_length
        
        # Extract metadata
        is_bot = raw_event.get('bot', False)
        is_minor = raw_event.get('minor', False)
        wiki = raw_event.get('wiki', 'unknown')
        namespace = raw_event.get('namespace', 0)
        comment = raw_event.get('comment', '')
        
        # Classify edit type
        edit_type = classify_edit_type(diff_size, is_bot, is_minor, comment)
        
        # Create parsed event
        parsed = ParsedEvent(
            user=user,
            title=title,
            timestamp=timestamp,
            edit_type=edit_type,
            diff_size=diff_size,
            is_bot=is_bot,
            is_minor=is_minor,
            wiki=wiki,
            namespace=namespace,
            comment=comment
        )
        
        return Some(parsed)
        
    except Exception as e:
        # Any parsing error results in Nothing
        return Nothing()


def classify_edit_type(
    diff_size: int,
    is_bot: bool,
    is_minor: bool,
    comment: str
) -> EditType:
    """
    Classify the type of edit based on characteristics.
    
    Classification logic:
    - Bot edits: BOT_EDIT
    - Small changes (|diff| < 50): TYPO_EDITING
    - Marked as minor: MINOR_EDIT
    - Larger changes: CONTENT_ADDITION
    
    Args:
        diff_size: Size difference (new - old)
        is_bot: Whether edit was made by a bot
        is_minor: Whether marked as minor edit
        comment: Edit comment
        
    Returns:
        EditType: Classification of the edit
    """
    
    if is_bot:
        return EditType.BOT_EDIT
    
    # Absolute difference
    abs_diff = abs(diff_size)
    
    # Small changes are likely typo fixes
    if abs_diff < 50:
        return EditType.TYPO_EDITING
    
    # Marked as minor
    if is_minor:
        return EditType.MINOR_EDIT
    
    # Larger changes are content additions/modifications
    if abs_diff >= 50:
        return EditType.CONTENT_ADDITION
    
    return EditType.UNKNOWN


# ============================================================================
# Functor Operations - Extracting information from ParsedEvent
# ============================================================================

def extract_user_info(event: ParsedEvent) -> Dict[str, Any]:
    """
    FUNCTOR: Extract user information from parsed event.
    
    This can be used with Maybe.map() to transform parsed events.
    
    Args:
        event: Parsed event
        
    Returns:
        Dict with user information
    """
    return {
        'user': event.user,
        'timestamp': event.timestamp,
        'wiki': event.wiki
    }


def extract_contribution(event: ParsedEvent) -> Dict[str, Any]:
    """
    FUNCTOR: Extract contribution data for statistics.
    
    Args:
        event: Parsed event
        
    Returns:
        Dict with contribution data
    """
    return {
        'user': event.user,
        'title': event.title,
        'timestamp': event.timestamp,
        'edit_type': event.edit_type,
        'diff_size': event.diff_size,
        'is_typo': event.edit_type == EditType.TYPO_EDITING
    }


def extract_topic(event: ParsedEvent) -> str:
    """
    Extract topic (page title) from event.
    
    Args:
        event: Parsed event
        
    Returns:
        Page title as topic
    """
    return event.title


def is_typo_edit(event: ParsedEvent) -> bool:
    """
    Predicate to check if edit is a typo fix.
    
    Used for filtering streams.
    
    Args:
        event: Parsed event
        
    Returns:
        True if this is a typo edit
    """
    return event.edit_type == EditType.TYPO_EDITING


def is_content_addition(event: ParsedEvent) -> bool:
    """
    Predicate to check if edit is content addition.
    
    Args:
        event: Parsed event
        
    Returns:
        True if this is content addition
    """
    return event.edit_type == EditType.CONTENT_ADDITION


# ============================================================================
# Composed Processing Functions
# ============================================================================

def parse_and_extract_user(raw_event: Dict[str, Any]) -> Maybe[Dict[str, Any]]:
    """
    FUNCTOR COMPOSITION: Parse event and extract user info in one step.
    
    Demonstrates function composition with Maybe monad.
    
    Args:
        raw_event: Raw event from stream
        
    Returns:
        Maybe[Dict]: User info or Nothing
    """
    return parse_event(raw_event).map(extract_user_info)


def parse_and_extract_contribution(raw_event: Dict[str, Any]) -> Maybe[Dict[str, Any]]:
    """
    FUNCTOR COMPOSITION: Parse event and extract contribution data.
    
    Args:
        raw_event: Raw event from stream
        
    Returns:
        Maybe[Dict]: Contribution data or Nothing
    """
    return parse_event(raw_event).map(extract_contribution)


# ============================================================================
# Batch Processing
# ============================================================================

def parse_events_batch(raw_events: list[Dict[str, Any]]) -> list[ParsedEvent]:
    """
    Parse multiple events, filtering out failures.
    
    FUNCTOR: Maps parse_event over a list, keeping only successful parses.
    
    Args:
        raw_events: List of raw events
        
    Returns:
        List of successfully parsed events (failures are dropped)
    """
    parsed = []
    for event in raw_events:
        maybe_parsed = parse_event(event)
        if maybe_parsed.is_some():
            parsed.append(maybe_parsed.get_or_else(None))
    
    return parsed


def filter_by_edit_type(events: list[ParsedEvent], edit_type: EditType) -> list[ParsedEvent]:
    """
    Filter events by edit type.
    
    Args:
        events: List of parsed events
        edit_type: Type to filter for
        
    Returns:
        Filtered list
    """
    return [e for e in events if e.edit_type == edit_type]


def group_by_user(events: list[ParsedEvent]) -> Dict[str, list[ParsedEvent]]:
    """
    Group events by user.
    
    Args:
        events: List of parsed events
        
    Returns:
        Dict mapping username to list of their events
    """
    groups = {}
    for event in events:
        if event.user not in groups:
            groups[event.user] = []
        groups[event.user].append(event)
    
    return groups


def group_by_topic(events: list[ParsedEvent]) -> Dict[str, list[ParsedEvent]]:
    """
    Group events by topic (page title).
    
    Args:
        events: List of parsed events
        
    Returns:
        Dict mapping topic to list of events
    """
    groups = {}
    for event in events:
        if event.title not in groups:
            groups[event.title] = []
        groups[event.title].append(event)
    
    return groups


# ============================================================================
# Stream Operators (Functional Reactive)
# ============================================================================

@pipable_operator
async def parse_stream(source: AsyncIterator[Dict[str, Any]]) -> AsyncIterator[ParsedEvent]:
    """
    A pipable operator that parses raw events into ParsedEvent objects.
    Uses Maybe monad and filters out failures.
    
    Args:
        source: Async iterator of raw event dictionaries.
        
    Yields:
        Successfully parsed ParsedEvent objects.
    """
    async with aiostream.streamcontext(source) as streamer:
        async for raw_event in streamer:
            maybe_parsed = parse_event(raw_event)
            if maybe_parsed.is_some():
                yield maybe_parsed.get_or_else(None)


@pipable_operator
async def filter_edit_type_stream(
    source: AsyncIterator[ParsedEvent],
    edit_type: EditType
) -> AsyncIterator[ParsedEvent]:
    """
    A pipable operator that filters parsed events by edit type.
    
    Args:
        source: Async iterator of ParsedEvent objects.
        edit_type: Type to keep.
        
    Yields:
        Matching ParsedEvent objects.
    """
    async with aiostream.streamcontext(source) as streamer:
        async for event in streamer:
            if event.edit_type == edit_type:
                yield event
