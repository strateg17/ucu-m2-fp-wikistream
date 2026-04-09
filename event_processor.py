"""
Event Processing Pipeline (Functional)

Processes WikiMedia events using functional reactive operators and monads.
"""

from typing import Dict, Any, Optional, Tuple, AsyncIterator
from datetime import datetime
from enum import Enum
import aiostream
from aiostream import pipable_operator
from functional_utils import Maybe, Some, Nothing


class EditType(Enum):
    """Classification of edit types"""
    TYPO_EDITING = "typo_editing"      # Small edits (< 50 chars)
    CONTENT_ADDITION = "content_addition"  # Larger content changes
    MINOR_EDIT = "minor_edit"           # Marked as minor by editor
    BOT_EDIT = "bot_edit"               # Automated edits
    UNKNOWN = "unknown"


class ParsedEvent:
    """Immutable data class for Wikipedia events."""
    
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
        comment: Optional[str] = None,
        old_rev: Optional[int] = None,
        new_rev: Optional[int] = None,
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
        self.old_rev = old_rev
        self.new_rev = new_rev

    def __repr__(self):
        return (f"ParsedEvent(user={self.user}, title={self.title}, "
                f"edit_type={self.edit_type.value}, diff_size={self.diff_size})")


def parse_event(raw_event: Dict[str, Any]) -> Maybe[ParsedEvent]:
    """Parse raw event using Maybe monad for safety."""
    try:
        user = raw_event.get('user')
        title = raw_event.get('title')
        timestamp_val = raw_event.get('timestamp')
        
        if not user or not title or not timestamp_val:
            return Nothing()
        
        timestamp = datetime.fromtimestamp(timestamp_val)
        
        length = raw_event.get('length', {})
        old_length = length.get('old', 0) if isinstance(length, dict) else 0
        new_length = length.get('new', 0) if isinstance(length, dict) else 0
        diff_size = new_length - old_length
        
        is_bot = raw_event.get('bot', False)
        is_minor = raw_event.get('minor', False)
        wiki = raw_event.get('wiki', 'unknown')
        namespace = raw_event.get('namespace', 0)
        comment = raw_event.get('comment', '')

        revision = raw_event.get('revision', {})
        old_rev = revision.get('old') if isinstance(revision, dict) else None
        new_rev = revision.get('new') if isinstance(revision, dict) else None

        edit_type = classify_edit_type(diff_size, is_bot, is_minor, comment)
        
        return Some(ParsedEvent(
            user=user, title=title, timestamp=timestamp,
            edit_type=edit_type, diff_size=diff_size,
            is_bot=is_bot, is_minor=is_minor, wiki=wiki,
            namespace=namespace, comment=comment,
            old_rev=old_rev, new_rev=new_rev,
        ))
    except Exception:
        return Nothing()


def classify_edit_type(diff_size: int, is_bot: bool, is_minor: bool, comment: str) -> EditType:
    if is_bot: return EditType.BOT_EDIT
    abs_diff = abs(diff_size)
    if abs_diff < 50: return EditType.TYPO_EDITING
    if is_minor: return EditType.MINOR_EDIT
    if abs_diff >= 50: return EditType.CONTENT_ADDITION
    return EditType.UNKNOWN


@pipable_operator
async def parse_stream(source: AsyncIterator[Dict[str, Any]]) -> AsyncIterator[ParsedEvent]:
    """Pipable operator to parse events using Maybe monad."""
    async with aiostream.streamcontext(source) as streamer:
        async for raw_event in streamer:
            maybe_parsed = parse_event(raw_event)
            if maybe_parsed.is_some():
                yield maybe_parsed.get_or_else(None)


@pipable_operator
async def filter_edit_type_stream(source: AsyncIterator[ParsedEvent], edit_type: EditType) -> AsyncIterator[ParsedEvent]:
    """Pipable operator to filter parsed events by edit type."""
    async with aiostream.streamcontext(source) as streamer:
        async for event in streamer:
            if event.edit_type == edit_type:
                yield event
