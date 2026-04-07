import re
import aiohttp
from functools import reduce
from html.parser import HTMLParser
from typing import List, Tuple, Optional

from functional_utils import Either, Left, Right, Maybe, Some, Nothing


WIKIPEDIA_API_URL = "https://en.wikipedia.org/w/api.php"
_MAX_WORDS_IN_DIFF = 30  # Noise filter: skip large paragraph rewrites


# ============================================================================
# Helpers
# ============================================================================

def levenshtein_distance(s1: str, s2: str) -> int:
    """
    Compute Levenshtein distance using row-reduction via functools.reduce.
    """
    if not s1:
        return len(s2)
    if not s2:
        return len(s1)

    def _next_row(prev: List[int], ch1: str) -> List[int]:
        cur = [prev[0] + 1]
        for j, ch2 in enumerate(s2):
            cost = 0 if ch1 == ch2 else 1
            cur.append(min(cur[j] + 1, prev[j + 1] + 1, prev[j] + cost))
        return cur

    return reduce(_next_row, s1, list(range(len(s2) + 1)))[-1]


def _word_core(word: str) -> str:
    """
    Strip leading/trailing non-alpha characters and lowercase.
    'receive,'  → 'receive'
    '(recieve)' → 'recieve'
    "don't"     → "don't"   (internal apostrophe preserved)
    """
    return re.sub(r"^[^a-zA-Z]+|[^a-zA-Z]+$", "", word).lower()


def is_likely_correction(old_word: str, new_word: str) -> bool:
    """
    Predicate: does old_word → new_word look like a spelling correction?

    Wider acceptance than a pure ratio check:
    - Normalises via _word_core (strips punctuation edges)
    - Allows words with internal apostrophes / hyphens
    - Uses absolute length difference (≤ 2) — fairer for short words
    - Levenshtein distance ≤ 2
    """
    old_c = _word_core(old_word)
    new_c = _word_core(new_word)

    if not old_c or not new_c:
        return False
    if old_c == new_c:
        return False
    # Reject tokens with numbers or other symbols after edge-stripping
    if not re.match(r"^[a-zA-Z'\-]+$", old_word) or not re.match(r"^[a-zA-Z'\-]+$", new_word):
        return False
    # Absolute length difference — more generous for short words than a ratio
    if abs(len(old_c) - len(new_c)) > 2:
        return False
    return levenshtein_distance(old_c, new_c) <= 2


# ============================================================================
# Diff HTML Parsing
# ============================================================================

class _DiffParser(HTMLParser):
    """
    Extract deleted / inserted word spans from Wikipedia diff HTML.
    Richer word regex captures contractions ("don't") and hyphenated
    words ("well-known") — more tokens pass is_likely_correction.
    """
    _WORD_RE = re.compile(r"[a-zA-Z]+(?:['\-][a-zA-Z]+)*")

    def __init__(self):
        super().__init__()
        self._deleted: List[str] = []
        self._inserted: List[str] = []
        self._in_del = False
        self._in_ins = False

    def handle_starttag(self, tag, attrs):
        if tag == "del":
            self._in_del = True
        elif tag == "ins":
            self._in_ins = True

    def handle_endtag(self, tag):
        if tag == "del":
            self._in_del = False
        elif tag == "ins":
            self._in_ins = False

    def handle_data(self, data):
        words = self._WORD_RE.findall(data)
        if self._in_del:
            self._deleted.extend(words)
        elif self._in_ins:
            self._inserted.extend(words)

    @property
    def deleted(self) -> List[str]:
        return list(self._deleted)

    @property
    def inserted(self) -> List[str]:
        return list(self._inserted)


def parse_diff_words(diff_html: str) -> Tuple[List[str], List[str]]:
    """Return (deleted_words, inserted_words) from Wikipedia diff HTML."""
    parser = _DiffParser()
    parser.feed(diff_html)
    return parser.deleted, parser.inserted


# ============================================================================
# Word Alignment — Two Strategies
# ============================================================================

def _align_by_position(
    deleted: List[str], inserted: List[str]
) -> List[Tuple[str, str]]:
    """
    Primary alignment: pair deleted[i] ↔ inserted[i].

    Words at the SAME position in their diff lists are most likely to be a
    direct in-place substitution (typo → correction).  zip naturally stops
    at the shorter list.  Each pair is wrapped in a Maybe so the predicate
    check stays expression-level (no if-statements that assign).
    """
    def to_maybe(d: str, i: str) -> Maybe[Tuple[str, str]]:
        return Some((_word_core(d), _word_core(i))) if is_likely_correction(d, i) else Nothing()

    return [
        pair
        for pair in (to_maybe(d, i).get_or_else(None) for d, i in zip(deleted, inserted))
        if pair is not None
    ]


def _align_by_similarity(
    deleted: List[str], inserted: List[str]
) -> List[Tuple[str, str]]:
    """
    Fallback alignment: best-Levenshtein match for each deleted word.

    Used when list lengths differ (an insertion/deletion elsewhere shifted
    word positions) or when positional alignment found nothing.
    Uses reduce so the accumulator (result list + used-set) is never mutated.
    Pure.
    """
    def step(
        acc: Tuple[List[Tuple[str, str]], frozenset],
        old: str
    ) -> Tuple[List[Tuple[str, str]], frozenset]:
        found, used = acc
        candidates = [
            (ins, levenshtein_distance(_word_core(old), _word_core(ins)))
            for ins in inserted
            if ins not in used and is_likely_correction(old, ins)
        ]
        if not candidates:
            return found, used
        best, _ = min(candidates, key=lambda x: x[1])
        return found + [(_word_core(old), _word_core(best))], used | {best}

    pairs, _ = reduce(step, deleted, ([], frozenset()))
    return pairs


def extract_corrections(diff_html: str) -> List[Tuple[str, str]]:
    """
    Combine positional and similarity alignment to extract spelling corrections.

    Strategy:
    1. Try positional pairing first (fastest, most precise).
    2. If lists differ in length OR positional found nothing, also run
       similarity matching and merge results (deduplicating).
    """
    deleted, inserted = parse_diff_words(diff_html)

    if not deleted or not inserted:
        return []
    if len(deleted) > _MAX_WORDS_IN_DIFF or len(inserted) > _MAX_WORDS_IN_DIFF:
        return []

    positional = _align_by_position(deleted, inserted)

    if len(deleted) != len(inserted) or not positional:
        similar = _align_by_similarity(deleted, inserted)
        positional_set = set(positional)
        return positional + [p for p in similar if p not in positional_set]

    return positional


# ============================================================================
# Async I/O (wrapped in Either)
# ============================================================================

async def fetch_revision_diff(old_rev: int, new_rev: int) -> Either[str, str]:
    """
    Fetch HTML diff between two Wikipedia revisions via action=compare.
    Returns Either[error_str, diff_html].
    Only impure function in this module — all I/O isolated here.
    """
    if not old_rev or not new_rev:
        return Left("Missing revision IDs")

    params = {
        "action": "compare",
        "fromrev": old_rev,
        "torev": new_rev,
        "prop": "diff",
        "format": "json",
    }
    headers = {"User-Agent": "WikiStreamAnalyzer/1.0 (Educational; pure-functional)"}

    try:
        async with aiohttp.ClientSession(headers=headers) as session:
            async with session.get(
                WIKIPEDIA_API_URL, params=params,
                timeout=aiohttp.ClientTimeout(total=10)
            ) as resp:
                data = await resp.json(content_type=None)
                diff_html = data.get("compare", {}).get("*", "")
                return Right(diff_html) if diff_html else Left("Empty diff")
    except Exception as exc:
        return Left(f"API error: {exc}")


async def analyze_event_corrections(
    old_rev: Optional[int],
    new_rev: Optional[int],
) -> Either[str, List[Tuple[str, str]]]:
    """
    Full pipeline: fetch diff → extract corrections.
    Returns Either[reason_skipped, list_of_(old_word, new_word)_pairs].
    """
    diff_result = await fetch_revision_diff(old_rev, new_rev)
    if diff_result.is_left():
        return diff_result

    corrections = extract_corrections(diff_result.get_right())
    return Right(corrections) if corrections else Left("No corrections detected")
