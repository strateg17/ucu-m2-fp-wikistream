from __future__ import annotations

import re

from app.models import ChangeEvent, ContributionType

TYPO_KEYWORDS = {
    "typo",
    "spelling",
    "grammar",
    "copyedit",
    "copy edit",
    "punctuation",
    "wikify",
    "fix typo",
}
CONTENT_KEYWORDS = {
    "add",
    "added",
    "new section",
    "expand",
    "expanded",
    "content",
    "reference",
    "sources",
}
WORD_PATTERN = re.compile(r"\b([A-Za-z]{3,})\b")


def classify_contribution(event: ChangeEvent) -> ContributionType:
    comment = event.comment.lower()
    old = event.length.old or 0
    new = event.length.new or old
    delta = new - old

    if any(keyword in comment for keyword in TYPO_KEYWORDS):
        return ContributionType.TYPO_EDITING
    if any(keyword in comment for keyword in CONTENT_KEYWORDS):
        return ContributionType.CONTENT_ADDITION
    if abs(delta) <= 25:
        return ContributionType.TYPO_EDITING
    return ContributionType.CONTENT_ADDITION


def typo_words_from_comment(comment: str) -> list[str]:
    lowered = comment.lower()
    if "typo" not in lowered and "spelling" not in lowered and "misspell" not in lowered:
        return []
    return [m.group(1) for m in WORD_PATTERN.finditer(comment)]
