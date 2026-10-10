"""Stop-word detection (NFKC, case-insensitive, whole-message match)."""

from __future__ import annotations

import unicodedata
from collections.abc import Iterable


def normalize(text: str) -> str:
    return unicodedata.normalize("NFKC", text).strip().upper()


class StopWords:
    def __init__(self, words: Iterable[str]) -> None:
        self._words = frozenset(normalize(w) for w in words)

    def matches(self, text: str) -> bool:
        return normalize(text) in self._words
