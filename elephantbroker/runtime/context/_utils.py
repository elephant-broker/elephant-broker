"""Shared utilities for context assembly and lifecycle."""
from __future__ import annotations

STOP_WORDS: frozenset[str] = frozenset({
    "the", "a", "an", "is", "are", "was", "were", "be", "been", "being",
    "have", "has", "had", "do", "does", "did", "will", "would", "could",
    "should", "may", "might", "shall", "can", "to", "of", "in", "for",
    "on", "with", "at", "by", "from", "as", "into", "through", "during",
    "before", "after", "above", "below", "between", "and", "but", "or",
    "not", "no", "nor", "so", "if", "then", "than", "that", "this",
    "these", "those", "it", "its", "i", "we", "you", "he", "she", "they",
    "me", "us", "him", "her", "them", "my", "our", "your", "his",
})

# T-1 (2026-04-21): strip edge punctuation from tokens so phrases like
# "timescaledb," and "extension." match paraphrased agent responses.
# Hyphens explicitly preserved — "time-series" is a single semantic token.
# Alphanumerics/unicode inside tokens untouched — only trims edges.
_PHRASE_EDGE_STRIP = ".,;:!?()[]{}\"'`"


def _extract_key_phrases(text: str) -> list[str]:
    """Extract 2-3 word consecutive chunks, filtering stop words.

    Tokens have edge punctuation stripped (commas, periods, brackets,
    quotes, backticks) so that paraphrased responses missing the original
    punctuation can still match. Hyphens are NOT stripped — "time-series",
    "auto-recall", "pre-prompt" must survive as single semantic tokens.
    """
    words = [
        s for w in text.lower().split()
        if (s := w.strip(_PHRASE_EDGE_STRIP))
        and s not in STOP_WORDS
        and len(s) > 2
    ]
    phrases: list[str] = []
    for i in range(len(words) - 1):
        phrases.append(f"{words[i]} {words[i + 1]}")
        if i + 2 < len(words):
            phrases.append(f"{words[i]} {words[i + 1]} {words[i + 2]}")
    return phrases
