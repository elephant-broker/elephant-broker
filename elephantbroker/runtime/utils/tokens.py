"""Token counting utility."""
from __future__ import annotations


def count_tokens(text: str, model: str = "cl100k_base") -> int:
    """Count tokens in text. Falls back to len(text) // 4 if tiktoken unavailable."""
    if not text:
        return 0
    try:
        import tiktoken
        enc = tiktoken.get_encoding(model)
        return len(enc.encode(text))
    except Exception:
        return len(text) // 4
