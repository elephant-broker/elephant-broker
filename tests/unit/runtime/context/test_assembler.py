"""Tests for context assembler utilities (PR #11 R1 TODO-11-004)."""
from __future__ import annotations

import pytest

from elephantbroker.runtime.context.assembler import _truncate_to_budget


class TestTruncateToBudget:
    def test_within_budget_returns_unchanged(self):
        text = "short text"
        assert _truncate_to_budget(text, 100) == text

    def test_exact_budget_returns_unchanged(self):
        # 10 tokens * 4 chars = 40 chars budget
        text = "a" * 40
        assert _truncate_to_budget(text, 10) == text

    def test_truncates_at_line_boundary(self):
        # budget = 5 tokens = 20 chars
        text = "first line\nsecond line\nthird line which is very long"
        result = _truncate_to_budget(text, 5)
        assert result.endswith("...")
        assert "third" not in result

    def test_truncates_at_word_boundary(self):
        # budget = 3 tokens = 12 chars; "hello world!" is 12 chars exactly
        # "hello world extra stuff beyond budget" is 36 chars, budget 3 tokens = 12 chars
        text = "hello world extra stuff beyond budget"
        result = _truncate_to_budget(text, 3)
        assert result.endswith("...")
        assert not result.rstrip("...").endswith("extr")  # no mid-word cut

    def test_single_long_word(self):
        # A single word longer than budget — no word boundary to find in first half
        text = "a" * 100
        result = _truncate_to_budget(text, 5)  # 20 char budget
        assert len(result) <= 20  # budget-accounted: 17 chars + "..." = 20
        assert result.endswith("...")

    def test_empty_string(self):
        assert _truncate_to_budget("", 10) == ""

    def test_realistic_multiline_truncation(self):
        """Realistic scenario: multi-line context text with ~100 token budget."""
        lines = [
            "## System Context",
            "You are a helpful coding assistant.",
            "The user is working on a Python web application.",
            "Key facts:",
            "- The project uses FastAPI and Pydantic v2",
            "- Database is PostgreSQL with SQLAlchemy ORM",
            "- Tests use pytest with asyncio fixtures",
            "- Deployment target is Kubernetes on GCP",
            "- The team follows trunk-based development",
            "Additional notes about the architecture and design patterns used.",
        ]
        text = "\n".join(lines)
        budget = 25  # 25 tokens = 100 chars max
        result = _truncate_to_budget(text, budget)
        assert len(result) <= budget * 4, (
            f"Result length {len(result)} exceeds budget of {budget * 4} chars"
        )
        assert result.endswith("...")
        # Should cut at a line boundary (not mid-word)
        content = result[:-3]  # strip "..."
        assert not content.endswith(" ")  # clean cut

    def test_suffix_accounted_in_budget(self):
        """The '...' suffix must be included in the budget, not appended on top."""
        # budget = 5 tokens = 20 chars total (including "...")
        text = "a" * 50
        result = _truncate_to_budget(text, 5)
        assert len(result) <= 20, f"Result length {len(result)} exceeds budget of 20 chars"
        assert result.endswith("...")
        assert result == "a" * 17 + "..."
