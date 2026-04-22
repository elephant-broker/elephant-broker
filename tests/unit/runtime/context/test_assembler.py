"""Tests for context assembler utilities (PR #11 R1 TODO-11-004)."""
from __future__ import annotations

import pytest

from elephantbroker.runtime.context.assembler import (
    _render_item_block,
    _truncate_to_budget,
)
from tests.fixtures.factories import make_working_set_item


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


# ======================================================================
# TODO-6-101 (Round 1, Business Logic Reviewer, MEDIUM):
# _render_item_block uses `item.retrieval_source or item.source_type` so
# that retrieval-sourced facts keep their per-path provenance label in
# the agent-facing prompt (pre-T-3 semantics restored via Option C
# stamping), while non-fact items fall back gracefully to source_type.
# ======================================================================


class TestRenderItemBlockLabel:
    """The agent-facing ``[Memory (<label>)]`` / ``[<label>]`` prefix must
    prefer ``retrieval_source`` over ``source_type`` so the retrieval
    path (vector/keyword/structural/graph) is visible to the model."""

    def test_retrieval_sourced_fact_renders_retrieval_path_label(self):
        """Fact stamped with ``retrieval_source="vector"`` renders as
        ``[vector] ...`` — NOT ``[fact] ...``. This restores the
        pre-T-3 prompt semantics that the agent reasons over."""
        item = make_working_set_item(
            text="short fact body",  # < 400-char threshold → inline branch
            source_type="fact",
            retrieval_source="vector",
        )
        rendered = _render_item_block(item)
        assert rendered.startswith("[vector]"), (
            f"expected retrieval_source to win the label prefix, got: {rendered!r}"
        )
        assert "[fact]" not in rendered

    def test_non_fact_item_falls_back_to_source_type_label(self):
        """Non-fact item (goal / procedure / artifact / etc.) has
        ``retrieval_source=None`` by construction, so the label falls
        back to the DataPoint-type ``source_type`` — agent still sees
        ``[goal]`` / ``[procedure]`` / ``[artifact]`` etc., not
        ``[None]`` or empty."""
        item = make_working_set_item(
            text="pursue project milestone",
            source_type="goal",
            retrieval_source=None,
        )
        rendered = _render_item_block(item)
        assert rendered.startswith("[goal]"), (
            f"expected source_type fallback when retrieval_source is None, got: {rendered!r}"
        )

    def test_fact_with_none_retrieval_source_gracefully_falls_back(self):
        """Fact with ``retrieval_source=None`` (legacy / pre-T-3
        records, or structural hits that weren't stamped) falls back to
        ``source_type="fact"`` rather than producing a ``[None]`` label.
        Safe-degradation guarantee for data without retrieval provenance."""
        item = make_working_set_item(
            text="short legacy fact",
            source_type="fact",
            retrieval_source=None,
        )
        rendered = _render_item_block(item)
        assert rendered.startswith("[fact]"), (
            f"expected graceful fallback to source_type=fact, got: {rendered!r}"
        )
        assert "[None]" not in rendered

    def test_retrieval_sourced_fact_renders_retrieval_path_in_artifact_placeholder(self):
        """Large items (>400 chars) trigger the artifact-placeholder
        branch — the ``[Memory (<label>): ...]`` prefix must also honor
        ``retrieval_source`` so the agent sees the retrieval path even
        when the item is elided for budget reasons."""
        long_text = "x " * 300  # 600 chars → placeholder branch
        item = make_working_set_item(
            text=long_text,
            source_type="fact",
            retrieval_source="keyword",
        )
        rendered = _render_item_block(item)
        assert rendered.startswith("[Memory (keyword):"), (
            f"expected artifact-placeholder branch to honor retrieval_source, got first line: "
            f"{rendered.splitlines()[0]!r}"
        )
        assert "[Memory (fact)" not in rendered
