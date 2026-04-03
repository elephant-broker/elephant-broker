"""Unit tests for summarize_artifact task."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from elephantbroker.runtime.adapters.cognee.tasks.summarize_artifact import summarize_artifact
from elephantbroker.schemas.artifact import ToolArtifact


def _make_config(**overrides):
    config = MagicMock()
    config.summarization_min_artifact_chars = overrides.get("summarization_min_artifact_chars", 500)
    config.summarization_max_output_tokens = overrides.get("summarization_max_output_tokens", 200)
    return config


class TestSummarizeArtifact:
    async def test_small_artifact_truncated(self):
        """Content shorter than min_chars should be truncated to 200 chars, no LLM."""
        llm = MagicMock()
        llm.complete = AsyncMock(return_value="llm summary")
        config = _make_config(summarization_min_artifact_chars=500)
        art = ToolArtifact(tool_name="grep", content="x" * 300)
        result = await summarize_artifact(art, llm, config)
        assert len(result.summary) == 200
        assert result.tool_name == "grep"
        llm.complete.assert_not_called()

    async def test_large_artifact_llm_summary(self):
        """Content >= min_chars should trigger LLM summarization."""
        llm = MagicMock()
        llm.complete = AsyncMock(return_value="LLM generated summary of the artifact")
        config = _make_config(summarization_min_artifact_chars=500)
        art = ToolArtifact(tool_name="cat", content="x" * 600)
        result = await summarize_artifact(art, llm, config)
        assert result.summary == "LLM generated summary of the artifact"
        llm.complete.assert_called_once()

    async def test_no_llm_falls_back(self):
        """No LLM client should fallback to truncation."""
        config = _make_config(summarization_min_artifact_chars=500)
        art = ToolArtifact(tool_name="cat", content="y" * 600)
        result = await summarize_artifact(art, llm_client=None, config=config)
        assert len(result.summary) == 200
        assert result.summary == "y" * 200

    async def test_preserves_artifact_id(self):
        art = ToolArtifact(tool_name="cat", content="data")
        result = await summarize_artifact(art)
        assert result.artifact_id == art.artifact_id

    async def test_empty_content(self):
        art = ToolArtifact(tool_name="empty", content="")
        result = await summarize_artifact(art)
        assert result.summary == ""

    async def test_llm_failure_falls_back_to_truncation(self):
        """If LLM raises, should fallback to truncation."""
        llm = MagicMock()
        llm.complete = AsyncMock(side_effect=RuntimeError("LLM down"))
        config = _make_config(summarization_min_artifact_chars=500)
        art = ToolArtifact(tool_name="cat", content="z" * 600)
        result = await summarize_artifact(art, llm, config)
        assert len(result.summary) == 200

    async def test_no_config_uses_defaults(self):
        """No config should use default min_chars of 500."""
        art = ToolArtifact(tool_name="ls", content="short")
        result = await summarize_artifact(art, llm_client=None, config=None)
        assert result.summary == "short"
