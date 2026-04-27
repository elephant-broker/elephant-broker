"""Unit tests for ArtifactIngestPipeline."""
from __future__ import annotations

import hashlib
from unittest.mock import AsyncMock, MagicMock

import pytest

from elephantbroker.pipelines.artifact_ingest.pipeline import ArtifactIngestPipeline
from elephantbroker.schemas.pipeline import ArtifactIngestResult, ArtifactInput


def _make_trace():
    trace = MagicMock()
    trace.append_event = AsyncMock(side_effect=lambda e: e)
    return trace


def _make_store(existing_hash=None):
    store = MagicMock()
    store.store_artifact = AsyncMock()
    if existing_hash:
        store.get_by_hash = AsyncMock(return_value=MagicMock())
    else:
        store.get_by_hash = AsyncMock(return_value=None)
    return store


def _make_llm():
    llm = MagicMock()
    llm.complete = AsyncMock(return_value="summary text")
    return llm


def _make_config():
    config = MagicMock()
    config.summarization_min_artifact_chars = 500
    config.summarization_max_output_tokens = 200
    return config


def _make_input(**overrides):
    defaults = {
        "tool_name": "grep",
        "tool_output": "found: file.py:42: def main():",
    }
    defaults.update(overrides)
    return ArtifactInput(**defaults)


class TestArtifactIngestPipeline:
    async def test_hash_includes_tool_name_and_args(self):
        """Hash must include tool_name + sorted args + output, not just output."""
        store = _make_store()
        trace = _make_trace()
        pipe = ArtifactIngestPipeline(store, MagicMock(), _make_llm(), trace, _make_config())

        # Same output but different tool_name — should NOT dedup
        inp1 = _make_input(tool_name="grep", tool_output="result")
        inp2 = _make_input(tool_name="find", tool_output="result")
        r1 = await pipe.run(inp1)
        r2 = await pipe.run(inp2)
        assert r1.is_duplicate is False
        assert r2.is_duplicate is False  # Different tool_name → different hash

    async def test_hash_deterministic_same_input(self):
        """Same tool_name + args + output should produce same hash (dedup on 2nd call)."""
        store = _make_store()
        trace = _make_trace()
        pipe = ArtifactIngestPipeline(store, MagicMock(), _make_llm(), trace, _make_config())
        inp = _make_input(tool_name="bash", tool_args={"cmd": "ls"}, tool_output="file.txt")
        r1 = await pipe.run(inp)
        r2 = await pipe.run(inp)
        assert r1.is_duplicate is False
        assert r2.is_duplicate is True

    async def test_dedup_by_hash_returns_existing(self):
        """If store has the hash already, return is_duplicate=True."""
        store = _make_store(existing_hash=True)
        trace = _make_trace()
        pipe = ArtifactIngestPipeline(store, MagicMock(), _make_llm(), trace, _make_config())
        inp = _make_input()
        result = await pipe.run(inp)
        assert result.is_duplicate is True
        assert result.artifact is None

    async def test_dedup_skips_remaining_tasks(self):
        """Duplicate should not store, summarize, or trace."""
        store = _make_store(existing_hash=True)
        trace = _make_trace()
        llm = _make_llm()
        pipe = ArtifactIngestPipeline(store, MagicMock(), llm, trace, _make_config())
        inp = _make_input()
        await pipe.run(inp)
        store.store_artifact.assert_not_called()
        llm.complete.assert_not_called()
        trace.append_event.assert_not_called()

    async def test_new_artifact_stores_and_summarizes(self):
        """New artifact should be stored, summarized, and traced."""
        store = _make_store()
        trace = _make_trace()
        llm = _make_llm()
        pipe = ArtifactIngestPipeline(store, MagicMock(), llm, trace, _make_config())
        inp = _make_input()
        result = await pipe.run(inp)
        assert result.is_duplicate is False
        assert result.artifact is not None
        assert result.artifact.tool_name == "grep"
        store.store_artifact.assert_called_once()

    async def test_emits_trace_event(self):
        """Pipeline should emit ARTIFACT_CREATED trace event."""
        store = _make_store()
        trace = _make_trace()
        pipe = ArtifactIngestPipeline(store, MagicMock(), _make_llm(), trace, _make_config())
        inp = _make_input()
        result = await pipe.run(inp)
        trace.append_event.assert_called_once()
        event = trace.append_event.call_args[0][0]
        assert event.event_type.value == "artifact_created"
        assert result.trace_event_id is not None

    async def test_in_memory_dedup_cache(self):
        """Second call with same content should hit in-memory cache."""
        store = _make_store()
        trace = _make_trace()
        pipe = ArtifactIngestPipeline(store, MagicMock(), _make_llm(), trace, _make_config())
        inp = _make_input(tool_output="same content")
        result1 = await pipe.run(inp)
        assert result1.is_duplicate is False
        result2 = await pipe.run(inp)
        assert result2.is_duplicate is True


class TestArtifactIngestPipelineMetrics:
    """Gap #2: inc_pipeline('artifact_ingest', 'success') must be emitted."""

    async def test_inc_pipeline_success_on_happy_path(self):
        """inc_pipeline('artifact_ingest', 'success') called on successful ingest."""
        store = _make_store()
        trace = _make_trace()
        metrics = MagicMock()
        pipe = ArtifactIngestPipeline(store, MagicMock(), _make_llm(), trace, _make_config(), metrics=metrics)
        inp = _make_input()
        await pipe.run(inp)
        metrics.inc_pipeline.assert_called_once_with("artifact_ingest", "success")

    async def test_inc_pipeline_not_called_on_dedup(self):
        """Dedup short-circuits before the metric emission point."""
        store = _make_store(existing_hash=True)
        trace = _make_trace()
        metrics = MagicMock()
        pipe = ArtifactIngestPipeline(store, MagicMock(), _make_llm(), trace, _make_config(), metrics=metrics)
        inp = _make_input()
        await pipe.run(inp)
        metrics.inc_pipeline.assert_not_called()
