"""Tests for RT-1: SuccessfulUseReasoningTask."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elephantbroker.runtime.consolidation.successful_use_task import SuccessfulUseReasoningTask
from elephantbroker.schemas.config import SuccessfulUseConfig
from tests.fixtures.factories import make_fact_assertion


def _make_task(enabled=True, llm_response=None, llm_fail=False):
    config = SuccessfulUseConfig(
        enabled=enabled, endpoint="http://test:8811/v1",
        api_key="test-key", model="test-model",
        batch_size=5, min_confidence=0.7,
    )
    memory = AsyncMock()
    task = SuccessfulUseReasoningTask(config, memory)
    if enabled:
        mock_client = AsyncMock()
        if llm_fail:
            mock_client.post = AsyncMock(side_effect=RuntimeError("LLM down"))
        else:
            resp = MagicMock()
            resp.status_code = 200
            resp.raise_for_status = MagicMock()
            resp.json.return_value = {
                "choices": [{"message": {"content": json.dumps(llm_response or {"used_fact_indices": [], "reasoning": ""})}}],
            }
            mock_client.post = AsyncMock(return_value=resp)
        task._client = mock_client
    return task, memory


class TestSuccessfulUseReasoningTask:
    async def test_batch_eval_returns_used_fact_ids(self):
        facts = [make_fact_assertion(text=f"fact {i}") for i in range(3)]
        task, memory = _make_task(llm_response={"used_fact_indices": [0, 2], "reasoning": "matched"})
        result = await task.evaluate_batch(facts, [[{"role": "assistant", "content": "ok"}]], [], "gw")
        assert len(result) == 2
        assert str(facts[0].id) in result
        assert str(facts[2].id) in result

    async def test_llm_error_returns_empty(self):
        facts = [make_fact_assertion(text="test")]
        task, memory = _make_task(llm_fail=True)
        result = await task.evaluate_batch(facts, [[]], [], "gw")
        assert result == []

    async def test_empty_response_returns_empty(self):
        task, memory = _make_task(llm_response={"used_fact_indices": [], "reasoning": ""})
        result = await task.evaluate_batch([make_fact_assertion(text="x")], [[]], [], "gw")
        assert result == []

    async def test_fact_update_called(self):
        facts = [make_fact_assertion(text="used fact", successful_use_count=3)]
        task, memory = _make_task(llm_response={"used_fact_indices": [0], "reasoning": "used"})
        await task.evaluate_batch(facts, [[{"role": "assistant", "content": "ok"}]], [], "gw")
        memory.update.assert_called_once()
        call_args = memory.update.call_args[0]
        assert call_args[0] == facts[0].id
        assert call_args[1]["successful_use_count"] == 4

    async def test_disabled_config_skips(self):
        task, memory = _make_task(enabled=False)
        result = await task.evaluate_batch([], [[]], [], "gw")
        assert result == []

    async def test_empty_facts_returns_empty(self):
        task, _ = _make_task()
        result = await task.evaluate_batch([], [[]], [], "gw")
        assert result == []

    async def test_invalid_index_ignored(self):
        facts = [make_fact_assertion(text="only one")]
        task, memory = _make_task(llm_response={"used_fact_indices": [0, 99], "reasoning": "test"})
        result = await task.evaluate_batch(facts, [[{"role": "user", "content": "hi"}]], [], "gw")
        assert len(result) == 1  # Index 99 out of range, ignored
