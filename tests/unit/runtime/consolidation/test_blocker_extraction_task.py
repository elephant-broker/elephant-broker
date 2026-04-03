"""Tests for RT-2: BlockerExtractionTask."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

from elephantbroker.runtime.consolidation.blocker_extraction_task import BlockerExtractionTask
from elephantbroker.schemas.config import BlockerExtractionConfig
from elephantbroker.schemas.goal import GoalState


def _make_task(enabled=True, llm_response=None, llm_fail=False):
    config = BlockerExtractionConfig(
        enabled=enabled, endpoint="http://test:8811/v1",
        api_key="test-key", model="test-model",
        run_every_n_turns=3, recent_messages_window=10,
    )
    goal_store = AsyncMock()
    task = BlockerExtractionTask(config, goal_store)
    if enabled:
        mock_client = AsyncMock()
        if llm_fail:
            mock_client.post = AsyncMock(side_effect=RuntimeError("LLM down"))
        else:
            resp = MagicMock()
            resp.status_code = 200
            resp.raise_for_status = MagicMock()
            resp.json.return_value = {
                "choices": [{"message": {"content": json.dumps(llm_response or [])}}],
            }
            mock_client.post = AsyncMock(return_value=resp)
        task._client = mock_client
    return task, goal_store


def _make_goal(title="Test Goal", blockers=None):
    return GoalState(title=title, blockers=blockers or [])


class TestBlockerExtractionTask:
    async def test_extraction_returns_blockers(self):
        goals = [_make_goal("Deploy app")]
        task, store = _make_task(llm_response=[{"goal_index": 0, "blocker_text": "CI pipeline broken"}])
        messages = [{"role": "user", "content": "CI is failing"}]
        result = await task.extract("sk", "sid", "gw", messages, goals)
        assert len(result) == 1
        assert result[0]["blocker_text"] == "CI pipeline broken"

    async def test_redis_updated_with_blockers(self):
        goals = [_make_goal("Ship feature")]
        task, store = _make_task(llm_response=[{"goal_index": 0, "blocker_text": "API rate limit"}])
        messages = [{"role": "assistant", "content": "hitting rate limits"}]
        await task.extract("sk", "sid", "gw", messages, goals)
        store.update_goal.assert_called_once()

    async def test_interval_gate_no_goals(self):
        task, store = _make_task()
        result = await task.extract("sk", "sid", "gw", [{"role": "user", "content": "hi"}], [])
        assert result == []

    async def test_disabled_config_skips(self):
        task, store = _make_task(enabled=False)
        result = await task.extract("sk", "sid", "gw", [], [_make_goal()])
        assert result == []

    async def test_llm_failure_returns_empty(self):
        goals = [_make_goal("Test")]
        task, store = _make_task(llm_fail=True)
        result = await task.extract("sk", "sid", "gw", [{"role": "user", "content": "x"}], goals)
        assert result == []

    async def test_no_messages_returns_empty(self):
        task, store = _make_task()
        result = await task.extract("sk", "sid", "gw", [], [_make_goal()])
        assert result == []

    async def test_duplicate_blocker_not_added(self):
        goals = [_make_goal("Goal", blockers=["existing blocker"])]
        task, store = _make_task(llm_response=[{"goal_index": 0, "blocker_text": "existing blocker"}])
        result = await task.extract("sk", "sid", "gw", [{"role": "user", "content": "x"}], goals)
        assert len(result) == 0  # Duplicate, not added
        store.update_goal.assert_not_called()

    async def test_invalid_goal_index_ignored(self):
        goals = [_make_goal("Only goal")]
        task, store = _make_task(llm_response=[{"goal_index": 5, "blocker_text": "out of range"}])
        result = await task.extract("sk", "sid", "gw", [{"role": "user", "content": "x"}], goals)
        assert result == []
