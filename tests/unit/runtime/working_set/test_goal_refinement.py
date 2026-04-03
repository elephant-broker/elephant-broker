"""Tests for GoalRefinementTask and GoalHintProcessor."""
from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elephantbroker.runtime.working_set.goal_refinement import GoalRefinementTask
from elephantbroker.runtime.working_set.hint_processor import GoalHintProcessor
from elephantbroker.schemas.config import GoalRefinementConfig
from elephantbroker.schemas.goal import GoalState, GoalStatus
from tests.fixtures.factories import make_goal_state


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_task(*, llm=None, config=None, trace=None) -> GoalRefinementTask:
    return GoalRefinementTask(
        llm_client=llm,
        config=config or GoalRefinementConfig(),
        trace_ledger=trace,
    )


def _make_mock_store():
    store = AsyncMock()
    store.update_goal = AsyncMock()
    store.add_goal = AsyncMock()
    return store


def _make_processor(*, store=None, task=None, config=None) -> GoalHintProcessor:
    return GoalHintProcessor(
        session_goal_store=store or _make_mock_store(),
        goal_refinement_task=task or _make_task(),
        config=config or GoalRefinementConfig(),
    )


# ===========================================================================
# Tier 1: completed
# ===========================================================================

class TestTier1Completed:
    @pytest.mark.asyncio
    async def test_completed_sets_status(self):
        task = _make_task()
        goal = make_goal_state(status=GoalStatus.ACTIVE)
        result = await task.process_hint(goal, "completed", "all tests pass")
        assert result is not None
        assert result.status == GoalStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_completed_appends_evidence_to_criteria(self):
        task = _make_task()
        goal = make_goal_state(success_criteria=["criterion 1"])
        result = await task.process_hint(goal, "completed", "criterion 2")
        assert "criterion 2" in result.success_criteria
        assert "criterion 1" in result.success_criteria

    @pytest.mark.asyncio
    async def test_completed_does_not_duplicate_evidence(self):
        task = _make_task()
        goal = make_goal_state(success_criteria=["already here"])
        result = await task.process_hint(goal, "completed", "already here")
        assert result.success_criteria.count("already here") == 1

    @pytest.mark.asyncio
    async def test_completed_updates_timestamp(self):
        task = _make_task()
        old_time = datetime(2020, 1, 1, tzinfo=UTC)
        goal = make_goal_state(updated_at=old_time)
        result = await task.process_hint(goal, "completed", "done")
        assert result.updated_at > old_time


# ===========================================================================
# Tier 1: abandoned
# ===========================================================================

class TestTier1Abandoned:
    @pytest.mark.asyncio
    async def test_abandoned_sets_status(self):
        task = _make_task()
        goal = make_goal_state(status=GoalStatus.ACTIVE)
        result = await task.process_hint(goal, "abandoned", "")
        assert result.status == GoalStatus.ABANDONED

    @pytest.mark.asyncio
    async def test_abandoned_updates_timestamp(self):
        task = _make_task()
        old_time = datetime(2020, 1, 1, tzinfo=UTC)
        goal = make_goal_state(updated_at=old_time)
        result = await task.process_hint(goal, "abandoned", "")
        assert result.updated_at > old_time


# ===========================================================================
# Tier 1: blocked
# ===========================================================================

class TestTier1Blocked:
    @pytest.mark.asyncio
    async def test_blocked_appends_blocker(self):
        task = _make_task()
        goal = make_goal_state(blockers=[])
        result = await task.process_hint(goal, "blocked", "dependency missing")
        assert "dependency missing" in result.blockers

    @pytest.mark.asyncio
    async def test_blocked_does_not_duplicate_blocker(self):
        task = _make_task()
        goal = make_goal_state(blockers=["dependency missing"])
        result = await task.process_hint(goal, "blocked", "dependency missing")
        assert result.blockers.count("dependency missing") == 1


# ===========================================================================
# Tier 1: progressed
# ===========================================================================

class TestTier1Progressed:
    @pytest.mark.asyncio
    async def test_progressed_increases_confidence(self):
        task = _make_task()
        goal = make_goal_state(confidence=0.5)
        result = await task.process_hint(goal, "progressed", "step done")
        assert result.confidence == pytest.approx(0.6)

    @pytest.mark.asyncio
    async def test_progressed_caps_at_one(self):
        task = _make_task()
        goal = make_goal_state(confidence=0.95)
        result = await task.process_hint(goal, "progressed", "almost done")
        assert result.confidence == pytest.approx(1.0)


# ===========================================================================
# Tier 2: refined (LLM)
# ===========================================================================

class TestTier2Refined:
    @pytest.mark.asyncio
    async def test_refined_triggers_llm(self):
        llm = AsyncMock()
        llm.complete_json = AsyncMock(return_value={
            "title": "Refined title",
            "description": "Refined desc",
            "success_criteria": ["c1"],
        })
        task = _make_task(llm=llm)
        goal = make_goal_state(title="Old title")
        result = await task.process_hint(goal, "refined", "new evidence")
        assert result.title == "Refined title"
        assert result.description == "Refined desc"
        assert result.success_criteria == ["c1"]
        llm.complete_json.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_refined_without_llm_returns_goal_unchanged(self):
        task = _make_task(llm=None)
        goal = make_goal_state(title="Original")
        result = await task.process_hint(goal, "refined", "evidence")
        assert result.title == "Original"

    @pytest.mark.asyncio
    async def test_refined_llm_error_returns_goal(self):
        llm = AsyncMock()
        llm.complete_json = AsyncMock(side_effect=RuntimeError("LLM down"))
        task = _make_task(llm=llm)
        goal = make_goal_state(title="Keep me")
        result = await task.process_hint(goal, "refined", "evidence")
        assert result is not None
        assert result.title == "Keep me"


# ===========================================================================
# Tier 2: new_subgoal (LLM)
# ===========================================================================

class TestTier2NewSubgoal:
    @pytest.mark.asyncio
    async def test_new_subgoal_creates_child(self):
        llm = AsyncMock()
        llm.complete_json = AsyncMock(return_value={
            "title": "Sub task A",
            "description": "Detail",
            "success_criteria": ["done"],
        })
        task = _make_task(llm=llm)
        parent = make_goal_state(title="Parent goal")
        result = await task.process_hint(
            parent, "new_subgoal", "need sub",
            session_goals=[parent],
        )
        assert result is not None
        assert result.parent_goal_id == parent.id
        assert result.title == "Sub task A"

    @pytest.mark.asyncio
    async def test_new_subgoal_without_llm_uses_evidence(self):
        task = _make_task(llm=None)
        parent = make_goal_state()
        result = await task.process_hint(
            parent, "new_subgoal", "implement logging",
            session_goals=[parent],
        )
        assert result is not None
        assert result.title == "implement logging"
        assert result.parent_goal_id == parent.id


# ===========================================================================
# Limits
# ===========================================================================

class TestLimits:
    @pytest.mark.asyncio
    async def test_subgoal_limit_enforced(self):
        config = GoalRefinementConfig(max_subgoals_per_session=2)
        task = _make_task(config=config)
        parent = make_goal_state()
        # Two existing children already at limit
        children = [
            make_goal_state(parent_goal_id=parent.id),
            make_goal_state(parent_goal_id=parent.id),
        ]
        all_goals = [parent] + children
        result = await task.process_hint(
            parent, "new_subgoal", "one more",
            session_goals=all_goals,
        )
        assert result is None

    @pytest.mark.asyncio
    async def test_disabled_hints_return_none(self):
        config = GoalRefinementConfig(hints_enabled=False)
        task = _make_task(config=config)
        goal = make_goal_state()
        result = await task.process_hint(goal, "completed", "done")
        assert result is None

    @pytest.mark.asyncio
    async def test_invalid_hint_type_returns_none(self):
        task = _make_task()
        goal = make_goal_state()
        result = await task.process_hint(goal, "unknown_hint", "data")
        assert result is None


# ===========================================================================
# GoalRefinementTask internals
# ===========================================================================

class TestGoalRefinementTaskInternals:
    @pytest.mark.asyncio
    async def test_refine_updates_fields(self):
        llm = AsyncMock()
        llm.complete_json = AsyncMock(return_value={
            "title": "Better title",
            "description": "Better desc",
        })
        task = _make_task(llm=llm)
        goal = make_goal_state(title="Old", description="Old desc")
        result = await task._refine_goal(goal, "evidence", [])
        assert result.title == "Better title"
        assert result.description == "Better desc"

    @pytest.mark.asyncio
    async def test_subgoal_parent_id_set(self):
        llm = AsyncMock()
        llm.complete_json = AsyncMock(return_value={"title": "Child"})
        task = _make_task(llm=llm)
        parent = make_goal_state()
        result = await task._create_subgoal(parent, "ev", [], [])
        assert result is not None
        assert result.parent_goal_id == parent.id

    @pytest.mark.asyncio
    async def test_llm_error_in_subgoal_returns_none(self):
        llm = AsyncMock()
        llm.complete_json = AsyncMock(side_effect=ValueError("bad JSON"))
        task = _make_task(llm=llm)
        parent = make_goal_state()
        result = await task._create_subgoal(parent, "ev", [], [])
        assert result is None

    @pytest.mark.asyncio
    async def test_refinement_disabled_returns_none_for_tier2(self):
        config = GoalRefinementConfig(refinement_task_enabled=False)
        task = _make_task(config=config)
        goal = make_goal_state()
        result = await task.process_hint(goal, "refined", "data")
        assert result is None


# ===========================================================================
# Hierarchy: parent confidence from completion ratio
# ===========================================================================

class TestHierarchyConfidence:
    @pytest.mark.asyncio
    async def test_completed_child_does_not_change_parent_confidence(self):
        """Completing a child only updates the child, not the parent directly."""
        task = _make_task()
        parent = make_goal_state(confidence=0.5)
        child = make_goal_state(parent_goal_id=parent.id, confidence=0.3)
        result = await task.process_hint(child, "completed", "done")
        assert result.status == GoalStatus.COMPLETED
        # Parent untouched by this operation
        assert parent.confidence == pytest.approx(0.5)

    @pytest.mark.asyncio
    async def test_progressed_child_confidence_independent_of_parent(self):
        task = _make_task()
        parent = make_goal_state(confidence=0.8)
        child = make_goal_state(parent_goal_id=parent.id, confidence=0.4)
        result = await task.process_hint(child, "progressed", "step")
        assert result.confidence == pytest.approx(0.5)
        assert parent.confidence == pytest.approx(0.8)


# ===========================================================================
# Dedup: Jaccard similarity
# ===========================================================================

class TestDedup:
    def test_duplicate_rejected(self):
        task = _make_task()
        parent = make_goal_state()
        existing = make_goal_state(
            title="implement user authentication",
            parent_goal_id=parent.id,
        )
        # Very similar title (high Jaccard)
        result = task._should_create_subgoal(
            parent.id, "implement user authentication system",
            [existing],
        )
        assert result is False

    def test_unique_accepted(self):
        task = _make_task()
        parent = make_goal_state()
        existing = make_goal_state(
            title="implement user authentication",
            parent_goal_id=parent.id,
        )
        # Very different title (low Jaccard)
        result = task._should_create_subgoal(
            parent.id, "deploy database migrations",
            [existing],
        )
        assert result is True


# ===========================================================================
# GoalHintProcessor: dispatching
# ===========================================================================

class TestGoalHintProcessorDispatch:
    @pytest.mark.asyncio
    async def test_dispatches_tier1_directly(self):
        store = _make_mock_store()
        task = _make_task()
        config = GoalRefinementConfig(run_refinement_async=False)
        processor = _make_processor(store=store, task=task, config=config)

        goal = make_goal_state(status=GoalStatus.ACTIVE)
        hints = [{"goal_index": 0, "hint": "completed", "evidence": "done"}]
        await processor.process_hints(
            hints, [goal],
            session_key="agent:main:main",
            session_id=uuid.uuid4(),
        )
        store.update_goal.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_dispatches_tier2_sync_when_not_async(self):
        store = _make_mock_store()
        llm = AsyncMock()
        llm.complete_json = AsyncMock(return_value={
            "title": "Refined",
            "description": "Better",
        })
        task = _make_task(llm=llm)
        config = GoalRefinementConfig(run_refinement_async=False)
        processor = _make_processor(store=store, task=task, config=config)

        goal = make_goal_state(status=GoalStatus.ACTIVE)
        hints = [{"goal_index": 0, "hint": "refined", "evidence": "new info"}]
        await processor.process_hints(
            hints, [goal],
            session_key="agent:main:main",
            session_id=uuid.uuid4(),
        )
        store.update_goal.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_dispatches_new_subgoal_calls_add_goal(self):
        store = _make_mock_store()
        llm = AsyncMock()
        llm.complete_json = AsyncMock(return_value={"title": "Sub"})
        task = _make_task(llm=llm)
        config = GoalRefinementConfig(run_refinement_async=False)
        processor = _make_processor(store=store, task=task, config=config)

        goal = make_goal_state()
        hints = [{"goal_index": 0, "hint": "new_subgoal", "evidence": "need sub"}]
        await processor.process_hints(
            hints, [goal],
            session_key="agent:main:main",
            session_id=uuid.uuid4(),
        )
        store.add_goal.assert_awaited_once()


# ===========================================================================
# GoalHintProcessor: disabled
# ===========================================================================

class TestGoalHintProcessorDisabled:
    @pytest.mark.asyncio
    async def test_no_hints_when_disabled(self):
        store = _make_mock_store()
        task = _make_task()
        config = GoalRefinementConfig(hints_enabled=False)
        processor = _make_processor(store=store, task=task, config=config)

        goal = make_goal_state()
        hints = [{"goal_index": 0, "hint": "completed", "evidence": "done"}]
        await processor.process_hints(
            hints, [goal],
            session_key="agent:main:main",
            session_id=uuid.uuid4(),
        )
        store.update_goal.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_no_hints_when_empty_list(self):
        store = _make_mock_store()
        task = _make_task()
        processor = _make_processor(store=store, task=task)

        await processor.process_hints(
            [], [make_goal_state()],
            session_key="agent:main:main",
            session_id=uuid.uuid4(),
        )
        store.update_goal.assert_not_awaited()
        store.add_goal.assert_not_awaited()
