"""Tests for procedure engine Phase 7 — completion gate + auto-goals."""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock

import pytest

from elephantbroker.runtime.procedures.engine import ProcedureEngine
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.schemas.goal import GoalState, GoalStatus
from elephantbroker.schemas.procedure import ProcedureDefinition, ProcedureStep, ProofRequirement, ProofType


def _make(with_goals=False):
    graph = AsyncMock()
    ledger = TraceLedger()
    engine = ProcedureEngine(graph, ledger, dataset_name="test", gateway_id="test")
    goal_store = None
    if with_goals:
        goal_store = AsyncMock()
        goal_store.add_goal = AsyncMock()
        goal_store.get_goals = AsyncMock(return_value=[])
        goal_store.update_goal = AsyncMock()
        engine._session_goal_store = goal_store
    return engine, graph, goal_store


class TestGetActiveExecutionIds:
    async def test_returns_active_executions(self):
        engine, graph, _ = _make()
        proc_id = uuid.uuid4()
        sid = uuid.uuid4()
        graph.get_entity = AsyncMock(return_value={"eb_id": str(proc_id), "name": "test"})
        execution = await engine.activate(proc_id, session_key="sk", session_id=sid)
        result = await engine.get_active_execution_ids("sk", sid)
        assert proc_id in result

    async def test_returns_empty_for_unknown_session(self):
        engine, _, _ = _make()
        result = await engine.get_active_execution_ids("unknown", uuid.uuid4())
        assert result == []


class TestCheckStepWithProof:
    async def test_step_without_evidence_engine_completes(self):
        engine, graph, _ = _make()
        proc_id = uuid.uuid4()
        graph.get_entity = AsyncMock(return_value={"eb_id": str(proc_id), "name": "test"})
        execution = await engine.activate(proc_id)
        step_id = uuid.uuid4()
        result = await engine.check_step(execution.execution_id, step_id)
        assert result.complete is True

    async def test_step_missing_execution(self):
        engine, _, _ = _make()
        result = await engine.check_step(uuid.uuid4(), uuid.uuid4())
        assert result.complete is False
        assert "not found" in result.missing_evidence[0].lower()


class TestValidateCompletionGate:
    async def test_fallback_without_evidence_engine(self):
        engine, graph, _ = _make()
        proc_id = uuid.uuid4()
        graph.get_entity = AsyncMock(return_value={"eb_id": str(proc_id), "name": "test"})
        execution = await engine.activate(proc_id)
        # No steps completed → incomplete
        result = await engine.validate_completion(execution.execution_id)
        assert result.complete is False

    async def test_fallback_with_completed_step(self):
        engine, graph, _ = _make()
        proc_id = uuid.uuid4()
        graph.get_entity = AsyncMock(return_value={"eb_id": str(proc_id), "name": "test"})
        execution = await engine.activate(proc_id)
        await engine.check_step(execution.execution_id, uuid.uuid4())
        result = await engine.validate_completion(execution.execution_id)
        assert result.complete is True


class TestAutoGoalCreation:
    async def test_activate_creates_goals_when_store_available(self):
        engine, graph, goal_store = _make(with_goals=True)
        proc_id = uuid.uuid4()
        step = ProcedureStep(order=0, instruction="Run tests",
                             required_evidence=[ProofRequirement(description="Test log", proof_type=ProofType.CHUNK_REF)])
        proc = ProcedureDefinition(id=proc_id, name="Deploy", steps=[step])
        # Mock graph to return entity that triggers auto-goals
        graph.get_entity = AsyncMock(return_value={"eb_id": str(proc_id), "name": "Deploy"})
        # Mock ProcedureDataPoint.to_schema_from_dict to return our proc
        import elephantbroker.runtime.adapters.cognee.datapoints as dp_mod
        orig = getattr(dp_mod, "ProcedureDataPoint", None)
        mock_dp = AsyncMock()
        mock_dp.to_schema_from_dict = lambda d: proc
        engine._definitions[proc_id] = proc  # Pre-cache
        execution = await engine.activate(proc_id, session_key="sk", session_id=uuid.uuid4())
        # Should have created goals: 1 parent + 1 sub-goal (for the proof step)
        assert goal_store.add_goal.call_count >= 2

    async def test_activate_no_goals_without_store(self):
        engine, graph, _ = _make(with_goals=False)
        proc_id = uuid.uuid4()
        graph.get_entity = AsyncMock(return_value={"eb_id": str(proc_id), "name": "test"})
        execution = await engine.activate(proc_id)
        # No exception, no goals created
        assert execution.procedure_id == proc_id


class TestAutoGoalEnforcement:
    async def test_manual_complete_of_auto_goal_rejected(self):
        """Agent cannot manually complete auto-goals."""
        from elephantbroker.runtime.working_set.session_goals import SessionGoalStore
        redis = AsyncMock()
        import json
        goal = GoalState(title="Auto goal", metadata={"source_type": "auto", "resolved_by_runtime": "false"})
        redis.get = AsyncMock(return_value=json.dumps([goal.model_dump(mode="json")]))
        redis.setex = AsyncMock()
        store = SessionGoalStore(redis=redis)
        with pytest.raises(ValueError, match="managed by the runtime"):
            await store.update_goal("sk", uuid.uuid4(), goal.id, {"status": GoalStatus.COMPLETED})

    async def test_runtime_can_complete_auto_goal(self):
        """Runtime (with resolved_by_runtime=true) can complete auto-goals."""
        from elephantbroker.runtime.working_set.session_goals import SessionGoalStore
        redis = AsyncMock()
        import json
        goal = GoalState(title="Auto goal", metadata={"source_type": "auto", "resolved_by_runtime": "false"})
        redis.get = AsyncMock(return_value=json.dumps([goal.model_dump(mode="json")]))
        redis.setex = AsyncMock()
        store = SessionGoalStore(redis=redis)
        result = await store.update_goal("sk", uuid.uuid4(), goal.id, {
            "status": GoalStatus.COMPLETED,
            "metadata": {"source_type": "auto", "resolved_by_runtime": "true"},
        })
        assert result is not None
        assert result.status == GoalStatus.COMPLETED

    async def test_non_auto_goal_can_be_completed(self):
        """Regular goals can be freely completed."""
        from elephantbroker.runtime.working_set.session_goals import SessionGoalStore
        redis = AsyncMock()
        import json
        goal = GoalState(title="Regular goal")
        redis.get = AsyncMock(return_value=json.dumps([goal.model_dump(mode="json")]))
        redis.setex = AsyncMock()
        store = SessionGoalStore(redis=redis)
        result = await store.update_goal("sk", uuid.uuid4(), goal.id, {"status": GoalStatus.COMPLETED})
        assert result is not None
        assert result.status == GoalStatus.COMPLETED
