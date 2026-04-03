"""Tests for GoalManager."""
import uuid
from unittest.mock import AsyncMock

import pytest

from elephantbroker.runtime.goals.manager import GoalManager
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.schemas.goal import GoalStatus
from tests.fixtures.factories import make_goal_state


class TestGoalManager:
    def _make(self):
        graph = AsyncMock()
        ledger = TraceLedger()
        return GoalManager(graph, ledger, dataset_name="test_ds"), graph, ledger

    async def test_set_goal(self, monkeypatch, mock_add_data_points, mock_cognee):
        mgr, graph, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.goals.manager.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.goals.manager.cognee", mock_cognee)
        goal = make_goal_state()
        result = await mgr.set_goal(goal)
        assert result.id == goal.id

    async def test_resolve_active_goals(self):
        mgr, graph, _ = self._make()
        graph.query_cypher = AsyncMock(return_value=[])
        goals = await mgr.resolve_active_goals(uuid.uuid4())
        assert goals == []

    async def test_get_goal_hierarchy_empty(self):
        mgr, graph, _ = self._make()
        graph.get_entity = AsyncMock(return_value=None)
        h = await mgr.get_goal_hierarchy(uuid.uuid4())
        assert h.root_goals == []

    async def test_update_goal_status(self, monkeypatch, mock_add_data_points, mock_cognee):
        mgr, graph, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.goals.manager.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.goals.manager.cognee", mock_cognee)
        goal = make_goal_state()
        graph.get_entity = AsyncMock(return_value={
            "eb_id": str(goal.id), "title": goal.title, "description": "",
            "status": "active", "scope": "session", "eb_created_at": 0,
            "eb_updated_at": 0, "owner_actor_ids": [], "success_criteria": [],
            "blockers": [], "confidence": 1.0,
        })
        result = await mgr.update_goal_status(goal.id, GoalStatus.COMPLETED)
        assert result.status == GoalStatus.COMPLETED

    async def test_set_goal_emits_trace(self, monkeypatch, mock_add_data_points, mock_cognee):
        mgr, graph, ledger = self._make()
        monkeypatch.setattr("elephantbroker.runtime.goals.manager.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.goals.manager.cognee", mock_cognee)
        await mgr.set_goal(make_goal_state())
        from elephantbroker.schemas.trace import TraceQuery
        events = await ledger.query_trace(TraceQuery())
        assert len(events) == 1

    async def test_set_goal_with_parent(self, monkeypatch, mock_add_data_points, mock_cognee):
        mgr, graph, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.goals.manager.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.goals.manager.cognee", mock_cognee)
        goal = make_goal_state(parent_goal_id=uuid.uuid4())
        graph.add_relation = AsyncMock()
        await mgr.set_goal(goal)
        graph.add_relation.assert_called_once()

    async def test_set_goal_calls_add_data_points(self, monkeypatch, mock_add_data_points, mock_cognee):
        """CREATE: add_data_points called with GoalDataPoint."""
        mgr, graph, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.goals.manager.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.goals.manager.cognee", mock_cognee)
        goal = make_goal_state()
        await mgr.set_goal(goal)
        assert len(mock_add_data_points.calls) == 1
        dp = mock_add_data_points.calls[0]["data_points"][0]
        assert dp.eb_id == str(goal.id)

    async def test_set_goal_calls_cognee_add_with_goal_text(self, monkeypatch, mock_add_data_points, mock_cognee):
        """CREATE: cognee.add() called with goal title + description."""
        mgr, graph, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.goals.manager.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.goals.manager.cognee", mock_cognee)
        goal = make_goal_state(title="My special goal", description="A test goal")
        await mgr.set_goal(goal)
        mock_cognee.add.assert_called_once()
        text = mock_cognee.add.call_args[0][0]
        assert "My special goal" in text

    async def test_update_goal_status_calls_add_data_points_not_cognee_add(self, monkeypatch, mock_add_data_points, mock_cognee):
        """UPDATE: add_data_points called but cognee.add() is NOT called."""
        mgr, graph, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.goals.manager.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.goals.manager.cognee", mock_cognee)
        goal = make_goal_state()
        graph.get_entity = AsyncMock(return_value={
            "eb_id": str(goal.id), "title": goal.title, "description": "",
            "status": "active", "scope": "session", "eb_created_at": 0,
            "eb_updated_at": 0, "owner_actor_ids": [], "success_criteria": [],
            "blockers": [], "confidence": 1.0,
        })
        await mgr.update_goal_status(goal.id, GoalStatus.COMPLETED)
        assert len(mock_add_data_points.calls) == 1
        mock_cognee.add.assert_not_called()

    async def test_update_goal_status_raises_on_missing(self):
        mgr, graph, _ = self._make()
        graph.get_entity = AsyncMock(return_value=None)
        with pytest.raises(KeyError):
            await mgr.update_goal_status(uuid.uuid4(), GoalStatus.COMPLETED)

    async def test_resolve_active_goals_returns_goals(self, monkeypatch, mock_add_data_points, mock_cognee):
        mgr, graph, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.goals.manager.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.goals.manager.cognee", mock_cognee)
        goal = make_goal_state()
        graph.query_cypher = AsyncMock(return_value=[{
            "props": {
                "eb_id": str(goal.id), "title": goal.title, "description": "",
                "status": "active", "scope": "session", "eb_created_at": 0,
                "eb_updated_at": 0, "owner_actor_ids": [], "success_criteria": [],
                "blockers": [], "confidence": 1.0,
            }
        }])
        goals = await mgr.resolve_active_goals(uuid.uuid4())
        assert len(goals) == 1
        assert goals[0].title == goal.title

    async def test_get_goal_hierarchy_with_children(self, monkeypatch, mock_add_data_points, mock_cognee):
        mgr, graph, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.goals.manager.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.goals.manager.cognee", mock_cognee)
        root = make_goal_state(title="Root")
        child = make_goal_state(title="Child")
        graph.get_entity = AsyncMock(return_value={
            "eb_id": str(root.id), "title": root.title, "description": "",
            "status": "active", "scope": "session", "eb_created_at": 0,
            "eb_updated_at": 0, "owner_actor_ids": [], "success_criteria": [],
            "blockers": [], "confidence": 1.0,
        })
        graph.query_cypher = AsyncMock(return_value=[{
            "props": {
                "eb_id": str(child.id), "title": child.title, "description": "",
                "status": "active", "scope": "session", "eb_created_at": 0,
                "eb_updated_at": 0, "owner_actor_ids": [], "success_criteria": [],
                "blockers": [], "confidence": 1.0,
            }
        }])
        h = await mgr.get_goal_hierarchy(root.id)
        assert len(h.root_goals) == 1
        assert str(root.id) in h.children
        assert len(h.children[str(root.id)]) == 1

    async def test_set_goal_creates_owns_goal_edges(self, monkeypatch, mock_add_data_points, mock_cognee):
        """OWNS_GOAL edges created for each owner_actor_id."""
        mgr, graph, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.goals.manager.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.goals.manager.cognee", mock_cognee)
        graph.add_relation = AsyncMock()
        owner1, owner2 = uuid.uuid4(), uuid.uuid4()
        goal = make_goal_state(owner_actor_ids=[owner1, owner2])
        await mgr.set_goal(goal)
        owns_calls = [c for c in graph.add_relation.call_args_list
                      if "OWNS_GOAL" in str(c)]
        assert len(owns_calls) == 2

    async def test_set_goal_no_owners_no_edges(self, monkeypatch, mock_add_data_points, mock_cognee):
        """No OWNS_GOAL edges when owner_actor_ids is empty."""
        mgr, graph, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.goals.manager.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.goals.manager.cognee", mock_cognee)
        graph.add_relation = AsyncMock()
        goal = make_goal_state(owner_actor_ids=[])
        await mgr.set_goal(goal)
        owns_calls = [c for c in graph.add_relation.call_args_list
                      if "OWNS_GOAL" in str(c)]
        assert len(owns_calls) == 0

    async def test_owns_goal_best_effort_on_missing_actor(self, monkeypatch, mock_add_data_points, mock_cognee):
        """OWNS_GOAL edge failure doesn't block goal creation."""
        mgr, graph, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.goals.manager.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.goals.manager.cognee", mock_cognee)
        graph.add_relation = AsyncMock(side_effect=Exception("Actor not found"))
        goal = make_goal_state(owner_actor_ids=[uuid.uuid4()])
        result = await mgr.set_goal(goal)
        assert result.title == goal.title  # Goal still created despite edge failure
