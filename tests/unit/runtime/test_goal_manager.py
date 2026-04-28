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

    async def test_set_goal_auto_populates_org_team(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """TF-05-009 #550: ``set_goal(goal, org_id, team_id)`` auto-
        populates the goal's ``org_id`` / ``team_id`` from the kwargs
        when those fields are unset on the incoming schema.

        Pins ``goals/manager.py:36-42``: the enrichment is conditional on
        ``not goal.org_id`` / ``not goal.team_id`` so caller-supplied
        values are preserved (no override). Without the enrichment,
        scoped goals from API routes that pass identity from
        ``request.state`` would land with ``None`` org/team — the
        identity envelope would be silently dropped.
        """
        mgr, graph, _ = self._make()
        monkeypatch.setattr(
            "elephantbroker.runtime.goals.manager.add_data_points", mock_add_data_points,
        )
        monkeypatch.setattr(
            "elephantbroker.runtime.goals.manager.cognee", mock_cognee,
        )
        org_uuid = uuid.uuid4()
        team_uuid = uuid.uuid4()
        # Goal has neither org_id nor team_id set — must be enriched.
        goal = make_goal_state()
        assert goal.org_id is None
        assert goal.team_id is None
        result = await mgr.set_goal(
            goal, org_id=str(org_uuid), team_id=str(team_uuid),
        )
        assert result.org_id == org_uuid
        assert result.team_id == team_uuid

    async def test_set_goal_text_indexing_concatenation(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """TF-05-009 #551: the text passed to ``cognee.add()`` for
        Qdrant indexing follows the exact concatenation format
        ``"Goal: <title> — <description> criteria: c1, c2, ..."``.

        Pins ``goals/manager.py:46-51``. The sibling test
        ``test_set_goal_calls_cognee_add_with_goal_text`` only asserts
        the title substring is present; this one pins the *full* shape:
        the ``Goal:`` prefix, the EM-DASH separator (U+2014, NOT a plain
        hyphen), the ``criteria:`` keyword, and the ``", "`` join. A
        regression that swaps the separator (hyphen vs en-dash vs
        em-dash) or reorders the parts would leave the substring test
        passing while silently breaking semantic search ranking.
        """
        mgr, graph, _ = self._make()
        monkeypatch.setattr(
            "elephantbroker.runtime.goals.manager.add_data_points", mock_add_data_points,
        )
        monkeypatch.setattr(
            "elephantbroker.runtime.goals.manager.cognee", mock_cognee,
        )
        goal = make_goal_state(
            title="Ship feature X",
            description="Land the new endpoint",
            success_criteria=["tests green", "metric wired"],
        )
        await mgr.set_goal(goal)
        mock_cognee.add.assert_called_once()
        text = mock_cognee.add.call_args[0][0]
        # Em-dash separator (U+2014) — pin literally to catch a swap.
        expected = (
            "Goal: Ship feature X — Land the new endpoint "
            "criteria: tests green, metric wired"
        )
        assert text == expected

    async def test_get_goal_hierarchy_gateway_filtered(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """TF-05-009 #553: ``get_goal_hierarchy()`` Cypher restricts
        children to the same gateway as the manager.

        Pins ``goals/manager.py:131`` — the
        ``WHERE child.gateway_id = $gateway_id`` clause and the
        corresponding ``$gateway_id`` parameter on line 134. A
        regression that drops either piece would let a query rooted at
        a gateway-A goal pull in gateway-B children, violating the
        single-tenant-per-process boundary that
        ``GatewayIdentityMiddleware`` enforces at request entry.
        """
        graph = AsyncMock()
        ledger = TraceLedger()
        gw = "test-gateway-xyz"
        mgr = GoalManager(graph, ledger, dataset_name="test_ds", gateway_id=gw)
        monkeypatch.setattr(
            "elephantbroker.runtime.goals.manager.add_data_points", mock_add_data_points,
        )
        monkeypatch.setattr(
            "elephantbroker.runtime.goals.manager.cognee", mock_cognee,
        )
        root = make_goal_state(title="Root")
        graph.get_entity = AsyncMock(return_value={
            "eb_id": str(root.id), "title": root.title, "description": "",
            "status": "active", "scope": "session", "eb_created_at": 0,
            "eb_updated_at": 0, "owner_actor_ids": [], "success_criteria": [],
            "blockers": [], "confidence": 1.0,
        })
        graph.query_cypher = AsyncMock(return_value=[])
        await mgr.get_goal_hierarchy(root.id)
        graph.query_cypher.assert_called_once()
        cypher, params = graph.query_cypher.call_args[0]
        assert "child.gateway_id = $gateway_id" in cypher
        assert params["gateway_id"] == gw
