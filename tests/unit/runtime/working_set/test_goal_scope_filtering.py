"""Tests for scope-aware persistent goal filtering (Phase 8)."""
import uuid

import pytest
from unittest.mock import AsyncMock

from elephantbroker.runtime.working_set.candidates import CandidateGenerator


def _make_gen(graph_records=None):
    graph = AsyncMock()
    graph.query_cypher = AsyncMock(return_value=graph_records or [])
    gen = CandidateGenerator(
        retrieval=AsyncMock(),
        goal_manager=AsyncMock(),
        graph=graph,
        gateway_id="gw-test",
    )
    gen._goal_manager = AsyncMock()
    gen._goal_manager.resolve_active_goals = AsyncMock(return_value=[])
    return gen, graph


def _goal_props(scope="global", title="Test Goal", org_id=None, team_id=None):
    return {"props": {
        "eb_id": str(uuid.uuid4()), "title": title,
        "description": "", "scope": scope, "status": "active",
        "gateway_id": "gw-test", "org_id": org_id, "team_id": team_id,
    }}


class TestPersistentGoalHierarchy:
    async def test_global_goal_visible_to_all(self):
        gen, graph = _make_gen([_goal_props(scope="global", title="Privacy First")])
        items = await gen._get_persistent_goal_items(org_id="acme", team_ids=["team1"])
        assert len(items) == 1
        assert "Privacy First" in items[0].text

    async def test_org_goal_visible_to_same_org(self):
        gen, graph = _make_gen([_goal_props(scope="organization", org_id="acme")])
        items = await gen._get_persistent_goal_items(org_id="acme", team_ids=[])
        assert len(items) == 1

    async def test_org_goal_hidden_from_other_org(self):
        gen, graph = _make_gen([])  # Query filtered by org_id → empty
        items = await gen._get_persistent_goal_items(org_id="other-org", team_ids=[])
        assert len(items) == 0

    async def test_team_goal_visible_to_team_members(self):
        gen, graph = _make_gen([_goal_props(scope="team", team_id="team1")])
        items = await gen._get_persistent_goal_items(org_id="acme", team_ids=["team1"])
        assert len(items) == 1

    async def test_team_goal_visible_to_multi_team_actor(self):
        gen, graph = _make_gen([_goal_props(scope="team", team_id="team2")])
        items = await gen._get_persistent_goal_items(org_id="acme", team_ids=["team1", "team2"])
        assert len(items) == 1

    async def test_actor_goal_visible_only_to_owner(self):
        gen, graph = _make_gen([_goal_props(scope="actor")])
        actor_id = uuid.uuid4()
        items = await gen._get_persistent_goal_items(
            actor_ids=[actor_id], org_id="acme", team_ids=["team1"]
        )
        assert len(items) == 1

    async def test_no_org_configured_uses_fallback(self):
        """When org_id and team_ids are None, use Phase 5 binary filter."""
        gen, graph = _make_gen([_goal_props(scope="global")])
        items = await gen._get_persistent_goal_items(actor_ids=[uuid.uuid4()])
        # Falls back to Phase 5 OWNS_GOAL filter
        assert len(items) == 1

    async def test_no_org_no_actors_returns_all(self):
        gen, graph = _make_gen([_goal_props(scope="global")])
        items = await gen._get_persistent_goal_items()
        assert len(items) == 1

    async def test_scope_aware_cypher_called_with_org(self):
        gen, graph = _make_gen([])
        await gen._get_persistent_goal_items(org_id="acme", team_ids=["t1"])
        cypher_call = graph.query_cypher.call_args
        assert "g.scope = 'global'" in cypher_call[0][0]
        assert "g.scope = 'organization'" in cypher_call[0][0]
        assert "g.scope = 'team'" in cypher_call[0][0]
        assert "g.scope = 'actor'" in cypher_call[0][0]

    async def test_fallback_cypher_without_org(self):
        gen, graph = _make_gen([])
        await gen._get_persistent_goal_items(actor_ids=[uuid.uuid4()])
        cypher_call = graph.query_cypher.call_args
        # Should use Phase 5 OWNS_GOAL filter, not scope-aware
        assert "OWNS_GOAL" in cypher_call[0][0]

    async def test_empty_team_ids_sees_no_team_goals(self):
        gen, graph = _make_gen([])
        await gen._get_persistent_goal_items(org_id="acme", team_ids=[])
        cypher_call = graph.query_cypher.call_args
        # Params are passed as the second positional arg
        params = cypher_call[0][1]
        assert params["team_ids"] == []

    async def test_generate_accepts_org_context(self):
        """generate() accepts org_id and team_ids parameters without error."""
        gen, graph = _make_gen([])
        gen._retrieval = AsyncMock()
        gen._retrieval.retrieve_candidates = AsyncMock(return_value=[])
        gen._session_goal_store = None
        gen._procedure_engine = None
        # Should not raise — org_id/team_ids accepted as params
        _, items = await gen.generate(
            session_id=uuid.uuid4(), session_key="sk", query="test",
            org_id="acme", team_ids=["t1", "t2"],
        )
        assert isinstance(items, list)
