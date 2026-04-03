"""Tests for ActorRegistry handle resolution and MEMBER_OF edges."""
import uuid

import pytest
from unittest.mock import AsyncMock, MagicMock

from elephantbroker.runtime.actors.registry import ActorRegistry
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.schemas.actor import ActorRef, ActorType


@pytest.fixture(autouse=True)
def _mock_cognee(monkeypatch):
    """Mock Cognee APIs for all actor registry tests."""
    async def fake_add_dp(data_points, **kwargs):
        return list(data_points)
    mock_cognee = MagicMock()
    mock_cognee.add = AsyncMock(return_value=None)
    monkeypatch.setattr("elephantbroker.runtime.actors.registry.add_data_points", fake_add_dp)
    monkeypatch.setattr("elephantbroker.runtime.actors.registry.cognee", mock_cognee)


def _make_registry(graph=None):
    g = graph or AsyncMock()
    g.get_entity = AsyncMock(return_value=None)
    g.add_relation = AsyncMock()
    g.query_cypher = AsyncMock(return_value=[])
    return ActorRegistry(g, TraceLedger(), dataset_name="test"), g


class TestResolveByHandle:
    async def test_resolve_by_platform_qualified_handle(self):
        reg, graph = _make_registry()
        graph.query_cypher = AsyncMock(return_value=[{"props": {
            "eb_id": str(uuid.uuid4()), "actor_type": "human_operator",
            "display_name": "Admin", "handles": ["telegram:admin_tg"],
            "authority_level": 50, "gateway_id": "local",
        }}])
        result = await reg.resolve_by_handle("telegram:admin_tg")
        assert result is not None
        assert result.display_name == "Admin"
        assert "telegram:admin_tg" in result.handles

    async def test_resolve_bare_handle_backward_compat(self):
        reg, graph = _make_registry()
        graph.query_cypher = AsyncMock(return_value=[{"props": {
            "eb_id": str(uuid.uuid4()), "actor_type": "worker_agent",
            "display_name": "bot", "handles": ["@bot"],
            "gateway_id": "local",
        }}])
        result = await reg.resolve_by_handle("@bot")
        assert result is not None
        assert result.display_name == "bot"

    async def test_resolve_nonexistent_handle_returns_none(self):
        reg, graph = _make_registry()
        graph.query_cypher = AsyncMock(return_value=[])
        result = await reg.resolve_by_handle("telegram:nobody")
        assert result is None

    async def test_resolve_multi_handle_actor_found_by_any(self):
        reg, graph = _make_registry()
        aid = str(uuid.uuid4())
        graph.query_cypher = AsyncMock(return_value=[{"props": {
            "eb_id": aid, "actor_type": "human_coordinator",
            "display_name": "Admin",
            "handles": ["telegram:admin_tg", "email:admin@acme.com", "slack:U0123ADM"],
            "gateway_id": "local",
        }}])
        result = await reg.resolve_by_handle("email:admin@acme.com")
        assert result is not None
        assert str(result.id) == aid


class TestRegisterActorMemberOfEdges:
    async def test_register_creates_member_of_edges(self):
        reg, graph = _make_registry()
        team1 = uuid.uuid4()
        team2 = uuid.uuid4()
        actor = ActorRef(
            type=ActorType.HUMAN_COORDINATOR,
            display_name="Admin",
            team_ids=[team1, team2],
        )
        await reg.register_actor(actor)
        # Two MEMBER_OF edges created
        member_of_calls = [c for c in graph.add_relation.call_args_list if c[0][2] == "MEMBER_OF"]
        assert len(member_of_calls) == 2

    async def test_register_no_teams_no_edges(self):
        reg, graph = _make_registry()
        actor = ActorRef(type=ActorType.WORKER_AGENT, display_name="bot")
        await reg.register_actor(actor)
        member_of_calls = [c for c in graph.add_relation.call_args_list if c[0][2] == "MEMBER_OF"]
        assert len(member_of_calls) == 0


class TestResolveActorBackwardCompat:
    async def test_resolve_old_team_id_field(self):
        """Old Neo4j nodes have team_id (string), not team_ids (list)."""
        reg, graph = _make_registry()
        team = str(uuid.uuid4())
        graph.get_entity = AsyncMock(return_value={
            "eb_id": str(uuid.uuid4()), "actor_type": "worker_agent",
            "display_name": "old-bot", "team_id": team,  # OLD field name
            "gateway_id": "local",
        })
        result = await reg.resolve_actor(uuid.uuid4())
        assert result is not None
        assert len(result.team_ids) == 1
        assert str(result.team_ids[0]) == team

    async def test_resolve_new_team_ids_field(self):
        """New Neo4j nodes have team_ids (list)."""
        reg, graph = _make_registry()
        team1 = str(uuid.uuid4())
        team2 = str(uuid.uuid4())
        graph.get_entity = AsyncMock(return_value={
            "eb_id": str(uuid.uuid4()), "actor_type": "human_coordinator",
            "display_name": "new-actor", "team_ids": [team1, team2],
            "gateway_id": "local",
        })
        result = await reg.resolve_actor(uuid.uuid4())
        assert result is not None
        assert len(result.team_ids) == 2
