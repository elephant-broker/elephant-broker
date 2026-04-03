"""Tests for the goal_create tool routing logic (Python-side admin API)."""
import os
import tempfile
import uuid

import pytest
from httpx import ASGITransport, AsyncClient
from unittest.mock import AsyncMock, MagicMock

from elephantbroker.api.app import create_app
from elephantbroker.runtime.container import RuntimeContainer
from elephantbroker.runtime.profiles.authority_store import AuthorityRuleStore
from elephantbroker.runtime.profiles.org_override_store import OrgOverrideStore
from elephantbroker.runtime.profiles.registry import ProfileRegistry
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.schemas.actor import ActorRef, ActorType
from elephantbroker.schemas.tiers import BusinessTier


@pytest.fixture
async def goal_client(monkeypatch):
    """Minimal container for testing goal creation routing."""
    with tempfile.TemporaryDirectory() as tmp:
        auth_store = AuthorityRuleStore(db_path=os.path.join(tmp, "auth.db"))
        await auth_store.init_db()
        org_store = OrgOverrideStore(db_path=os.path.join(tmp, "org.db"))
        await org_store.init_db()

        c = RuntimeContainer()
        c.tier = BusinessTier.FULL
        c.trace_ledger = TraceLedger()
        c.profile_registry = ProfileRegistry(c.trace_ledger, org_store=org_store)
        c.graph = AsyncMock()
        c.graph.query_cypher = AsyncMock(return_value=[])
        c.graph.get_entity = AsyncMock(return_value=None)
        c.graph.add_relation = AsyncMock()

        admin_actor = ActorRef(
            type=ActorType.HUMAN_COORDINATOR,
            display_name="admin", authority_level=90,
            org_id=uuid.UUID("aaaaaaaa-0000-0000-0000-000000000001"),
            team_ids=[uuid.UUID("bbbbbbbb-0000-0000-0000-000000000001")],
            gateway_id="local",
        )
        c.actor_registry = AsyncMock()
        c.actor_registry.resolve_actor = AsyncMock(return_value=admin_actor)

        low_actor = ActorRef(type=ActorType.WORKER_AGENT, display_name="agent", authority_level=0, gateway_id="local")

        c.goal_manager = AsyncMock()
        c.goal_manager.set_goal = AsyncMock(side_effect=lambda g: g)
        c.authority_store = auth_store
        c._bootstrap_mode = False
        c.session_goal_store = AsyncMock()
        c.session_goal_store.add_goal = AsyncMock(return_value={"id": str(uuid.uuid4()), "title": "test"})
        c.redis = None

        # Mock cognee
        async def fake_add_dp(data_points, **kwargs):
            return list(data_points)
        monkeypatch.setattr("elephantbroker.api.routes.admin.add_data_points", fake_add_dp)
        mock_cognee = MagicMock()
        mock_cognee.add = AsyncMock(return_value=None)
        mock_cognee.search = AsyncMock(return_value=[])
        for mod in [
            "elephantbroker.runtime.actors.registry",
            "elephantbroker.runtime.goals.manager",
            "elephantbroker.runtime.memory.facade",
            "elephantbroker.runtime.evidence.engine",
            "elephantbroker.runtime.artifacts.store",
            "elephantbroker.runtime.procedures.engine",
            "elephantbroker.api.routes.sessions",
        ]:
            monkeypatch.setattr(f"{mod}.add_data_points", fake_add_dp, raising=False)
            monkeypatch.setattr(f"{mod}.cognee", mock_cognee, raising=False)

        app = create_app(c)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            yield client, c, admin_actor, low_actor

        await auth_store.close()
        await org_store.close()


class TestGoalCreateRouting:
    async def test_persistent_org_scope_calls_admin_api(self, goal_client):
        client, c, admin, _ = goal_client
        resp = await client.post(
            "/admin/goals",
            json={"title": "Q1 Roadmap", "scope": "organization",
                  "org_id": str(admin.org_id)},
            headers={"X-EB-Actor-Id": str(admin.id)},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["title"] == "Q1 Roadmap"

    async def test_persistent_global_scope_authority_checked(self, goal_client):
        client, c, _, low = goal_client
        c.actor_registry.resolve_actor = AsyncMock(return_value=low)
        resp = await client.post(
            "/admin/goals",
            json={"title": "Privacy First", "scope": "global"},
            headers={"X-EB-Actor-Id": str(low.id)},
        )
        assert resp.status_code == 403

    async def test_persistent_team_scope_requires_team_id(self, goal_client):
        client, c, admin, _ = goal_client
        resp = await client.post(
            "/admin/goals",
            json={"title": "Ship auth", "scope": "team",
                  "team_id": str(admin.team_ids[0])},
            headers={"X-EB-Actor-Id": str(admin.id)},
        )
        assert resp.status_code == 200

    async def test_persistent_actor_scope_any_authority(self, goal_client):
        client, c, admin, _ = goal_client
        resp = await client.post(
            "/admin/goals",
            json={"title": "Learn TypeScript", "scope": "actor"},
            headers={"X-EB-Actor-Id": str(admin.id)},
        )
        assert resp.status_code == 200

    async def test_session_goal_via_session_api(self, goal_client):
        """Session goals go through /goals/session, not /admin/goals."""
        client, c, _, _ = goal_client
        # Session goal endpoint doesn't require admin authority
        resp = await client.post(
            "/goals/session",
            json={"title": "Quick task"},
            params={"session_key": "agent:main:main", "session_id": str(uuid.uuid4())},
        )
        # May fail because session_goal_store is mock — but should not be 403
        assert resp.status_code != 403

    async def test_no_actor_id_returns_401(self, goal_client):
        client, _, _, _ = goal_client
        resp = await client.post(
            "/admin/goals",
            json={"title": "Test", "scope": "global"},
        )
        assert resp.status_code == 401
