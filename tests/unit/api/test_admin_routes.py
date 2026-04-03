"""Tests for admin API routes."""
import os
import tempfile
import uuid

import pytest
from httpx import ASGITransport, AsyncClient
from unittest.mock import AsyncMock, MagicMock

from elephantbroker.api.app import create_app
from elephantbroker.runtime.container import RuntimeContainer
from elephantbroker.runtime.profiles.authority_store import AuthorityRuleStore
from elephantbroker.runtime.profiles.registry import ProfileRegistry
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.schemas.actor import ActorRef, ActorType
from elephantbroker.schemas.tiers import BusinessTier


@pytest.fixture
async def auth_store():
    with tempfile.TemporaryDirectory() as tmp:
        s = AuthorityRuleStore(db_path=os.path.join(tmp, "test_auth.db"))
        await s.init_db()
        yield s
        await s.close()


@pytest.fixture
def admin_actor():
    """A system admin actor with authority 90."""
    return ActorRef(
        type=ActorType.HUMAN_COORDINATOR,
        display_name="admin",
        authority_level=90,
        org_id=uuid.UUID("aaaaaaaa-0000-0000-0000-000000000001"),
        team_ids=[uuid.UUID("bbbbbbbb-0000-0000-0000-000000000001")],
        gateway_id="local",
    )


@pytest.fixture
def low_actor():
    """A regular actor with authority 0."""
    return ActorRef(
        type=ActorType.WORKER_AGENT, display_name="agent", authority_level=0, gateway_id="local",
    )


@pytest.fixture
async def org_store():
    from elephantbroker.runtime.profiles.org_override_store import OrgOverrideStore
    with tempfile.TemporaryDirectory() as tmp:
        s = OrgOverrideStore(db_path=os.path.join(tmp, "test_overrides.db"))
        await s.init_db()
        yield s
        await s.close()


@pytest.fixture
def admin_container(auth_store, org_store, admin_actor, low_actor):
    c = RuntimeContainer()
    c.tier = BusinessTier.FULL
    c.trace_ledger = TraceLedger()
    c.profile_registry = ProfileRegistry(c.trace_ledger, org_store=org_store)

    # Mock graph
    c.graph = AsyncMock()
    c.graph.query_cypher = AsyncMock(return_value=[])
    c.graph.get_entity = AsyncMock(return_value=None)
    c.graph.add_relation = AsyncMock()
    c.graph.delete_relation = AsyncMock()

    # Actor registry that returns admin or low actor based on ID
    c.actor_registry = AsyncMock()

    def resolve_side_effect(aid):
        if aid == admin_actor.id:
            return admin_actor
        if aid == low_actor.id:
            return low_actor
        return None

    c.actor_registry.resolve_actor = AsyncMock(side_effect=resolve_side_effect)
    c.actor_registry.register_actor = AsyncMock(side_effect=lambda a: a)
    c.goal_manager = AsyncMock()
    c.goal_manager.set_goal = AsyncMock(side_effect=lambda g: g)

    # Wire authority and identity
    c.authority_store = auth_store
    c._bootstrap_mode = False

    # Mock cognee
    from unittest.mock import patch
    return c, admin_actor, low_actor


@pytest.fixture
async def admin_client(admin_container, monkeypatch):
    c, admin_actor, low_actor = admin_container

    # Mock add_data_points globally (same pattern as conftest.py)
    async def fake_add_dp(data_points, context=None, custom_edges=None, embed_triplets=False):
        return list(data_points)

    monkeypatch.setattr("elephantbroker.api.routes.admin.add_data_points", fake_add_dp)

    # Also mock cognee imports used by other routes loaded via create_app
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


class TestAdminRoutes:
    async def test_bootstrap_status(self, admin_client):
        client, c, _, _ = admin_client
        resp = await client.get("/admin/bootstrap-status")
        assert resp.status_code == 200
        assert resp.json()["bootstrap_mode"] is False

    async def test_bootstrap_mode_active(self, admin_client):
        client, c, _, _ = admin_client
        c._bootstrap_mode = True
        c._bootstrap_checked = True
        resp = await client.get("/admin/bootstrap-status")
        assert resp.json()["bootstrap_mode"] is True

    async def test_create_org_requires_authority_90(self, admin_client):
        client, c, admin, low = admin_client
        # Admin succeeds
        resp = await client.post(
            "/admin/organizations",
            json={"name": "Acme", "display_label": "Acme"},
            headers={"X-EB-Actor-Id": str(admin.id)},
        )
        assert resp.status_code == 200
        assert "org_id" in resp.json()

    async def test_create_org_unauthorized(self, admin_client):
        client, c, _, low = admin_client
        resp = await client.post(
            "/admin/organizations",
            json={"name": "Acme"},
            headers={"X-EB-Actor-Id": str(low.id)},
        )
        assert resp.status_code == 403

    async def test_list_authority_rules(self, admin_client):
        client, c, _, _ = admin_client
        resp = await client.get("/admin/authority-rules")
        assert resp.status_code == 200
        data = resp.json()
        assert "create_org" in data
        assert "create_team" in data

    async def test_create_team_requires_matching_org(self, admin_client):
        client, c, admin, _ = admin_client
        resp = await client.post(
            "/admin/teams",
            json={"name": "Backend", "org_id": str(admin.org_id)},
            headers={"X-EB-Actor-Id": str(admin.id)},
        )
        assert resp.status_code == 200
        assert "team_id" in resp.json()

    async def test_add_member(self, admin_client):
        client, c, admin, _ = admin_client
        team_id = str(uuid.uuid4())
        resp = await client.post(
            f"/admin/teams/{team_id}/members",
            json={"actor_id": str(uuid.uuid4())},
            headers={"X-EB-Actor-Id": str(admin.id)},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "added"

    async def test_remove_member(self, admin_client):
        client, c, admin, _ = admin_client
        team_id = str(uuid.uuid4())
        actor_id = str(uuid.uuid4())
        resp = await client.delete(
            f"/admin/teams/{team_id}/members/{actor_id}",
            headers={"X-EB-Actor-Id": str(admin.id)},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "removed"

    async def test_register_actor(self, admin_client):
        client, c, admin, _ = admin_client
        resp = await client.post(
            "/admin/actors",
            json={"type": "human_operator", "display_name": "Maria", "authority_level": 50,
                  "org_id": str(uuid.uuid4()), "team_ids": [str(uuid.uuid4())]},
            headers={"X-EB-Actor-Id": str(admin.id)},
        )
        assert resp.status_code == 200

    async def test_bootstrap_mode_allows_first_actor(self, admin_client):
        client, c, _, _ = admin_client
        c._bootstrap_mode = True
        c._bootstrap_checked = True
        resp = await client.post(
            "/admin/actors",
            json={"type": "human_coordinator", "display_name": "Bootstrap Admin", "authority_level": 90},
            headers={"X-EB-Actor-Id": str(uuid.uuid4())},
        )
        assert resp.status_code == 200
        # Bootstrap mode disabled after first actor
        assert c._bootstrap_mode is False

    async def test_bootstrap_mode_disabled_after_first_actor(self, admin_client):
        client, c, _, _ = admin_client
        c._bootstrap_mode = True
        c._bootstrap_checked = True
        await client.post(
            "/admin/actors",
            json={"type": "human_coordinator", "display_name": "Admin", "authority_level": 90},
            headers={"X-EB-Actor-Id": str(uuid.uuid4())},
        )
        assert c._bootstrap_mode is False

    async def test_create_persistent_goal_org_scope(self, admin_client):
        client, c, admin, _ = admin_client
        resp = await client.post(
            "/admin/goals",
            json={"title": "Q1 Roadmap", "scope": "organization", "org_id": str(admin.org_id)},
            headers={"X-EB-Actor-Id": str(admin.id)},
        )
        assert resp.status_code == 200

    async def test_create_global_goal_requires_90(self, admin_client):
        client, c, _, low = admin_client
        resp = await client.post(
            "/admin/goals",
            json={"title": "Privacy First", "scope": "global"},
            headers={"X-EB-Actor-Id": str(low.id)},
        )
        assert resp.status_code == 403

    async def test_set_profile_override(self, admin_client):
        client, c, admin, _ = admin_client
        org_id = str(admin.org_id)
        resp = await client.put(
            f"/admin/profiles/overrides/{org_id}/coding",
            json={"overrides": {"scoring_weights": {"evidence_strength": 0.99}}},
            headers={"X-EB-Actor-Id": str(admin.id)},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "set"

    async def test_no_actor_id_returns_401(self, admin_client):
        client, c, _, _ = admin_client
        resp = await client.post("/admin/organizations", json={"name": "Acme"})
        assert resp.status_code == 401

    async def test_list_teams_filters_by_org(self, admin_client):
        client, c, admin, _ = admin_client
        org_id = str(uuid.uuid4())
        resp = await client.get(
            f"/admin/teams?org_id={org_id}",
            headers={"X-EB-Actor-Id": str(admin.id)},
        )
        assert resp.status_code == 200

    async def test_list_team_members(self, admin_client):
        client, c, admin, _ = admin_client
        team_id = str(uuid.uuid4())
        resp = await client.get(
            f"/admin/teams/{team_id}/members",
            headers={"X-EB-Actor-Id": str(admin.id)},
        )
        assert resp.status_code == 200
