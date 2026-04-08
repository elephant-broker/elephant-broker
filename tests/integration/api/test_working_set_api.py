"""API-level integration tests for the working set and session goal endpoints.

Uses the FastAPI test client backed by a real RuntimeContainer wired to
Docker infrastructure (Neo4j, Qdrant, Redis).
"""
from __future__ import annotations

import uuid

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from elephantbroker.api.app import create_app
from elephantbroker.runtime.container import RuntimeContainer
from elephantbroker.schemas.config import ElephantBrokerConfig
from elephantbroker.schemas.tiers import BusinessTier

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def live_client(monkeypatch):
    """Create an async test client backed by real infrastructure.

    R2 integration RED fix (cascade fallout from TODO-3-343 / Bucket A-R2-Test):
    Bucket A-R2-Test removed the global EB_ALLOW_DEFAULT_GATEWAY_ID opt-out
    from tests/conftest.py and scoped it to the unit-side test_container.py
    only. Integration fixtures call RuntimeContainer.from_config() directly
    without that scoping, and the Bucket A startup safety check (R1 `d850186`)
    correctly refuses to boot with empty gateway_id. Set a distinctive value
    here so any cross-test pollution surfaces as a visible mismatch instead of
    a silent collision. Same pattern as the I-R2 fix to
    tests/integration/runtime/working_set/test_working_set_integration.py.
    """
    monkeypatch.setenv("EB_GATEWAY_ID", "test-ws-api-gateway")
    config = ElephantBrokerConfig.load()
    container = await RuntimeContainer.from_config(config, BusinessTier.FULL)
    app = create_app(container)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
    try:
        await container.close()
    except Exception:
        pass


class TestWorkingSetAPI:
    """Working set build + get via the HTTP API."""

    async def test_build_working_set_endpoint(self, live_client):
        """POST /working-set/build returns a valid snapshot."""
        session_id = str(uuid.uuid4())
        body = {
            "session_id": session_id,
            "session_key": "test:api:ws",
            "profile_name": "coding",
            "query": "API integration test query",
        }
        r = await live_client.post("/working-set/build", json=body)
        assert r.status_code == 200
        data = r.json()
        assert "snapshot_id" in data
        assert "items" in data
        assert data["tokens_used"] <= data["token_budget"]

    async def test_get_working_set_after_build(self, live_client):
        """GET /working-set/{session_id} returns the snapshot built earlier."""
        session_id = str(uuid.uuid4())
        build_body = {
            "session_id": session_id,
            "session_key": "test:api:get",
            "profile_name": "coding",
            "query": "cache roundtrip via API",
        }
        r = await live_client.post("/working-set/build", json=build_body)
        assert r.status_code == 200
        snapshot_id = r.json()["snapshot_id"]

        r = await live_client.get(f"/working-set/{session_id}")
        assert r.status_code == 200
        assert r.json()["snapshot_id"] == snapshot_id

    async def test_get_working_set_not_found(self, live_client):
        """GET /working-set/{unknown_id} returns 404."""
        r = await live_client.get(f"/working-set/{uuid.uuid4()}")
        assert r.status_code == 404

    async def test_session_goal_crud_via_api(self, live_client):
        """Full CRUD cycle for session goals through the /goals/session endpoints."""
        session_key = "test:api:goal"
        session_id = str(uuid.uuid4())
        params = {"session_key": session_key, "session_id": session_id}

        # Create
        r = await live_client.post(
            "/goals/session",
            params=params,
            json={"title": "API test goal", "description": "created via API"},
        )
        assert r.status_code == 200
        goal_id = r.json()["id"]

        # Read
        r = await live_client.get("/goals/session", params=params)
        assert r.status_code == 200
        goals = r.json()["goals"]
        assert any(g["id"] == goal_id for g in goals)

        # Update status
        r = await live_client.patch(
            f"/goals/session/{goal_id}",
            params=params,
            json={"status": "completed"},
        )
        assert r.status_code == 200
        assert r.json()["status"] == "completed"

        # Verify update persisted
        r = await live_client.get("/goals/session", params=params)
        assert r.status_code == 200
        updated = [g for g in r.json()["goals"] if g["id"] == goal_id]
        assert len(updated) == 1
        assert updated[0]["status"] == "completed"
