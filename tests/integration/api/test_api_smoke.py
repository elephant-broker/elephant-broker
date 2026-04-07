"""API smoke tests against real infrastructure."""
from __future__ import annotations

import asyncio

import pytest
import pytest_asyncio
from httpx import ASGITransport, AsyncClient

from elephantbroker.api.app import create_app
from elephantbroker.runtime.container import RuntimeContainer
from elephantbroker.schemas.config import ElephantBrokerConfig
from elephantbroker.schemas.tiers import BusinessTier


@pytest_asyncio.fixture
async def live_client():
    config = ElephantBrokerConfig.load()
    container = await RuntimeContainer.from_config(config, BusinessTier.FULL)
    app = create_app(container)
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
    try:
        await container.close()
    except Exception:
        pass  # Async teardown may fail if event loop is closing


@pytest.mark.integration
class TestAPISmokeTests:
    async def test_health_endpoint(self, live_client):
        r = await live_client.get("/health/")
        assert r.status_code == 200

    async def test_ready_endpoint(self, live_client):
        # Neo4j may accept TCP before auth is fully initialized; retry.
        last_data = None
        for attempt in range(15):
            r = await live_client.get("/health/ready")
            assert r.status_code == 200
            data = r.json()
            last_data = data
            assert "ready" in data, f"Response: {data}"
            assert "checks" in data, f"Response: {data}"
            # Check if all required backends are healthy
            neo4j_ok = data.get("checks", {}).get("neo4j", {}).get("status") == "ok"
            qdrant_ok = data.get("checks", {}).get("qdrant", {}).get("status") == "ok"
            trace_ok = data.get("checks", {}).get("trace_ledger", {}).get("status") == "ok"
            if neo4j_ok and qdrant_ok and trace_ok:
                return
            await asyncio.sleep(1)
        # Log which checks failed on final attempt
        for name, check in last_data.get("checks", {}).items():
            if check.get("status") != "ok":
                print(f"  /ready check '{name}': {check}")
        assert last_data["checks"]["trace_ledger"]["status"] == "ok", f"trace_ledger: {last_data['checks']['trace_ledger']}"
        assert last_data["checks"]["neo4j"]["status"] == "ok", f"neo4j: {last_data['checks']['neo4j']}"
        assert last_data["checks"]["qdrant"]["status"] == "ok", f"qdrant: {last_data['checks']['qdrant']}"

    async def test_full_actor_flow(self, live_client):
        # Create
        body = {"type": "worker_agent", "display_name": "smoke-test-bot"}
        r = await live_client.post("/actors/", json=body)
        assert r.status_code == 200
        actor_id = r.json()["id"]

        # Get
        r = await live_client.get(f"/actors/{actor_id}")
        assert r.status_code == 200

        # Relationships
        r = await live_client.get(f"/actors/{actor_id}/relationships")
        assert r.status_code == 200

    async def test_full_goal_flow(self, live_client):
        r = await live_client.post("/goals/", json={"title": "Smoke test goal"})
        assert r.status_code == 200
        goal_id = r.json()["id"]

        r = await live_client.put(f"/goals/{goal_id}", json={"status": "completed"})
        assert r.status_code == 200

    async def test_profile_resolution_via_api(self, live_client):
        r = await live_client.get("/profiles/coding/resolve")
        assert r.status_code == 200
        data = r.json()
        assert data["weights"]["turn_relevance"] == 1.5

    async def test_full_memory_flow(self, live_client):
        # Store
        r = await live_client.post("/memory/store", json={"fact": {"text": "Smoke test fact", "category": "general"}})
        assert r.status_code == 200

        # Search
        r = await live_client.post("/memory/search", json={"query": "smoke test"})
        assert r.status_code == 200
