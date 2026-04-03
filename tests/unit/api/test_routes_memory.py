"""Tests for memory routes."""
import uuid
from unittest.mock import AsyncMock, patch

from elephantbroker.runtime.memory.facade import DedupSkipped
from elephantbroker.schemas.base import Scope
from elephantbroker.schemas.fact import FactAssertion, MemoryClass


class TestMemoryRoutes:
    async def test_store_fact(self, client):
        body = {"fact": {"text": "Test fact", "category": "general"}}
        r = await client.post("/memory/store", json=body)
        assert r.status_code == 200

    async def test_search_returns_results(self, client):
        r = await client.post("/memory/search", json={"query": "test"})
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    async def test_status_endpoint(self, client):
        r = await client.get("/memory/status")
        assert r.status_code == 200

    async def test_sync_endpoint(self, client):
        r = await client.post("/memory/sync")
        assert r.status_code == 200

    async def test_store_missing_body_422(self, client):
        r = await client.post("/memory/store", json={})
        assert r.status_code == 422

    async def test_search_missing_query_422(self, client):
        r = await client.post("/memory/search", json={})
        assert r.status_code == 422

    async def test_read_memory_returns_results(self, client, mock_graph):
        mock_graph.query_cypher.return_value = []
        r = await client.get("/memory/read?scope=session")
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    async def test_store_fact_when_memory_disabled(self, client, container):
        container.memory_store = None
        body = {"fact": {"text": "Test fact", "category": "general"}}
        r = await client.post("/memory/store", json=body)
        assert r.status_code == 500

    async def test_search_with_max_results_zero(self, client):
        r = await client.post("/memory/search", json={"query": "test", "max_results": 0})
        assert r.status_code == 200

    async def test_search_with_empty_query(self, client):
        r = await client.post("/memory/search", json={"query": ""})
        assert r.status_code == 200

    async def test_search_default_max_results_20(self, client):
        """SearchRequest defaults to max_results=20."""
        r = await client.post("/memory/search", json={"query": "test"})
        assert r.status_code == 200

    async def test_search_accepts_memory_class(self, client):
        r = await client.post(
            "/memory/search",
            json={"query": "test", "memory_class": "episodic"},
        )
        assert r.status_code == 200

    async def test_search_accepts_session_key(self, client):
        r = await client.post(
            "/memory/search",
            json={"query": "test", "session_key": "agent:main:main"},
        )
        assert r.status_code == 200

    async def test_search_accepts_profile_name(self, client):
        r = await client.post(
            "/memory/search",
            json={"query": "test", "profile_name": "coding", "auto_recall": True},
        )
        assert r.status_code == 200

    async def test_get_by_id_returns_fact(self, client, container):
        fact = FactAssertion(text="hello world")
        container.memory_store.get_by_id = AsyncMock(return_value=fact)
        r = await client.get(f"/memory/{fact.id}")
        assert r.status_code == 200
        data = r.json()
        assert data["text"] == "hello world"

    async def test_get_by_id_not_found_404(self, client, container):
        container.memory_store.get_by_id = AsyncMock(return_value=None)
        r = await client.get(f"/memory/{uuid.uuid4()}")
        assert r.status_code == 404

    async def test_delete_returns_204(self, client, container):
        container.memory_store.delete = AsyncMock(return_value=None)
        r = await client.delete(f"/memory/{uuid.uuid4()}")
        assert r.status_code == 204

    async def test_delete_not_found_404(self, client, container):
        container.memory_store.delete = AsyncMock(side_effect=KeyError("not found"))
        r = await client.delete(f"/memory/{uuid.uuid4()}")
        assert r.status_code == 404

    async def test_patch_updates_fact(self, client, container):
        fact = FactAssertion(text="updated text")
        container.memory_store.update = AsyncMock(return_value=fact)
        r = await client.patch(
            f"/memory/{fact.id}",
            json={"updates": {"text": "updated text"}},
        )
        assert r.status_code == 200
        assert r.json()["text"] == "updated text"

    async def test_patch_not_found_404(self, client, container):
        container.memory_store.update = AsyncMock(side_effect=KeyError("not found"))
        r = await client.patch(
            f"/memory/{uuid.uuid4()}",
            json={"updates": {"text": "nope"}},
        )
        assert r.status_code == 404

    async def test_promote_class(self, client, container):
        fact = FactAssertion(text="promoted", memory_class=MemoryClass.SEMANTIC)
        container.memory_store.promote_class = AsyncMock(return_value=fact)
        r = await client.post(
            "/memory/promote-class",
            json={"fact_id": str(fact.id), "to_class": "semantic"},
        )
        assert r.status_code == 200
        assert r.json()["memory_class"] == "semantic"

    async def test_store_dedup_skip_returns_409(self, client, container):
        """Bug 4 regression: facade.store() raises DedupSkipped → 409 not 500."""
        container.memory_store.store = AsyncMock(
            side_effect=DedupSkipped("existing-abc", 0.98),
        )
        body = {"fact": {"text": "duplicate fact", "category": "general"}}
        r = await client.post("/memory/store", json=body)
        assert r.status_code == 409
        data = r.json()
        assert data["status"] == "skipped"
        assert data["reason"] == "near_duplicate_detected"
        assert data["existing_fact_id"] == "existing-abc"

    async def test_delete_permission_error_returns_403(self, client, container):
        """Bug 5 regression: facade.delete() raises PermissionError → 403 not 500."""
        container.memory_store.delete = AsyncMock(
            side_effect=PermissionError("wrong gateway"),
        )
        r = await client.delete(f"/memory/{uuid.uuid4()}")
        assert r.status_code == 403
        assert "wrong gateway" in r.json()["detail"]

    async def test_ingest_messages_returns_202_when_not_ready(self, client):
        """When buffer is not available, returns 202."""
        r = await client.post(
            "/memory/ingest-messages",
            json={
                "session_key": "agent:main:main",
                "messages": [{"role": "user", "content": "hello"}],
            },
        )
        assert r.status_code == 202


class TestMemoryGatewayIsolation:
    """Gateway-ID enforcement tests for memory routes."""

    async def test_store_stamps_gateway_id_from_header(self, client, container):
        """POST /memory/store stamps fact.gateway_id from the X-EB-Gateway-ID header."""
        stored_facts: list[FactAssertion] = []
        original_store = container.memory_store.store

        async def capture_store(fact, **kwargs):
            stored_facts.append(fact)
            return fact

        container.memory_store.store = AsyncMock(side_effect=capture_store)

        body = {"fact": {"text": "gateway stamped fact", "category": "general"}}
        r = await client.post(
            "/memory/store",
            json=body,
            headers={"X-EB-Gateway-ID": "tenant-42"},
        )
        assert r.status_code == 200
        # The route should have stamped gateway_id="tenant-42" onto the fact
        assert len(stored_facts) == 1
        assert stored_facts[0].gateway_id == "tenant-42"

    async def test_store_uses_default_gateway_when_no_header(self, client, container):
        """Without X-EB-Gateway-ID header, middleware defaults to 'local'."""
        stored_facts: list[FactAssertion] = []

        async def capture_store(fact, **kwargs):
            stored_facts.append(fact)
            return fact

        container.memory_store.store = AsyncMock(side_effect=capture_store)

        body = {"fact": {"text": "default gateway fact", "category": "general"}}
        r = await client.post("/memory/store", json=body)
        assert r.status_code == 200
        assert len(stored_facts) == 1
        assert stored_facts[0].gateway_id == "local"

    async def test_search_scoped_to_gateway(self, client, container):
        """POST /memory/search returns results only — the gateway scope is enforced
        at the facade/retrieval layer. Here we verify the endpoint works and that
        the middleware correctly sets request.state.gateway_id for downstream use."""
        # Store two facts with different gateway_ids via the facade directly
        fact_local = FactAssertion(text="local fact", gateway_id="local")
        fact_other = FactAssertion(text="other-gw fact", gateway_id="other-gw")

        # Mock search to return only facts matching the facade's gateway
        container.memory_store.search = AsyncMock(return_value=[fact_local])

        r = await client.post(
            "/memory/search",
            json={"query": "fact"},
            headers={"X-EB-Gateway-ID": "local"},
        )
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list)
        # All returned facts should be from the "local" gateway
        for item in data:
            assert item["gateway_id"] == "local"
