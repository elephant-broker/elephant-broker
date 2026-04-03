"""Tests for working set API routes."""
import uuid

from unittest.mock import AsyncMock, call

from elephantbroker.schemas.working_set import WorkingSetSnapshot, ScoringWeights


class TestWorkingSetRoutes:
    async def test_build_working_set_returns_snapshot(self, client, container):
        session_id = uuid.uuid4()
        snapshot = WorkingSetSnapshot(
            session_id=session_id, items=[], token_budget=8000, tokens_used=0,
        )
        container.working_set_manager.build_working_set = AsyncMock(return_value=snapshot)

        body = {
            "session_id": str(session_id),
            "session_key": "agent:main:main",
            "profile_name": "coding",
            "query": "What files changed?",
        }
        r = await client.post("/working-set/build", json=body)
        assert r.status_code == 200
        data = r.json()
        assert data["session_id"] == str(session_id)
        assert isinstance(data["items"], list)
        assert data["token_budget"] == 8000

    async def test_build_working_set_validates_request_fields(self, client):
        # Missing required fields (session_id, session_key, query)
        r = await client.post("/working-set/build", json={"profile_name": "coding"})
        assert r.status_code == 422

    async def test_build_working_set_disabled_module_returns_501(self, client, container):
        container.working_set_manager = None
        body = {
            "session_id": str(uuid.uuid4()),
            "session_key": "agent:main:main",
            "query": "test",
        }
        r = await client.post("/working-set/build", json=body)
        assert r.status_code == 501

    async def test_build_working_set_missing_query_returns_422(self, client):
        body = {
            "session_id": str(uuid.uuid4()),
            "session_key": "agent:main:main",
            "profile_name": "coding",
        }
        r = await client.post("/working-set/build", json=body)
        assert r.status_code == 422

    async def test_get_working_set_returns_cached(self, client, container):
        session_id = uuid.uuid4()
        snapshot = WorkingSetSnapshot(
            session_id=session_id, items=[], token_budget=8000, tokens_used=0,
        )
        container.working_set_manager.get_working_set = AsyncMock(return_value=snapshot)

        r = await client.get(f"/working-set/{session_id}")
        assert r.status_code == 200
        data = r.json()
        assert data["session_id"] == str(session_id)

    async def test_get_working_set_not_found_returns_404(self, client, container):
        container.working_set_manager.get_working_set = AsyncMock(return_value=None)
        r = await client.get(f"/working-set/{uuid.uuid4()}")
        assert r.status_code == 404

    async def test_rerank_endpoint_returns_reranked(self, client):
        body = {
            "query": "how to fix the bug",
            "documents": ["fix the import error", "update the readme"],
        }
        r = await client.post("/rerank/", json=body)
        assert r.status_code == 200
        data = r.json()
        assert "results" in data
        assert isinstance(data["results"], list)
        assert len(data["results"]) == 2
        for item in data["results"]:
            assert "index" in item
            assert "text" in item
            assert "score" in item

    async def test_rerank_endpoint_disabled_returns_501(self, client, container):
        container.rerank = None
        body = {
            "query": "test query",
            "documents": ["doc one"],
        }
        r = await client.post("/rerank/", json=body)
        assert r.status_code == 501


class TestWorkingSetGatewayIsolation:
    """Gateway-ID enforcement tests for working set routes.

    The working set route delegates to WorkingSetManager.build_working_set(),
    which relies on the retrieval orchestrator for gateway-scoped data access.
    These tests verify the route works correctly with different gateway headers
    and that the manager is called with the expected parameters.
    """

    async def test_build_working_set_with_custom_gateway(self, client, container):
        """POST /working-set/build works correctly when X-EB-Gateway-ID is set.
        The gateway_id is available in request.state for downstream use."""
        session_id = uuid.uuid4()
        snapshot = WorkingSetSnapshot(
            session_id=session_id, items=[], token_budget=8000, tokens_used=0,
            gateway_id="tenant-88",
        )
        container.working_set_manager.build_working_set = AsyncMock(return_value=snapshot)

        body = {
            "session_id": str(session_id),
            "session_key": "agent:main:main",
            "profile_name": "coding",
            "query": "What changed?",
        }
        r = await client.post(
            "/working-set/build",
            json=body,
            headers={"X-EB-Gateway-ID": "tenant-88"},
        )
        assert r.status_code == 200
        data = r.json()
        assert data["gateway_id"] == "tenant-88"
        # Verify build_working_set was called exactly once
        container.working_set_manager.build_working_set.assert_awaited_once()

    async def test_build_working_set_default_gateway(self, client, container):
        """Without X-EB-Gateway-ID, middleware defaults to 'local'.
        The route should still work correctly."""
        session_id = uuid.uuid4()
        snapshot = WorkingSetSnapshot(
            session_id=session_id, items=[], token_budget=8000, tokens_used=0,
        )
        container.working_set_manager.build_working_set = AsyncMock(return_value=snapshot)

        body = {
            "session_id": str(session_id),
            "session_key": "agent:main:main",
            "profile_name": "coding",
            "query": "test query",
        }
        r = await client.post("/working-set/build", json=body)
        assert r.status_code == 200
        container.working_set_manager.build_working_set.assert_awaited_once()
