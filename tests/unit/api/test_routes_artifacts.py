"""Tests for artifact routes."""
import uuid


class TestArtifactRoutes:
    async def test_store_artifact(self, client):
        body = {"tool_name": "test-tool", "content": "result data"}
        r = await client.post("/artifacts/", json=body)
        assert r.status_code == 200

    async def test_get_artifact(self, client):
        r = await client.get(f"/artifacts/{uuid.uuid4()}")
        assert r.status_code == 200

    async def test_search_artifacts(self, client):
        r = await client.post("/artifacts/search", json={"query": "test"})
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    async def test_store_artifact_missing_body_422(self, client):
        r = await client.post("/artifacts/", json={})
        assert r.status_code == 422

    async def test_search_artifacts_missing_query_422(self, client):
        r = await client.post("/artifacts/search", json={})
        assert r.status_code == 422

    async def test_store_artifact_when_artifacts_disabled(self, client, container):
        container.artifact_store = None
        body = {"tool_name": "test", "content": "data"}
        r = await client.post("/artifacts/", json=body)
        assert r.status_code == 500

    async def test_search_with_max_results_zero(self, client):
        r = await client.post("/artifacts/search", json={"query": "test", "max_results": 0})
        assert r.status_code == 200
