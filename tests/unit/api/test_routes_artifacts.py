"""Tests for artifact routes."""
import uuid
from unittest.mock import AsyncMock

from elephantbroker.schemas.artifact import SessionArtifact


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

    async def test_session_search_increments_searched_count_per_result(self, client, container):
        """TF-06-006 V4 (route side): POST /artifacts/session/search calls
        increment_searched(session_key, session_id, artifact_id) once for
        each returned SessionArtifact. Pins artifacts.py:83-85 fan-out loop."""
        a1 = SessionArtifact(tool_name="psql", content="alpha", summary="postgres timescale compression")
        a2 = SessionArtifact(tool_name="psql", content="bravo", summary="postgres timescale tuning")
        container.session_artifact_store.search = AsyncMock(return_value=[a1, a2])
        container.session_artifact_store.increment_searched = AsyncMock()

        body = {
            "session_key": "agent:main:main",
            "session_id": str(uuid.uuid4()),
            "query": "postgres timescale compression",
        }
        r = await client.post("/artifacts/session/search", json=body)

        assert r.status_code == 200
        payload = r.json()
        assert isinstance(payload, list)
        assert len(payload) == 2
        # increment_searched fired once per returned artifact, in order
        calls = container.session_artifact_store.increment_searched.call_args_list
        assert len(calls) == 2
        ids_incremented = [c.args[2] for c in calls]
        assert str(a1.artifact_id) in ids_incremented
        assert str(a2.artifact_id) in ids_incremented

    async def test_session_search_no_results_no_increments(self, client, container):
        """TF-06-006 V4 negative: empty result set → no increment_searched calls."""
        container.session_artifact_store.search = AsyncMock(return_value=[])
        container.session_artifact_store.increment_searched = AsyncMock()

        body = {
            "session_key": "agent:main:main",
            "session_id": str(uuid.uuid4()),
            "query": "no matches",
        }
        r = await client.post("/artifacts/session/search", json=body)

        assert r.status_code == 200
        assert r.json() == []
        container.session_artifact_store.increment_searched.assert_not_called()
