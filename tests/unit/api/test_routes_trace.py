"""Tests for trace routes."""
import uuid


class TestTraceRoutes:
    async def test_list_traces(self, client):
        r = await client.get("/trace/")
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    async def test_get_trace_event_not_found(self, client):
        r = await client.get(f"/trace/{uuid.uuid4()}")
        assert r.status_code == 404

    async def test_list_traces_with_session_filter(self, client):
        r = await client.get(f"/trace/?session_id={uuid.uuid4()}")
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    async def test_get_nonexistent_trace_returns_404(self, client):
        r = await client.get(f"/trace/{uuid.uuid4()}")
        assert r.status_code == 404
