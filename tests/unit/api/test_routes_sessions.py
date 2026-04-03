"""Tests for session lifecycle routes."""
from unittest.mock import AsyncMock

from elephantbroker.schemas.config import ElephantBrokerConfig


class TestSessionRoutes:
    async def test_session_start_returns_ok(self, client):
        r = await client.post(
            "/sessions/start",
            json={"session_key": "agent:main:main", "session_id": "abc-123"},
        )
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "ok"
        assert data["session_key"] == "agent:main:main"
        assert data["session_id"] == "abc-123"

    async def test_session_end_returns_summary(self, client):
        r = await client.post(
            "/sessions/end",
            json={"session_key": "agent:main:main", "session_id": "abc-123"},
        )
        assert r.status_code == 200
        data = r.json()
        assert data["session_key"] == "agent:main:main"
        assert data["facts_count"] == 0
        assert data["goals_flushed"] == 0
        assert data["messages_flushed"] == 0
        assert "trace_event_id" in data
        assert data["trace_event_id"] is not None

    async def test_session_start_with_parent(self, client):
        r = await client.post(
            "/sessions/start",
            json={
                "session_key": "agent:worker:task1",
                "session_id": "def-456",
                "parent_session_key": "agent:main:main",
            },
        )
        assert r.status_code == 200
        data = r.json()
        assert data["session_key"] == "agent:worker:task1"

    async def test_session_start_parent_uses_config_ttl(self, client, container):
        """BUG-5: parent TTL must come from config, not hardcoded 86400."""
        redis_mock = AsyncMock()
        container.redis = redis_mock
        container.config = ElephantBrokerConfig(consolidation_min_retention_seconds=259200)

        r = await client.post("/sessions/start", json={
            "session_key": "agent:child:main",
            "session_id": "sid-123",
            "parent_session_key": "agent:parent:main",
        })
        assert r.status_code == 200
        redis_mock.setex.assert_called()
        ttl_arg = redis_mock.setex.call_args[0][1]
        assert ttl_arg == 259200  # NOT 86400
