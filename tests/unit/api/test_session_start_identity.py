"""Tests for session_start agent identity registration."""
import pytest
from unittest.mock import AsyncMock


@pytest.mark.asyncio
async def test_session_start_returns_agent_key(client):
    resp = await client.post("/sessions/start", json={
        "session_key": "agent:main:main",
        "session_id": "test-sid",
        "gateway_id": "gw-test",
        "agent_id": "main",
        "agent_key": "gw-test:main",
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["agent_key"] == "gw-test:main"
    assert data["agent_actor_id"] is not None
    assert data["status"] == "ok"


@pytest.mark.asyncio
async def test_session_start_without_agent_key(client):
    resp = await client.post("/sessions/start", json={
        "session_key": "agent:main:main",
        "session_id": "test-sid",
    })
    assert resp.status_code == 200
    data = resp.json()
    # No agent_id in body or headers → agent_key is empty
    assert data["agent_key"] == ""
    assert data["agent_actor_id"] is None


@pytest.mark.asyncio
async def test_session_start_derives_agent_key_from_parts(client):
    """When agent_key is not provided but gateway_id + agent_id are, it's derived."""
    resp = await client.post("/sessions/start", json={
        "session_key": "agent:main:main",
        "session_id": "test-sid",
        "gateway_id": "gw-prod",
        "agent_id": "main",
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["agent_key"] == "gw-prod:main"


@pytest.mark.asyncio
async def test_session_end_includes_gateway_in_trace(client, container):
    # Track trace events
    events = container.trace_ledger._events
    initial_count = len(events)

    resp = await client.post("/sessions/end", json={
        "session_key": "agent:main:main",
        "session_id": "test-sid",
        "gateway_id": "gw-test",
        "agent_key": "gw-test:main",
    })
    assert resp.status_code == 200

    # Check trace event was emitted with gateway_id
    new_events = events[initial_count:]
    assert any(e.gateway_id for e in new_events)


@pytest.mark.asyncio
async def test_session_start_with_identity_headers(client):
    """Gateway identity from headers should be used when body fields are empty."""
    resp = await client.post(
        "/sessions/start",
        json={"session_key": "agent:main:main", "session_id": "test-sid"},
        headers={
            "X-EB-Gateway-ID": "gw-from-header",
            "X-EB-Agent-ID": "main",
            "X-EB-Agent-Key": "gw-from-header:main",
        },
    )
    assert resp.status_code == 200
    data = resp.json()
    assert data["agent_key"] == "gw-from-header:main"


@pytest.mark.asyncio
async def test_session_start_with_parent_session_key(client, container):
    """Subagent parent mapping should be stored when parent_session_key is provided."""
    container.redis = AsyncMock()
    container.redis.setex = AsyncMock()

    resp = await client.post("/sessions/start", json={
        "session_key": "agent:main:subagent:abc",
        "session_id": "sub-sid",
        "gateway_id": "gw-test",
        "agent_id": "main",
        "agent_key": "gw-test:main",
        "parent_session_key": "agent:main:main",
    })
    assert resp.status_code == 200
    # Redis setex should have been called for parent mapping
    container.redis.setex.assert_called_once()
