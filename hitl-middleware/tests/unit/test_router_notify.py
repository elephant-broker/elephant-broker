"""Unit tests for router POST /intents/notify — 10 tests."""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, patch

import pytest
from httpx import ASGITransport, AsyncClient

from hitl_middleware.app import create_app
from hitl_middleware.config import HitlMiddlewareConfig


@pytest.fixture
def config():
    return HitlMiddlewareConfig(callback_secret="test-secret")


@pytest.fixture
def app(config):
    return create_app(config)


@pytest.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


def _notification_payload(**overrides) -> dict:
    data = {
        "guard_event_id": str(uuid.uuid4()),
        "session_id": str(uuid.uuid4()),
    }
    data.update(overrides)
    return data


class TestNotifyRoute:
    async def test_notify_success_200(self, client):
        """POST /intents/notify with valid payload returns 200."""
        resp = await client.post("/intents/notify", json=_notification_payload())
        assert resp.status_code == 200

    async def test_notify_dispatches_to_registry(self, app, client):
        """Notification intent is dispatched via the registry."""
        mock_registry = AsyncMock()
        mock_registry.dispatch_notification = AsyncMock(return_value=True)
        mock_registry.plugin_count = 1
        app.state.registry = mock_registry

        resp = await client.post("/intents/notify", json=_notification_payload())
        assert resp.status_code == 200
        mock_registry.dispatch_notification.assert_awaited_once()

    async def test_notify_missing_required_fields_422(self, client):
        """Missing required fields returns 422."""
        resp = await client.post("/intents/notify", json={})
        assert resp.status_code == 422

    async def test_notify_invalid_uuid_422(self, client):
        """Invalid UUID for guard_event_id returns 422."""
        resp = await client.post(
            "/intents/notify",
            json={"guard_event_id": "not-a-uuid", "session_id": str(uuid.uuid4())},
        )
        assert resp.status_code == 422

    async def test_notify_empty_action_summary_accepted(self, client):
        """Empty action_summary is accepted (it's optional)."""
        resp = await client.post(
            "/intents/notify",
            json=_notification_payload(action_summary=""),
        )
        assert resp.status_code == 200

    async def test_notify_no_plugins_registered(self, client):
        """With no plugins, dispatch returns dispatched=False."""
        resp = await client.post("/intents/notify", json=_notification_payload())
        body = resp.json()
        assert body["dispatched"] is False

    async def test_notify_plugin_failure_500(self, app, client):
        """If dispatch raises, returns 500."""
        mock_registry = AsyncMock()
        mock_registry.dispatch_notification = AsyncMock(side_effect=RuntimeError("boom"))
        mock_registry.plugin_count = 1
        app.state.registry = mock_registry

        resp = await client.post("/intents/notify", json=_notification_payload())
        assert resp.status_code == 500

    async def test_notify_all_optional_fields(self, client):
        """Notification with all optional fields set is accepted."""
        payload = _notification_payload(
            session_key="agent:main:main",
            gateway_id="gw-1",
            agent_key="gw-1:agent-1",
            action_summary="Doing something",
            decision_domain="security",
            outcome="warn",
            matched_rules=["rule-A"],
            explanation="Triggered because X",
        )
        resp = await client.post("/intents/notify", json=payload)
        assert resp.status_code == 200

    async def test_notify_response_format(self, client):
        """Response body contains dispatched and plugin_count keys."""
        resp = await client.post("/intents/notify", json=_notification_payload())
        body = resp.json()
        assert "dispatched" in body
        assert "plugin_count" in body

    async def test_notify_with_matched_rules_list(self, client):
        """matched_rules list is accepted in the payload."""
        payload = _notification_payload(matched_rules=["rule-1", "rule-2", "rule-3"])
        resp = await client.post("/intents/notify", json=payload)
        assert resp.status_code == 200
