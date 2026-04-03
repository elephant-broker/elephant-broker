"""Unit tests for router POST /intents/approval — 10 tests."""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock

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


def _approval_payload(**overrides) -> dict:
    data = {
        "request_id": str(uuid.uuid4()),
        "guard_event_id": str(uuid.uuid4()),
        "session_id": str(uuid.uuid4()),
    }
    data.update(overrides)
    return data


class TestApprovalRoute:
    async def test_approval_success_200(self, client):
        """POST /intents/approval with valid payload returns 200."""
        resp = await client.post("/intents/approval", json=_approval_payload())
        assert resp.status_code == 200

    async def test_approval_dispatches_to_registry(self, app, client):
        """Approval intent is dispatched via the registry."""
        mock_registry = AsyncMock()
        mock_registry.dispatch_approval = AsyncMock(return_value=True)
        mock_registry.plugin_count = 1
        app.state.registry = mock_registry

        resp = await client.post("/intents/approval", json=_approval_payload())
        assert resp.status_code == 200
        mock_registry.dispatch_approval.assert_awaited_once()

    async def test_approval_callback_urls_in_payload(self, app, client):
        """Callback URLs are passed through to the registry."""
        mock_registry = AsyncMock()
        mock_registry.dispatch_approval = AsyncMock(return_value=True)
        mock_registry.plugin_count = 1
        app.state.registry = mock_registry

        payload = _approval_payload(
            approve_callback_url="http://hitl/callbacks/approve",
            reject_callback_url="http://hitl/callbacks/reject",
        )
        resp = await client.post("/intents/approval", json=payload)
        assert resp.status_code == 200
        called_intent = mock_registry.dispatch_approval.call_args[0][0]
        assert called_intent.approve_callback_url == "http://hitl/callbacks/approve"

    async def test_approval_missing_fields_422(self, client):
        """Missing required fields returns 422."""
        resp = await client.post("/intents/approval", json={})
        assert resp.status_code == 422

    async def test_approval_invalid_uuid_422(self, client):
        """Invalid UUID for request_id returns 422."""
        resp = await client.post(
            "/intents/approval",
            json={
                "request_id": "not-a-uuid",
                "guard_event_id": str(uuid.uuid4()),
                "session_id": str(uuid.uuid4()),
            },
        )
        assert resp.status_code == 422

    async def test_approval_timeout_seconds_present(self, app, client):
        """timeout_seconds is passed through to the intent."""
        mock_registry = AsyncMock()
        mock_registry.dispatch_approval = AsyncMock(return_value=True)
        mock_registry.plugin_count = 1
        app.state.registry = mock_registry

        payload = _approval_payload(timeout_seconds=120)
        resp = await client.post("/intents/approval", json=payload)
        assert resp.status_code == 200
        called_intent = mock_registry.dispatch_approval.call_args[0][0]
        assert called_intent.timeout_seconds == 120

    async def test_approval_plugin_failure_500(self, app, client):
        """If dispatch raises, returns 500."""
        mock_registry = AsyncMock()
        mock_registry.dispatch_approval = AsyncMock(side_effect=RuntimeError("boom"))
        mock_registry.plugin_count = 1
        app.state.registry = mock_registry

        resp = await client.post("/intents/approval", json=_approval_payload())
        assert resp.status_code == 500

    async def test_approval_matched_rules_list(self, client):
        """matched_rules list is accepted in the payload."""
        payload = _approval_payload(matched_rules=["rule-A", "rule-B"])
        resp = await client.post("/intents/approval", json=payload)
        assert resp.status_code == 200

    async def test_approval_empty_explanation_ok(self, client):
        """Empty explanation is accepted (it's optional)."""
        payload = _approval_payload(explanation="")
        resp = await client.post("/intents/approval", json=payload)
        assert resp.status_code == 200

    async def test_approval_response_includes_request_id(self, client):
        """Response body contains the request_id."""
        rid = str(uuid.uuid4())
        payload = _approval_payload(request_id=rid)
        resp = await client.post("/intents/approval", json=payload)
        body = resp.json()
        assert body["request_id"] == rid
