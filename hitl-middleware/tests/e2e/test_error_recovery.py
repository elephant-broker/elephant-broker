"""End-to-end tests for error recovery and edge-case handling."""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from tests.e2e.conftest import make_approval_payload, make_notify_payload


def _patch_webhook_post(app, *, return_value=None, side_effect=None):
    """Patch the WebhookPlugin's httpx client POST on the actual plugin instance."""
    plugin = app.state.registry._plugins[0]
    mock_client = AsyncMock()
    if side_effect is not None:
        mock_client.post = AsyncMock(side_effect=side_effect)
    else:
        mock_client.post = AsyncMock(return_value=return_value or httpx.Response(200))
    return patch.object(plugin, "_get_client", return_value=mock_client), mock_client


# ---------------------------------------------------------------------------
# 1. test_webhook_down_still_returns_200
# ---------------------------------------------------------------------------


async def test_webhook_down_still_returns_200(e2e_client, e2e_app):
    """When the webhook endpoint is unreachable, /intents/notify still returns 200."""
    ctx, _ = _patch_webhook_post(e2e_app, side_effect=httpx.ConnectError("Connection refused"))

    with ctx:
        resp = await e2e_client.post("/intents/notify", json=make_notify_payload())

    assert resp.status_code == 200
    body = resp.json()
    # dispatch failed, but HTTP layer is still 200 (fire-and-forget)
    assert body["dispatched"] is False
    assert body["plugin_count"] >= 1


# ---------------------------------------------------------------------------
# 2. test_runtime_down_callback_returns_502
# ---------------------------------------------------------------------------


async def test_runtime_down_callback_returns_502(e2e_client):
    """When the runtime is unreachable, /callbacks/approve returns 502."""
    request_id = str(uuid.uuid4())

    with patch(
        "httpx.AsyncClient.patch",
        new_callable=AsyncMock,
        side_effect=httpx.ConnectError("Connection refused"),
    ):
        resp = await e2e_client.post(
            "/callbacks/approve",
            json={"request_id": request_id, "message": "ok", "approved_by": "alice"},
            headers={"X-HITL-Signature": "some-sig"},
        )

    assert resp.status_code == 502
    assert "unreachable" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# 3. test_missing_hmac_returns_403
# ---------------------------------------------------------------------------


async def test_missing_hmac_returns_403(e2e_client):
    """/callbacks/approve without X-HITL-Signature header returns 403."""
    request_id = str(uuid.uuid4())

    resp = await e2e_client.post(
        "/callbacks/approve",
        json={"request_id": request_id, "message": "approved", "approved_by": "alice"},
        # No X-HITL-Signature header
    )

    assert resp.status_code == 403
    assert "signature" in resp.json()["detail"].lower() or "missing" in resp.json()["detail"].lower()


# ---------------------------------------------------------------------------
# 4. test_middleware_stateless
# ---------------------------------------------------------------------------


async def test_middleware_stateless(e2e_client, e2e_app):
    """Two sequential requests have no state leakage between them."""
    ctx, _ = _patch_webhook_post(e2e_app, return_value=httpx.Response(200, json={"ok": True}))

    with ctx:
        # Request 1: notify with unique fields
        payload_1 = make_notify_payload(
            action_summary="First request",
            gateway_id="gw-first",
        )
        resp_1 = await e2e_client.post("/intents/notify", json=payload_1)
        assert resp_1.status_code == 200

        # Request 2: notify with different fields
        payload_2 = make_notify_payload(
            action_summary="Second request",
            gateway_id="gw-second",
        )
        resp_2 = await e2e_client.post("/intents/notify", json=payload_2)
        assert resp_2.status_code == 200

    # Both succeed independently -- no cross-contamination
    assert resp_1.json()["dispatched"] is True
    assert resp_2.json()["dispatched"] is True
