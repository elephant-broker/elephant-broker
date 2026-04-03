"""Integration tests for the approval intent and callback flows."""
from __future__ import annotations

import asyncio
import uuid
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from tests.integration.conftest import make_approval_payload


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
# 1. test_approval_dispatches_through_registry
# ---------------------------------------------------------------------------


async def test_approval_dispatches_through_registry(client, app):
    """POST /intents/approval dispatches through the plugin registry."""
    registry = app.state.registry
    with patch.object(
        registry._plugins[0], "send_approval_request", new_callable=AsyncMock, return_value=True
    ) as mock_send:
        resp = await client.post("/intents/approval", json=make_approval_payload())
        assert resp.status_code == 200
        body = resp.json()
        assert body["dispatched"] is True
        mock_send.assert_awaited_once()


# ---------------------------------------------------------------------------
# 2. test_approval_with_callback_urls
# ---------------------------------------------------------------------------


async def test_approval_with_callback_urls(client, app):
    """Callback URLs are included in the dispatched payload."""
    ctx, mock_client = _patch_webhook_post(app, return_value=httpx.Response(200, json={"ok": True}))

    with ctx:
        payload = make_approval_payload(
            approve_callback_url="http://test/callbacks/approve?token=abc",
            reject_callback_url="http://test/callbacks/reject?token=def",
        )
        resp = await client.post("/intents/approval", json=payload)
        assert resp.status_code == 200
        call_kwargs = mock_client.post.call_args
        sent_json = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert sent_json["approve_callback_url"] == "http://test/callbacks/approve?token=abc"
        assert sent_json["reject_callback_url"] == "http://test/callbacks/reject?token=def"


# ---------------------------------------------------------------------------
# 3. test_approval_approve_callback_flow
# ---------------------------------------------------------------------------


async def test_approval_approve_callback_flow(client, app, config):
    """Full approval flow: POST /intents/approval then POST /callbacks/approve."""
    request_id = str(uuid.uuid4())

    # Step 1: dispatch the approval intent
    ctx, _ = _patch_webhook_post(app, return_value=httpx.Response(200, json={"ok": True}))
    with ctx:
        resp = await client.post("/intents/approval", json=make_approval_payload(request_id=request_id))
        assert resp.status_code == 200

    # Step 2: approve callback -- mock the runtime PATCH
    mock_runtime_resp = httpx.Response(200, json={"status": "approved"})
    with patch("httpx.AsyncClient.patch", new_callable=AsyncMock, return_value=mock_runtime_resp):
        resp = await client.post(
            "/callbacks/approve",
            json={"request_id": request_id, "message": "Looks good", "approved_by": "alice"},
            headers={"X-HITL-Signature": "valid-sig"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "approved"
        assert body["request_id"] == request_id


# ---------------------------------------------------------------------------
# 4. test_approval_reject_callback_flow
# ---------------------------------------------------------------------------


async def test_approval_reject_callback_flow(client, app, config):
    """Full rejection flow: POST /intents/approval then POST /callbacks/reject."""
    request_id = str(uuid.uuid4())

    # Step 1: dispatch the approval intent
    ctx, _ = _patch_webhook_post(app, return_value=httpx.Response(200, json={"ok": True}))
    with ctx:
        resp = await client.post("/intents/approval", json=make_approval_payload(request_id=request_id))
        assert resp.status_code == 200

    # Step 2: reject callback
    mock_runtime_resp = httpx.Response(200, json={"status": "rejected"})
    with patch("httpx.AsyncClient.patch", new_callable=AsyncMock, return_value=mock_runtime_resp):
        resp = await client.post(
            "/callbacks/reject",
            json={"request_id": request_id, "reason": "Too risky", "rejected_by": "bob"},
            headers={"X-HITL-Signature": "valid-sig"},
        )
        assert resp.status_code == 200
        body = resp.json()
        assert body["status"] == "rejected"
        assert body["request_id"] == request_id


# ---------------------------------------------------------------------------
# 5. test_approval_timeout_seconds_preserved
# ---------------------------------------------------------------------------


async def test_approval_timeout_seconds_preserved(client, app):
    """timeout_seconds value flows through to the dispatched payload."""
    ctx, mock_client = _patch_webhook_post(app, return_value=httpx.Response(200, json={"ok": True}))

    with ctx:
        payload = make_approval_payload(timeout_seconds=600)
        resp = await client.post("/intents/approval", json=payload)
        assert resp.status_code == 200
        call_kwargs = mock_client.post.call_args
        sent_json = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert sent_json["timeout_seconds"] == 600


# ---------------------------------------------------------------------------
# 6. test_approval_duplicate_idempotent
# ---------------------------------------------------------------------------


async def test_approval_duplicate_idempotent(client, app):
    """Same request_id dispatched twice -- both succeed (stateless middleware)."""
    request_id = str(uuid.uuid4())
    ctx, _ = _patch_webhook_post(app, return_value=httpx.Response(200, json={"ok": True}))

    with ctx:
        payload = make_approval_payload(request_id=request_id)
        r1 = await client.post("/intents/approval", json=payload)
        r2 = await client.post("/intents/approval", json=payload)
        assert r1.status_code == 200
        assert r2.status_code == 200
        assert r1.json()["request_id"] == request_id
        assert r2.json()["request_id"] == request_id


# ---------------------------------------------------------------------------
# 7. test_approval_with_gateway_id
# ---------------------------------------------------------------------------


async def test_approval_with_gateway_id(client, app):
    """gateway_id flows through the approval dispatch."""
    ctx, mock_client = _patch_webhook_post(app, return_value=httpx.Response(200, json={"ok": True}))

    with ctx:
        payload = make_approval_payload(gateway_id="gw-prod-42")
        resp = await client.post("/intents/approval", json=payload)
        assert resp.status_code == 200
        call_kwargs = mock_client.post.call_args
        sent_json = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert sent_json["gateway_id"] == "gw-prod-42"


# ---------------------------------------------------------------------------
# 8. test_approval_concurrent_approve_reject
# ---------------------------------------------------------------------------


async def test_approval_concurrent_approve_reject(client, app):
    """Approve and reject callbacks sent concurrently -- both return a result."""
    request_id = str(uuid.uuid4())

    # Dispatch the approval intent first
    ctx, _ = _patch_webhook_post(app, return_value=httpx.Response(200, json={"ok": True}))
    with ctx:
        await client.post("/intents/approval", json=make_approval_payload(request_id=request_id))

    # Fire approve and reject concurrently
    mock_runtime_resp = httpx.Response(200, json={"status": "ok"})
    with patch("httpx.AsyncClient.patch", new_callable=AsyncMock, return_value=mock_runtime_resp):
        approve_task = client.post(
            "/callbacks/approve",
            json={"request_id": request_id, "message": "Yes", "approved_by": "alice"},
            headers={"X-HITL-Signature": "sig-a"},
        )
        reject_task = client.post(
            "/callbacks/reject",
            json={"request_id": request_id, "reason": "No", "rejected_by": "bob"},
            headers={"X-HITL-Signature": "sig-b"},
        )
        approve_resp, reject_resp = await asyncio.gather(approve_task, reject_task)

        # Both should succeed at the HTTP level (middleware is stateless)
        assert approve_resp.status_code == 200
        assert reject_resp.status_code == 200
