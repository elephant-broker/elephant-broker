"""End-to-end tests for full HITL Middleware lifecycle flows."""
from __future__ import annotations

import asyncio
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
# 1. test_notify_lifecycle
# ---------------------------------------------------------------------------


async def test_notify_lifecycle(e2e_client, e2e_app):
    """Full lifecycle: POST /intents/notify -> dispatched -> 200."""
    ctx, _ = _patch_webhook_post(e2e_app, return_value=httpx.Response(200, json={"ok": True}))

    with ctx:
        resp = await e2e_client.post("/intents/notify", json=make_notify_payload())

    assert resp.status_code == 200
    body = resp.json()
    assert body["dispatched"] is True
    assert body["plugin_count"] >= 1


# ---------------------------------------------------------------------------
# 2. test_approval_lifecycle
# ---------------------------------------------------------------------------


async def test_approval_lifecycle(e2e_client, e2e_app):
    """Full lifecycle: POST /intents/approval -> dispatched -> POST /callbacks/approve -> runtime updated."""
    request_id = str(uuid.uuid4())
    mock_runtime_resp = httpx.Response(200, json={"status": "approved"})

    # Step 1: dispatch approval
    ctx, _ = _patch_webhook_post(e2e_app, return_value=httpx.Response(200, json={"ok": True}))
    with ctx:
        resp = await e2e_client.post(
            "/intents/approval",
            json=make_approval_payload(request_id=request_id),
        )
        assert resp.status_code == 200
        assert resp.json()["dispatched"] is True
        assert resp.json()["request_id"] == request_id

    # Step 2: approve callback -> runtime PATCH
    with patch("httpx.AsyncClient.patch", new_callable=AsyncMock, return_value=mock_runtime_resp) as mock_patch:
        resp = await e2e_client.post(
            "/callbacks/approve",
            json={"request_id": request_id, "message": "Ship it", "approved_by": "lead"},
            headers={"X-HITL-Signature": "e2e-sig"},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "approved"

        # Verify runtime was called with correct payload
        mock_patch.assert_awaited_once()
        call_kwargs = mock_patch.call_args
        patch_json = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert patch_json["status"] == "approved"
        assert patch_json["message"] == "Ship it"
        assert patch_json["resolved_by"] == "lead"


# ---------------------------------------------------------------------------
# 3. test_rejection_lifecycle
# ---------------------------------------------------------------------------


async def test_rejection_lifecycle(e2e_client, e2e_app):
    """Full lifecycle: POST /intents/approval -> dispatched -> POST /callbacks/reject -> runtime updated."""
    request_id = str(uuid.uuid4())
    mock_runtime_resp = httpx.Response(200, json={"status": "rejected"})

    # Step 1: dispatch approval
    ctx, _ = _patch_webhook_post(e2e_app, return_value=httpx.Response(200, json={"ok": True}))
    with ctx:
        resp = await e2e_client.post(
            "/intents/approval",
            json=make_approval_payload(request_id=request_id),
        )
        assert resp.status_code == 200

    # Step 2: reject callback -> runtime PATCH
    with patch("httpx.AsyncClient.patch", new_callable=AsyncMock, return_value=mock_runtime_resp) as mock_patch:
        resp = await e2e_client.post(
            "/callbacks/reject",
            json={"request_id": request_id, "reason": "Policy violation", "rejected_by": "security"},
            headers={"X-HITL-Signature": "e2e-sig"},
        )
        assert resp.status_code == 200
        assert resp.json()["status"] == "rejected"

        patch_json = mock_patch.call_args.kwargs.get("json") or mock_patch.call_args[1].get("json")
        assert patch_json["status"] == "rejected"
        assert patch_json["reason"] == "Policy violation"
        assert patch_json["resolved_by"] == "security"


# ---------------------------------------------------------------------------
# 4. test_health_after_notify
# ---------------------------------------------------------------------------


async def test_health_after_notify(e2e_client, e2e_app):
    """After a notify, GET /health shows plugins_registered > 0."""
    ctx, _ = _patch_webhook_post(e2e_app, return_value=httpx.Response(200, json={"ok": True}))

    with ctx:
        await e2e_client.post("/intents/notify", json=make_notify_payload())

    health_resp = await e2e_client.get("/health")
    assert health_resp.status_code == 200
    body = health_resp.json()
    assert body["status"] == "ok"
    assert body["plugins_registered"] >= 1


# ---------------------------------------------------------------------------
# 5. test_concurrent_notify_and_approval
# ---------------------------------------------------------------------------


async def test_concurrent_notify_and_approval(e2e_client, e2e_app):
    """Notify and approval intents fired concurrently both succeed."""
    ctx, _ = _patch_webhook_post(e2e_app, return_value=httpx.Response(200, json={"ok": True}))

    with ctx:
        notify_task = e2e_client.post("/intents/notify", json=make_notify_payload())
        approval_task = e2e_client.post("/intents/approval", json=make_approval_payload())
        notify_resp, approval_resp = await asyncio.gather(notify_task, approval_task)

    assert notify_resp.status_code == 200
    assert notify_resp.json()["dispatched"] is True
    assert approval_resp.status_code == 200
    assert approval_resp.json()["dispatched"] is True


# ---------------------------------------------------------------------------
# 6. test_notify_all_fields
# ---------------------------------------------------------------------------


async def test_notify_all_fields(e2e_client, e2e_app):
    """Every optional field populated -- round-trip works without loss."""
    payload = make_notify_payload(
        session_key="agent:worker:sub1",
        gateway_id="gw-full",
        agent_key="gw-full:worker",
        action_summary="Complete action with all fields",
        decision_domain="data_access",
        outcome="warn",
        matched_rules=["rule-A", "rule-B", "rule-C"],
        explanation="Detailed explanation for the warn decision",
    )

    ctx, mock_client = _patch_webhook_post(e2e_app, return_value=httpx.Response(200, json={"ok": True}))

    with ctx:
        resp = await e2e_client.post("/intents/notify", json=payload)
        assert resp.status_code == 200
        assert resp.json()["dispatched"] is True

        # Verify all fields arrived at the webhook
        call_kwargs = mock_client.post.call_args
        sent_json = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert sent_json["session_key"] == "agent:worker:sub1"
        assert sent_json["gateway_id"] == "gw-full"
        assert sent_json["agent_key"] == "gw-full:worker"
        assert sent_json["action_summary"] == "Complete action with all fields"
        assert sent_json["decision_domain"] == "data_access"
        assert sent_json["outcome"] == "warn"
        assert sent_json["matched_rules"] == ["rule-A", "rule-B", "rule-C"]
        assert sent_json["explanation"] == "Detailed explanation for the warn decision"
