"""Integration tests for the notification intent flow."""
from __future__ import annotations

import asyncio
import uuid
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from tests.integration.conftest import make_notify_payload


def _patch_webhook_post(app, *, return_value=None, side_effect=None):
    """Return a context-manager that patches the WebhookPlugin's httpx client POST.

    Works by replacing _get_client on the actual plugin instance held by the app
    registry so that the lazy-created httpx.AsyncClient is a mock.
    """
    plugin = app.state.registry._plugins[0]
    mock_client = AsyncMock()
    if side_effect is not None:
        mock_client.post = AsyncMock(side_effect=side_effect)
    else:
        mock_client.post = AsyncMock(return_value=return_value or httpx.Response(200))
    return patch.object(plugin, "_get_client", return_value=mock_client), mock_client


# ---------------------------------------------------------------------------
# 1. test_notify_dispatches_through_registry
# ---------------------------------------------------------------------------


async def test_notify_dispatches_through_registry(client, app):
    """POST /intents/notify dispatches through the plugin registry."""
    registry = app.state.registry
    assert registry.plugin_count == 1, "WebhookPlugin should be auto-registered"

    with patch.object(
        registry._plugins[0], "send_notification", new_callable=AsyncMock, return_value=True
    ) as mock_send:
        resp = await client.post("/intents/notify", json=make_notify_payload())
        assert resp.status_code == 200
        body = resp.json()
        assert body["dispatched"] is True
        assert body["plugin_count"] == 1
        mock_send.assert_awaited_once()


# ---------------------------------------------------------------------------
# 2. test_notify_with_real_webhook_plugin
# ---------------------------------------------------------------------------


async def test_notify_with_real_webhook_plugin(client, app):
    """WebhookPlugin sends an actual POST (mocked at httpx client level)."""
    ctx, mock_client = _patch_webhook_post(app, return_value=httpx.Response(200, json={"ok": True}))

    with ctx:
        resp = await client.post("/intents/notify", json=make_notify_payload())
        assert resp.status_code == 200
        assert resp.json()["dispatched"] is True
        mock_client.post.assert_awaited_once()


# ---------------------------------------------------------------------------
# 3. test_notify_multiple_webhooks
# ---------------------------------------------------------------------------


async def test_notify_multiple_webhooks(client, app):
    """Two notification endpoints both receive a POST."""
    from hitl_middleware.config import WebhookConfig, WebhookEndpoint
    from hitl_middleware.plugins.webhook.plugin import WebhookPlugin

    extra_config = WebhookConfig(
        notification_endpoints=[WebhookEndpoint(url="http://mock-webhook:9999/notify2")],
    )
    extra_plugin = WebhookPlugin(config=extra_config)
    app.state.registry.register(extra_plugin)

    mock_resp = httpx.Response(200, json={"ok": True})
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=mock_resp)

    # Patch _get_client on BOTH plugins
    plugin_0 = app.state.registry._plugins[0]
    plugin_1 = app.state.registry._plugins[1]
    with (
        patch.object(plugin_0, "_get_client", return_value=mock_client),
        patch.object(plugin_1, "_get_client", return_value=mock_client),
    ):
        resp = await client.post("/intents/notify", json=make_notify_payload())
        assert resp.status_code == 200
        assert resp.json()["dispatched"] is True
        # Two plugins, each with 1 endpoint = 2 POST calls
        assert mock_client.post.await_count == 2


# ---------------------------------------------------------------------------
# 4. test_notify_webhook_failure_logged
# ---------------------------------------------------------------------------


async def test_notify_webhook_failure_logged(client, app):
    """Webhook returning 500 is logged but /intents/notify still returns 200."""
    ctx, _ = _patch_webhook_post(app, return_value=httpx.Response(500, text="Internal Server Error"))

    with ctx:
        resp = await client.post("/intents/notify", json=make_notify_payload())
        # Fire-and-forget: route always returns 200
        assert resp.status_code == 200
        # Plugin reports failure, but the route does not raise
        body = resp.json()
        assert body["dispatched"] is False


# ---------------------------------------------------------------------------
# 5. test_notify_webhook_connection_refused
# ---------------------------------------------------------------------------


async def test_notify_webhook_connection_refused(client, app):
    """Webhook endpoint unreachable is handled gracefully."""
    ctx, _ = _patch_webhook_post(app, side_effect=httpx.ConnectError("Connection refused"))

    with ctx:
        resp = await client.post("/intents/notify", json=make_notify_payload())
        assert resp.status_code == 200
        assert resp.json()["dispatched"] is False


# ---------------------------------------------------------------------------
# 6. test_notify_large_payload
# ---------------------------------------------------------------------------


async def test_notify_large_payload(client, app):
    """A 10KB action_summary is accepted without truncation."""
    large_summary = "x" * 10_000
    ctx, mock_client = _patch_webhook_post(app, return_value=httpx.Response(200, json={"ok": True}))

    with ctx:
        payload = make_notify_payload(action_summary=large_summary)
        resp = await client.post("/intents/notify", json=payload)
        assert resp.status_code == 200
        assert resp.json()["dispatched"] is True
        # Verify the full summary was forwarded
        call_kwargs = mock_client.post.call_args
        sent_json = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        assert sent_json["action_summary"] == large_summary


# ---------------------------------------------------------------------------
# 7. test_notify_concurrent_requests
# ---------------------------------------------------------------------------


async def test_notify_concurrent_requests(client, app):
    """5 concurrent notify calls all succeed."""
    ctx, _ = _patch_webhook_post(app, return_value=httpx.Response(200, json={"ok": True}))

    with ctx:
        payloads = [make_notify_payload() for _ in range(5)]
        tasks = [client.post("/intents/notify", json=p) for p in payloads]
        responses = await asyncio.gather(*tasks)
        for r in responses:
            assert r.status_code == 200
            assert r.json()["dispatched"] is True


# ---------------------------------------------------------------------------
# 8. test_notify_empty_matched_rules
# ---------------------------------------------------------------------------


async def test_notify_empty_matched_rules(client, app):
    """Empty matched_rules list is accepted."""
    ctx, _ = _patch_webhook_post(app, return_value=httpx.Response(200, json={"ok": True}))

    with ctx:
        payload = make_notify_payload(matched_rules=[])
        resp = await client.post("/intents/notify", json=payload)
        assert resp.status_code == 200
        assert resp.json()["dispatched"] is True
