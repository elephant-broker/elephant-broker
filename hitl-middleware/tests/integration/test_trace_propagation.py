"""Integration tests for trace context / traceparent header handling."""
from __future__ import annotations

from unittest.mock import AsyncMock, patch

import httpx
import pytest

from tests.integration.conftest import make_notify_payload


def _patch_webhook_post(app, *, return_value=None):
    """Patch the WebhookPlugin's httpx client POST on the actual plugin instance."""
    plugin = app.state.registry._plugins[0]
    mock_client = AsyncMock()
    mock_client.post = AsyncMock(return_value=return_value or httpx.Response(200))
    return patch.object(plugin, "_get_client", return_value=mock_client), mock_client


# ---------------------------------------------------------------------------
# 1. test_traceparent_header_accepted
# ---------------------------------------------------------------------------


async def test_traceparent_header_accepted(client, app):
    """POST with a traceparent header does not cause an error."""
    ctx, _ = _patch_webhook_post(app, return_value=httpx.Response(200, json={"ok": True}))

    with ctx:
        resp = await client.post(
            "/intents/notify",
            json=make_notify_payload(),
            headers={"traceparent": "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"},
        )
        assert resp.status_code == 200
        assert resp.json()["dispatched"] is True


# ---------------------------------------------------------------------------
# 2. test_request_without_traceparent
# ---------------------------------------------------------------------------


async def test_request_without_traceparent(client, app):
    """POST without traceparent header still works fine."""
    ctx, _ = _patch_webhook_post(app, return_value=httpx.Response(200, json={"ok": True}))

    with ctx:
        resp = await client.post("/intents/notify", json=make_notify_payload())
        assert resp.status_code == 200
        assert resp.json()["dispatched"] is True
