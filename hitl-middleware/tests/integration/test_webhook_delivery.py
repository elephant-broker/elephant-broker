"""Integration tests for webhook delivery mechanics."""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, patch

import httpx
import pytest

from hitl_middleware.app import create_app
from hitl_middleware.config import HitlMiddlewareConfig, WebhookConfig, WebhookEndpoint
from hitl_middleware.models import NotificationIntent
from hitl_middleware.plugins.webhook.plugin import WebhookPlugin


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _make_notification(**kw) -> NotificationIntent:
    defaults = {
        "guard_event_id": uuid.uuid4(),
        "session_id": uuid.uuid4(),
        "action_summary": "webhook delivery test",
    }
    defaults.update(kw)
    return NotificationIntent(**defaults)


# ---------------------------------------------------------------------------
# 1. test_webhook_post_body_matches_intent
# ---------------------------------------------------------------------------


async def test_webhook_post_body_matches_intent():
    """The JSON body POSTed to the webhook matches the intent's model_dump."""
    cfg = WebhookConfig(
        notification_endpoints=[WebhookEndpoint(url="http://hook:9999/n")],
    )
    plugin = WebhookPlugin(config=cfg)
    intent = _make_notification(action_summary="verify body format")
    mock_resp = httpx.Response(200)

    with patch.object(plugin, "_get_client") as mock_client_factory:
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_resp)
        mock_client_factory.return_value = mock_client

        await plugin.send_notification(intent)

        mock_client.post.assert_awaited_once()
        call_kwargs = mock_client.post.call_args
        sent_json = call_kwargs.kwargs.get("json") or call_kwargs[1].get("json")
        expected = intent.model_dump(mode="json")
        assert sent_json == expected


# ---------------------------------------------------------------------------
# 2. test_webhook_custom_headers_sent
# ---------------------------------------------------------------------------


async def test_webhook_custom_headers_sent():
    """Custom auth headers from endpoint config are sent in the POST."""
    cfg = WebhookConfig(
        notification_endpoints=[
            WebhookEndpoint(
                url="http://hook:9999/n",
                headers={"Authorization": "Bearer tok-123", "X-Custom": "value"},
            )
        ],
    )
    plugin = WebhookPlugin(config=cfg)
    intent = _make_notification()
    mock_resp = httpx.Response(200)

    with patch.object(plugin, "_get_client") as mock_client_factory:
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_resp)
        mock_client_factory.return_value = mock_client

        await plugin.send_notification(intent)

        call_kwargs = mock_client.post.call_args
        sent_headers = call_kwargs.kwargs.get("headers") or call_kwargs[1].get("headers")
        assert sent_headers["Authorization"] == "Bearer tok-123"
        assert sent_headers["X-Custom"] == "value"


# ---------------------------------------------------------------------------
# 3. test_webhook_connection_refused_handling
# ---------------------------------------------------------------------------


async def test_webhook_connection_refused_handling():
    """Connection refused is caught and send_notification returns False."""
    cfg = WebhookConfig(
        notification_endpoints=[WebhookEndpoint(url="http://unreachable:1/n")],
    )
    plugin = WebhookPlugin(config=cfg)
    intent = _make_notification()

    with patch.object(plugin, "_get_client") as mock_client_factory:
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=httpx.ConnectError("Connection refused"))
        mock_client_factory.return_value = mock_client

        result = await plugin.send_notification(intent)
        assert result is False


# ---------------------------------------------------------------------------
# 4. test_webhook_timeout_handling
# ---------------------------------------------------------------------------


async def test_webhook_timeout_handling():
    """A slow endpoint triggers timeout, handled gracefully."""
    cfg = WebhookConfig(
        notification_endpoints=[WebhookEndpoint(url="http://slow:9999/n", timeout_seconds=1.0)],
    )
    plugin = WebhookPlugin(config=cfg)
    intent = _make_notification()

    with patch.object(plugin, "_get_client") as mock_client_factory:
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=httpx.ReadTimeout("Read timed out"))
        mock_client_factory.return_value = mock_client

        result = await plugin.send_notification(intent)
        assert result is False


# ---------------------------------------------------------------------------
# 5. test_webhook_disabled_endpoint_skipped
# ---------------------------------------------------------------------------


async def test_webhook_disabled_endpoint_skipped():
    """An endpoint with enabled=False is not POSTed to."""
    cfg = WebhookConfig(
        notification_endpoints=[WebhookEndpoint(url="http://disabled:9999/n", enabled=False)],
    )
    plugin = WebhookPlugin(config=cfg)
    intent = _make_notification()

    with patch.object(plugin, "_get_client") as mock_client_factory:
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=httpx.Response(200))
        mock_client_factory.return_value = mock_client

        result = await plugin.send_notification(intent)
        # No enabled endpoints, so no POST and returns False
        assert result is False
        mock_client.post.assert_not_awaited()


# ---------------------------------------------------------------------------
# 6. test_webhook_multiple_endpoints_fan_out
# ---------------------------------------------------------------------------


async def test_webhook_multiple_endpoints_fan_out():
    """Three notification endpoints all receive a POST."""
    cfg = WebhookConfig(
        notification_endpoints=[
            WebhookEndpoint(url="http://hook1:9999/n"),
            WebhookEndpoint(url="http://hook2:9999/n"),
            WebhookEndpoint(url="http://hook3:9999/n"),
        ],
    )
    plugin = WebhookPlugin(config=cfg)
    intent = _make_notification()
    mock_resp = httpx.Response(200)

    with patch.object(plugin, "_get_client") as mock_client_factory:
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_resp)
        mock_client_factory.return_value = mock_client

        result = await plugin.send_notification(intent)
        assert result is True
        assert mock_client.post.await_count == 3
        # Verify each endpoint URL was called
        called_urls = [call.args[0] for call in mock_client.post.call_args_list]
        assert "http://hook1:9999/n" in called_urls
        assert "http://hook2:9999/n" in called_urls
        assert "http://hook3:9999/n" in called_urls
