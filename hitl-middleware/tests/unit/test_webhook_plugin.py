"""Unit tests for hitl_middleware.plugins.webhook.plugin — 10 tests."""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, patch, MagicMock

import httpx
import pytest

from hitl_middleware.config import WebhookConfig, WebhookEndpoint
from hitl_middleware.models import ApprovalIntent, NotificationIntent
from hitl_middleware.plugins.webhook.plugin import WebhookPlugin


def _notification_intent(**kw) -> NotificationIntent:
    defaults = {"guard_event_id": uuid.uuid4(), "session_id": uuid.uuid4()}
    defaults.update(kw)
    return NotificationIntent(**defaults)


def _approval_intent(**kw) -> ApprovalIntent:
    defaults = {
        "request_id": uuid.uuid4(),
        "guard_event_id": uuid.uuid4(),
        "session_id": uuid.uuid4(),
    }
    defaults.update(kw)
    return ApprovalIntent(**defaults)


def _ok_response():
    return httpx.Response(
        200, json={"ok": True}, request=httpx.Request("POST", "http://hook.test")
    )


def _error_response(status_code=500):
    return httpx.Response(
        status_code, json={"error": "fail"}, request=httpx.Request("POST", "http://hook.test")
    )


class TestWebhookPlugin:
    async def test_send_notification_posts_to_endpoints(self):
        """send_notification POSTs to all enabled notification endpoints."""
        config = WebhookConfig(
            notification_endpoints=[
                WebhookEndpoint(url="http://hook-a.test/notify"),
                WebhookEndpoint(url="http://hook-b.test/notify"),
            ]
        )
        plugin = WebhookPlugin(config=config)
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=_ok_response())
        plugin._client = mock_client

        result = await plugin.send_notification(_notification_intent())
        assert result is True
        assert mock_client.post.call_count == 2

    async def test_skips_disabled_endpoints(self):
        """Disabled endpoints are not called."""
        config = WebhookConfig(
            notification_endpoints=[
                WebhookEndpoint(url="http://hook-a.test/notify", enabled=True),
                WebhookEndpoint(url="http://hook-b.test/notify", enabled=False),
            ]
        )
        plugin = WebhookPlugin(config=config)
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=_ok_response())
        plugin._client = mock_client

        result = await plugin.send_notification(_notification_intent())
        assert result is True
        assert mock_client.post.call_count == 1

    async def test_includes_auth_headers(self):
        """Custom headers from endpoint config are sent with the request."""
        config = WebhookConfig(
            notification_endpoints=[
                WebhookEndpoint(
                    url="http://hook.test/notify",
                    headers={"Authorization": "Bearer tok123"},
                ),
            ]
        )
        plugin = WebhookPlugin(config=config)
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=_ok_response())
        plugin._client = mock_client

        await plugin.send_notification(_notification_intent())
        call_kwargs = mock_client.post.call_args
        assert call_kwargs[1]["headers"]["Authorization"] == "Bearer tok123"

    async def test_logs_http_error(self):
        """HTTP error responses are handled (returns False for that endpoint)."""
        config = WebhookConfig(
            notification_endpoints=[
                WebhookEndpoint(url="http://hook.test/notify"),
            ]
        )
        plugin = WebhookPlugin(config=config)
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=_error_response(500))
        plugin._client = mock_client

        result = await plugin.send_notification(_notification_intent())
        assert result is False

    async def test_logs_connection_error(self):
        """Connection errors are caught and logged (returns False)."""
        config = WebhookConfig(
            notification_endpoints=[
                WebhookEndpoint(url="http://hook.test/notify"),
            ]
        )
        plugin = WebhookPlugin(config=config)
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=httpx.ConnectError("refused"))
        plugin._client = mock_client

        result = await plugin.send_notification(_notification_intent())
        assert result is False

    async def test_send_approval_posts_to_approval_endpoints(self):
        """send_approval_request POSTs to all enabled approval endpoints."""
        config = WebhookConfig(
            approval_endpoints=[
                WebhookEndpoint(url="http://hook.test/approval"),
            ]
        )
        plugin = WebhookPlugin(config=config)
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=_ok_response())
        plugin._client = mock_client

        result = await plugin.send_approval_request(_approval_intent())
        assert result is True
        assert mock_client.post.call_count == 1

    async def test_approval_includes_callback_urls_in_payload(self):
        """Approval request payload includes callback URLs from the intent."""
        config = WebhookConfig(
            approval_endpoints=[
                WebhookEndpoint(url="http://hook.test/approval"),
            ]
        )
        plugin = WebhookPlugin(config=config)
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=_ok_response())
        plugin._client = mock_client

        intent = _approval_intent(
            approve_callback_url="http://hitl/approve",
            reject_callback_url="http://hitl/reject",
        )
        await plugin.send_approval_request(intent)
        call_kwargs = mock_client.post.call_args
        payload = call_kwargs[1]["json"]
        assert payload["approve_callback_url"] == "http://hitl/approve"
        assert payload["reject_callback_url"] == "http://hitl/reject"

    async def test_timeout_handling(self):
        """Timeout errors are caught and logged (returns False)."""
        config = WebhookConfig(
            notification_endpoints=[
                WebhookEndpoint(url="http://hook.test/notify", timeout_seconds=1.0),
            ]
        )
        plugin = WebhookPlugin(config=config)
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=httpx.ReadTimeout("timeout"))
        plugin._client = mock_client

        result = await plugin.send_notification(_notification_intent())
        assert result is False

    async def test_close_shuts_down_client(self):
        """close() calls aclose() on the httpx client."""
        config = WebhookConfig()
        plugin = WebhookPlugin(config=config)
        mock_client = AsyncMock()
        mock_client.aclose = AsyncMock()
        plugin._client = mock_client

        await plugin.close()
        mock_client.aclose.assert_awaited_once()
        assert plugin._client is None

    async def test_lazy_init(self):
        """Client is not created until first use."""
        config = WebhookConfig()
        plugin = WebhookPlugin(config=config)
        assert plugin._client is None
        client = plugin._get_client()
        assert client is not None
        assert plugin._client is client
        # Cleanup
        await plugin.close()
