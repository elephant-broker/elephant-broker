"""Unit tests for webhook configuration models — 3 tests."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from hitl_middleware.config import WebhookConfig, WebhookEndpoint


class TestWebhookEndpointDefaults:
    def test_defaults(self):
        """WebhookEndpoint has correct default values."""
        ep = WebhookEndpoint(url="http://hook.example.com")
        assert ep.url == "http://hook.example.com"
        assert ep.headers == {}
        assert ep.timeout_seconds == 10.0
        assert ep.enabled is True


class TestWebhookConfigValidation:
    def test_validation_retry_count(self):
        """retry_count must be >= 0."""
        with pytest.raises(ValidationError):
            WebhookConfig(retry_count=-1)

    def test_multiple_endpoints_accepted(self):
        """WebhookConfig accepts multiple endpoints of both types."""
        wc = WebhookConfig(
            notification_endpoints=[
                WebhookEndpoint(url="http://a.com"),
                WebhookEndpoint(url="http://b.com"),
            ],
            approval_endpoints=[
                WebhookEndpoint(url="http://c.com"),
                WebhookEndpoint(url="http://d.com"),
                WebhookEndpoint(url="http://e.com"),
            ],
            retry_count=5,
            retry_delay_seconds=2.0,
        )
        assert len(wc.notification_endpoints) == 2
        assert len(wc.approval_endpoints) == 3
        assert wc.retry_count == 5
        assert wc.retry_delay_seconds == 2.0
