"""Unit tests for hitl_middleware.config — 10 tests."""
from __future__ import annotations

import pytest
from pydantic import ValidationError

from hitl_middleware.config import HitlMiddlewareConfig, WebhookConfig, WebhookEndpoint


class TestHitlMiddlewareConfig:
    def test_defaults(self):
        """Config has sane defaults."""
        cfg = HitlMiddlewareConfig()
        assert cfg.host == "0.0.0.0"
        assert cfg.port == 8421
        assert cfg.log_level == "INFO"
        assert cfg.callback_secret == ""
        assert cfg.runtime_url == "http://localhost:8420"

    def test_from_env_with_vars(self, monkeypatch):
        """from_env() reads from environment variables."""
        monkeypatch.setenv("HITL_HOST", "127.0.0.1")
        monkeypatch.setenv("HITL_PORT", "9000")
        monkeypatch.setenv("HITL_LOG_LEVEL", "DEBUG")
        monkeypatch.setenv("EB_HITL_CALLBACK_SECRET", "s3cret")
        monkeypatch.setenv("EB_RUNTIME_URL", "http://runtime:8420")

        cfg = HitlMiddlewareConfig.from_env()
        assert cfg.host == "127.0.0.1"
        assert cfg.port == 9000
        assert cfg.log_level == "DEBUG"
        assert cfg.callback_secret == "s3cret"
        assert cfg.runtime_url == "http://runtime:8420"

    def test_from_env_partial_vars(self, monkeypatch):
        """from_env() uses defaults for missing vars."""
        monkeypatch.setenv("HITL_PORT", "5555")
        monkeypatch.delenv("HITL_HOST", raising=False)
        monkeypatch.delenv("HITL_LOG_LEVEL", raising=False)
        monkeypatch.delenv("EB_HITL_CALLBACK_SECRET", raising=False)
        monkeypatch.delenv("EB_RUNTIME_URL", raising=False)

        cfg = HitlMiddlewareConfig.from_env()
        assert cfg.port == 5555
        assert cfg.host == "0.0.0.0"
        assert cfg.callback_secret == ""

    def test_missing_callback_secret_empty(self):
        """callback_secret defaults to empty string."""
        cfg = HitlMiddlewareConfig()
        assert cfg.callback_secret == ""

    def test_port_validation_too_high(self):
        """Port above 65535 is rejected."""
        with pytest.raises(ValidationError):
            HitlMiddlewareConfig(port=70000)

    def test_port_validation_too_low(self):
        """Port below 1 is rejected."""
        with pytest.raises(ValidationError):
            HitlMiddlewareConfig(port=0)

    def test_runtime_url_custom_format(self):
        """runtime_url accepts various URL formats."""
        cfg = HitlMiddlewareConfig(runtime_url="https://api.example.com:443/v1")
        assert cfg.runtime_url == "https://api.example.com:443/v1"

    def test_log_level_values(self):
        """log_level accepts any string (validated at logging setup)."""
        cfg = HitlMiddlewareConfig(log_level="WARNING")
        assert cfg.log_level == "WARNING"


class TestWebhookConfig:
    def test_empty_lists(self):
        """WebhookConfig defaults to empty endpoint lists."""
        wc = WebhookConfig()
        assert wc.notification_endpoints == []
        assert wc.approval_endpoints == []
        assert wc.retry_count == 3

    def test_multiple_endpoints(self):
        """WebhookConfig can hold multiple endpoints."""
        wc = WebhookConfig(
            notification_endpoints=[
                WebhookEndpoint(url="http://a.com/hook"),
                WebhookEndpoint(url="http://b.com/hook"),
            ],
            approval_endpoints=[
                WebhookEndpoint(url="http://c.com/hook"),
            ],
        )
        assert len(wc.notification_endpoints) == 2
        assert len(wc.approval_endpoints) == 1
