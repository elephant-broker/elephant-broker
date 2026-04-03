"""Unit tests for hitl_middleware.app — 3 tests."""
from __future__ import annotations

import pytest
from fastapi import FastAPI

from hitl_middleware.app import create_app
from hitl_middleware.config import HitlMiddlewareConfig, WebhookConfig, WebhookEndpoint


class TestCreateApp:
    def test_returns_fastapi(self):
        """create_app returns a FastAPI instance."""
        config = HitlMiddlewareConfig()
        app = create_app(config)
        assert isinstance(app, FastAPI)

    def test_with_webhook_config_wires_plugin(self):
        """create_app with webhook endpoints registers the webhook plugin."""
        config = HitlMiddlewareConfig(
            webhook=WebhookConfig(
                notification_endpoints=[
                    WebhookEndpoint(url="http://hook.example.com/notify"),
                ],
            ),
        )
        app = create_app(config)
        assert app.state.registry.plugin_count == 1

    def test_with_empty_config_no_plugins(self):
        """create_app with no webhook endpoints has no plugins."""
        config = HitlMiddlewareConfig()
        app = create_app(config)
        assert app.state.registry.plugin_count == 0
