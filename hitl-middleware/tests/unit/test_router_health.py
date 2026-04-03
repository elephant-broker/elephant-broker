"""Unit tests for router GET /health — 3 tests."""
from __future__ import annotations

import pytest
from httpx import ASGITransport, AsyncClient

from hitl_middleware.app import create_app
from hitl_middleware.config import HitlMiddlewareConfig, WebhookConfig, WebhookEndpoint


@pytest.fixture
def config():
    return HitlMiddlewareConfig(callback_secret="test-secret")


@pytest.fixture
def app(config):
    return create_app(config)


@pytest.fixture
async def client(app):
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as c:
        yield c


class TestHealthRoute:
    async def test_health_200(self, client):
        """GET /health returns 200."""
        resp = await client.get("/health")
        assert resp.status_code == 200

    async def test_health_body_status_ok(self, client):
        """Health response body has status=ok."""
        resp = await client.get("/health")
        body = resp.json()
        assert body["status"] == "ok"

    async def test_health_shows_plugin_count(self):
        """Health endpoint reports the number of registered plugins."""
        config = HitlMiddlewareConfig(
            callback_secret="test",
            webhook=WebhookConfig(
                notification_endpoints=[WebhookEndpoint(url="http://hook.example.com")],
            ),
        )
        app = create_app(config)
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as c:
            resp = await c.get("/health")
            body = resp.json()
            assert body["plugins_registered"] >= 1
