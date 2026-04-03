"""Tests for the GatewayIdentityMiddleware."""
import pytest
from httpx import ASGITransport, AsyncClient
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse
from starlette.routing import Route

from elephantbroker.api.middleware.gateway import GatewayIdentityMiddleware


def _echo_identity(request: Request) -> JSONResponse:
    return JSONResponse({
        "gateway_id": getattr(request.state, "gateway_id", ""),
        "agent_key": getattr(request.state, "agent_key", ""),
        "agent_id": getattr(request.state, "agent_id", ""),
        "session_key": getattr(request.state, "session_key", ""),
    })


def _make_app(default_gw: str = "local"):
    app = Starlette(routes=[Route("/echo", _echo_identity)])
    app.add_middleware(GatewayIdentityMiddleware, default_gateway_id=default_gw)
    return app


@pytest.mark.asyncio
async def test_extracts_all_four_headers():
    app = _make_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/echo", headers={
            "X-EB-Gateway-ID": "gw-test",
            "X-EB-Agent-Key": "gw-test:main",
            "X-EB-Agent-ID": "main",
            "X-EB-Session-Key": "agent:main:main",
        })
        data = resp.json()
        assert data["gateway_id"] == "gw-test"
        assert data["agent_key"] == "gw-test:main"
        assert data["agent_id"] == "main"
        assert data["session_key"] == "agent:main:main"


@pytest.mark.asyncio
async def test_falls_back_to_default_when_headers_missing():
    app = _make_app("my-default")
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/echo")
        data = resp.json()
        assert data["gateway_id"] == "my-default"
        assert data["agent_key"] == ""
        assert data["agent_id"] == ""


@pytest.mark.asyncio
async def test_sets_empty_string_when_header_missing_and_no_default():
    app = _make_app("")
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/echo")
        data = resp.json()
        assert data["gateway_id"] == ""


@pytest.mark.asyncio
async def test_passes_through_to_next_handler():
    app = _make_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/echo", headers={"X-EB-Gateway-ID": "gw-1"})
        assert resp.status_code == 200
