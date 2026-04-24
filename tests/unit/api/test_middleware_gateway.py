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
        "actor_id": getattr(request.state, "actor_id", ""),
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


@pytest.mark.asyncio
async def test_extracts_fifth_actor_id_header():
    """G3 (#1174): The 5th X-EB-Actor-Id header is extracted into request.state.actor_id.

    CLAUDE.md Gateway Identity section formerly listed 4 headers; this test + D16
    doc-fix brings it in sync with the shipped code at gateway.py:37.
    """
    app = _make_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/echo", headers={"X-EB-Actor-Id": "actor-uuid-123"})
        data = resp.json()
        assert data["actor_id"] == "actor-uuid-123"


@pytest.mark.asyncio
async def test_headers_accept_injection_payloads_documented_prod_risk():
    """Pins PROD risk #1493 — GatewayIdentityMiddleware does NO validation on X-EB-*
    headers. Cypher-injection payloads, null bytes, and megabyte-scale string headers
    all pass through into request.state verbatim.

    Downstream RedisKeyBuilder + MetricsContext + Cypher-stamp call sites have their
    own character-set assumptions; a sanitizing pass here would be the safer place to
    enforce them. If validation is added, update this test and #1493 in the plan.
    """
    app = _make_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/echo", headers={
            "X-EB-Gateway-ID": "'; DROP TABLE users; --",
            # ASCII-only control chars (proves absence of null-byte rejection —
            # httpx normalizes \x00 so we test the next-nearest observable case)
            "X-EB-Agent-Key": "\x01\x02\x03",
            "X-EB-Session-Key": "a" * 1000,
        })
        assert resp.status_code == 200
        data = resp.json()
        assert data["gateway_id"] == "'; DROP TABLE users; --"
        assert data["agent_key"] == "\x01\x02\x03"
        assert data["session_key"] == "a" * 1000
