"""R2-P1.1 — GatewayIdentityMiddleware rejects mismatched X-EB-Gateway-ID
header.

Pins the new boundary-level enforcement contract: EB is single-tenant-
per-process per docs/DEPLOYMENT.md, and any request whose
X-EB-Gateway-ID header conflicts with the container's startup
gateway_id is rejected with 403. Closes the cross-tenant-via-header
bypass that R2-P1's startup-time Qdrant tenant-config fix could not
address.

Paired with TF-FN-014 G7 in test_middleware_gateway.py — that test
previously documented the silent-acceptance gap (#1493 PROD pin) and
is FLIPPED in the same commit to assert the new 403 reject contract.
"""
from __future__ import annotations

import os

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
    })


def _make_app(default_gw: str = "gw-a"):
    app = Starlette(routes=[Route("/echo", _echo_identity)])
    app.add_middleware(GatewayIdentityMiddleware, default_gateway_id=default_gw)
    return app


@pytest.mark.asyncio
async def test_middleware_rejects_mismatched_gateway_header(monkeypatch):
    """R2-P1.1 G1: a request whose X-EB-Gateway-ID header conflicts with
    the container's configured gateway_id is rejected with 403 and a
    clear detail message. Closes the cross-tenant-via-header bypass.
    """
    monkeypatch.delenv("EB_ALLOW_CROSS_GATEWAY_HEADER", raising=False)
    app = _make_app(default_gw="gw-a")
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/echo", headers={"X-EB-Gateway-ID": "gw-b"})
    assert resp.status_code == 403
    body = resp.json()
    assert "Cross-gateway request rejected" in body["detail"]
    assert "'gw-b'" in body["detail"]
    assert "'gw-a'" in body["detail"]
    assert "EB_ALLOW_CROSS_GATEWAY_HEADER" in body["detail"]


@pytest.mark.asyncio
async def test_middleware_accepts_matching_gateway_header(monkeypatch):
    """R2-P1.1 G2: a request whose header matches the container's
    gateway_id passes through untouched — the matching case is the
    common-and-correct path for clients that explicitly stamp identity.
    """
    monkeypatch.delenv("EB_ALLOW_CROSS_GATEWAY_HEADER", raising=False)
    app = _make_app(default_gw="gw-a")
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/echo", headers={"X-EB-Gateway-ID": "gw-a"})
    assert resp.status_code == 200
    assert resp.json()["gateway_id"] == "gw-a"


@pytest.mark.asyncio
async def test_middleware_accepts_empty_header_using_default(monkeypatch):
    """R2-P1.1 G3 (regression guard): a request with no X-EB-Gateway-ID
    header gets the container default stamped on request.state — the
    pre-R2-P1.1 fallback behavior is preserved.

    This is the legacy path callers like the local tooling agent rely on
    (don't bother sending the header; trust the container).
    """
    monkeypatch.delenv("EB_ALLOW_CROSS_GATEWAY_HEADER", raising=False)
    app = _make_app(default_gw="gw-a")
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/echo")
    assert resp.status_code == 200
    assert resp.json()["gateway_id"] == "gw-a"


@pytest.mark.asyncio
async def test_middleware_allows_mismatch_when_env_set(monkeypatch):
    """R2-P1.1 G4 (escape hatch): EB_ALLOW_CROSS_GATEWAY_HEADER=true
    bypasses the reject. The mismatched header value is then stamped
    onto request.state.gateway_id verbatim.

    Used by integration tests / L2 probes that need to drive a single
    EB process through cross-tenant scenarios. Must NEVER be set in
    production — that would silently re-open #1187.
    """
    monkeypatch.setenv("EB_ALLOW_CROSS_GATEWAY_HEADER", "true")
    app = _make_app(default_gw="gw-a")
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/echo", headers={"X-EB-Gateway-ID": "gw-b"})
    assert resp.status_code == 200
    # With escape hatch, header value is stamped — useful for L2 probes
    # exercising cross-tenant rejection at the facade layer.
    assert resp.json()["gateway_id"] == "gw-b"


@pytest.mark.asyncio
async def test_bypass_mismatch_emits_warning_log(monkeypatch, caplog):
    """M4: bypass active + mismatched header emits WARNING log."""
    import logging
    monkeypatch.setenv("EB_ALLOW_CROSS_GATEWAY_HEADER", "true")
    app = _make_app(default_gw="gw-a")
    transport = ASGITransport(app=app)
    with caplog.at_level(logging.WARNING, logger="elephantbroker.api.middleware.gateway"):
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/echo", headers={"X-EB-Gateway-ID": "gw-b"})
    assert resp.status_code == 200
    assert "Cross-gateway header bypass" in caplog.text
    assert "gw-b" in caplog.text
    assert "gw-a" in caplog.text


@pytest.mark.asyncio
async def test_bypass_matching_header_no_warning(monkeypatch, caplog):
    """M4-bis: bypass active + matching header → no warning (no-op path)."""
    import logging
    monkeypatch.setenv("EB_ALLOW_CROSS_GATEWAY_HEADER", "true")
    app = _make_app(default_gw="gw-a")
    transport = ASGITransport(app=app)
    with caplog.at_level(logging.WARNING, logger="elephantbroker.api.middleware.gateway"):
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get("/echo", headers={"X-EB-Gateway-ID": "gw-a"})
    assert resp.status_code == 200
    assert "Cross-gateway header bypass" not in caplog.text
