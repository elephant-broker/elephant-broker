"""R2-P5 — GatewayIdentityMiddleware charset + length validation
(#1493 fully RESOLVED).

R2-P1.1 closed the cross-gateway-via-header bypass surface but left
all five X-EB-* headers' charset validation deferred. R2-P5 adds the
explicit charset + length checks per the lead's brief:

* Allowed charset: ``[a-zA-Z0-9_-:]`` (alphanumeric + underscore +
  hyphen + colon — colon needed for ``agent_key = {gw}:{agentId}``)
* Max length: 255
* X-EB-Gateway-ID additionally rejects ``: * ? [ ]`` per A6 startup
  safety check (TF-FN-017)

Charset check runs BEFORE the R2-P1.1 mismatch check so injection
payloads can't slip through the ``EB_ALLOW_CROSS_GATEWAY_HEADER``
escape hatch either.
"""
from __future__ import annotations

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


def _make_app(default_gw: str = ""):
    app = Starlette(routes=[Route("/echo", _echo_identity)])
    app.add_middleware(GatewayIdentityMiddleware, default_gateway_id=default_gw)
    return app


# ---------------------------------------------------------------------------
# Charset validation — allowed values
# ---------------------------------------------------------------------------


async def test_valid_charset_passes_for_all_five_headers(monkeypatch):
    """G1 (R2-P5): canonical clean values for all 5 X-EB-* headers are
    accepted. Documents the allowed-charset shape: alphanumeric + ``_-:``.
    """
    monkeypatch.delenv("EB_ALLOW_CROSS_GATEWAY_HEADER", raising=False)
    app = _make_app("gw-prod")
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/echo", headers={
            "X-EB-Gateway-ID": "gw-prod",
            "X-EB-Agent-Key": "gw-prod:worker-7",
            "X-EB-Agent-ID": "worker-7",
            "X-EB-Session-Key": "agent:main:main",
            "X-EB-Actor-Id": "actor-uuid-123",
        })
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Charset validation — forbidden characters
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "header_name,bad_value",
    [
        # SQL/Cypher injection fragment — single quote, semicolon, space
        ("X-EB-Agent-Key", "'; DROP TABLE x; --"),
        # Control bytes (per the original #1493 pin's example)
        ("X-EB-Session-Key", "\x01\x02\x03"),
        # Whitespace
        ("X-EB-Actor-Id", "actor uuid 123"),
        # Path-traversal-ish
        ("X-EB-Agent-ID", "../../../etc/passwd"),
        # Wildcard / Redis-glob metachars (which would be A6-illegal in
        # gateway_id but are also out-of-charset for the other headers)
        ("X-EB-Agent-Key", "gw*"),
        ("X-EB-Session-Key", "agent[*]"),
    ],
    ids=[
        "sql_injection_in_agent_key",
        "control_bytes_in_session_key",
        "whitespace_in_actor_id",
        "path_traversal_in_agent_id",
        "wildcard_in_agent_key",
        "bracket_in_session_key",
    ],
)
async def test_forbidden_charset_rejected_with_400(monkeypatch, header_name, bad_value):
    """G2 (R2-P5): values outside ``[a-zA-Z0-9_-:]`` are rejected with
    400 Bad Request and a detail message naming the offending header
    and characters. Parametrized over a representative spread of
    real-world injection / corruption shapes.
    """
    monkeypatch.delenv("EB_ALLOW_CROSS_GATEWAY_HEADER", raising=False)
    app = _make_app("")
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/echo", headers={header_name: bad_value})
    assert resp.status_code == 400
    body = resp.json()
    assert header_name in body["detail"]
    assert "forbidden characters" in body["detail"]


# ---------------------------------------------------------------------------
# Length validation
# ---------------------------------------------------------------------------


async def test_overlong_header_rejected_with_400(monkeypatch):
    """G3 (R2-P5): a header longer than 255 chars is rejected with 400.
    Closes the megabyte-stuffing surface the original #1493 pin
    documented.
    """
    monkeypatch.delenv("EB_ALLOW_CROSS_GATEWAY_HEADER", raising=False)
    app = _make_app("")
    transport = ASGITransport(app=app)
    overlong = "a" * 256  # one beyond the 255 cap
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/echo", headers={"X-EB-Session-Key": overlong})
    assert resp.status_code == 400
    body = resp.json()
    assert "X-EB-Session-Key" in body["detail"]
    assert "exceeds max" in body["detail"]


async def test_exactly_255_char_header_accepted(monkeypatch):
    """G3-boundary (R2-P5): exactly 255 chars is the cap (inclusive).
    Documents the boundary so a future off-by-one regression surfaces.
    """
    monkeypatch.delenv("EB_ALLOW_CROSS_GATEWAY_HEADER", raising=False)
    app = _make_app("")
    transport = ASGITransport(app=app)
    boundary = "a" * 255
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/echo", headers={"X-EB-Session-Key": boundary})
    assert resp.status_code == 200


# ---------------------------------------------------------------------------
# Gateway-ID stricter rules (A6 set: : * ? [ ])
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_gateway_id",
    ["gw:colon", "gw*", "gw?", "gw[bracket", "gw]"],
    ids=["colon", "asterisk", "qmark", "open_bracket", "close_bracket"],
)
async def test_gateway_id_rejects_a6_forbidden_chars(monkeypatch, bad_gateway_id):
    """G4 (R2-P5): X-EB-Gateway-ID specifically rejects ``: * ? [ ]`` per
    the A6 startup-safety rule from TF-FN-017. The colon is allowed in
    other headers (e.g., agent_key="gw:main") but NEVER in gateway_id
    because it makes the Redis ``eb:{gw}:`` key ambiguous with a nested
    namespace.

    Note: ``*``, ``?``, ``[``, ``]`` are also out-of-charset per the
    base allowed-set; the A6 detail message is what surfaces because
    the gateway_id strict check runs after the base charset check —
    actually, since the base charset already excludes those four, they
    fail the base check first and the A6 check is a defense-in-depth
    refinement for the only colon case. The parametrize covers all five
    so a future loosening of the base charset doesn't accidentally
    re-admit them on gateway_id.
    """
    monkeypatch.delenv("EB_ALLOW_CROSS_GATEWAY_HEADER", raising=False)
    app = _make_app("")
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/echo", headers={"X-EB-Gateway-ID": bad_gateway_id})
    assert resp.status_code == 400
    body = resp.json()
    assert "X-EB-Gateway-ID" in body["detail"]


# ---------------------------------------------------------------------------
# Validation runs BEFORE the R2-P1.1 mismatch reject (and before the
# escape hatch) — closes the bypass-via-escape-hatch surface.
# ---------------------------------------------------------------------------


async def test_charset_check_fires_even_under_escape_hatch(monkeypatch):
    """G5 (R2-P5): even when ``EB_ALLOW_CROSS_GATEWAY_HEADER=true``
    bypasses the R2-P1.1 mismatch reject, the charset validator still
    runs and rejects malformed values. This closes the
    "L2 probes can launder injection payloads through the escape hatch"
    surface.
    """
    monkeypatch.setenv("EB_ALLOW_CROSS_GATEWAY_HEADER", "true")
    app = _make_app("gw-a")
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        # Mismatched AND charset-bad — the bad chars cause the 400 even
        # though escape hatch would otherwise have allowed the mismatch.
        resp = await client.get("/echo", headers={"X-EB-Gateway-ID": "gw-b'"})
    assert resp.status_code == 400
    body = resp.json()
    assert "X-EB-Gateway-ID" in body["detail"]
