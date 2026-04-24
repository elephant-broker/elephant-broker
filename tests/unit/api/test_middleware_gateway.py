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
async def test_empty_gateway_id_header_triggers_default_fallback():
    """G1 (TF-FN-016): an EXPLICITLY empty `X-EB-Gateway-ID` header (as opposed to
    a missing header) falls through to `default_gateway_id`.

    The middleware dispatch does `request.headers.get(...) or self._default`, and
    `"" or x` evaluates to `x` in Python, so both `None` (missing) and `""`
    (present-but-empty) take the default branch. This test distinguishes those
    two cases at the HTTP level — `falls_back_to_default_when_headers_missing`
    exercises absent-header; this one exercises empty-string-header. Same state
    result, different wire-level input.

    Pinning this matters because a well-meaning plugin could send
    `X-EB-Gateway-ID: ""` to signal "use server default" and get the same
    behavior as omitting the header. If the middleware ever changes to treat
    empty-string as a distinct sentinel (e.g., reject, or pass through as-is),
    this test will surface the semantic change.
    """
    app = _make_app("fallback-gw")
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/echo", headers={"X-EB-Gateway-ID": ""})
        data = resp.json()
        assert data["gateway_id"] == "fallback-gw"


@pytest.mark.asyncio
async def test_agent_key_follows_expected_format_convention():
    """G2 (TF-FN-016, #555): pins the documented `{gateway_id}:{agentId}` format
    convention for `X-EB-Agent-Key`.

    CLAUDE.md Gateway Identity section defines `agent_key = {gateway_id}:{agentId}`.
    The middleware does NOT validate/enforce this — it extracts headers verbatim
    (that's #1493 territory, separately pinned). This test documents the shape a
    compliant caller is expected to send and verifies that when the caller does
    follow the convention, the three state fields (`gateway_id`, `agent_id`,
    `agent_key`) are internally consistent on the server side.

    If this invariant ever ships with server-side synthesis (e.g., middleware
    deriving `agent_key` from `gateway_id` + `agent_id` when absent), update
    this test AND the CLAUDE.md Gateway Identity section.
    """
    app = _make_app()
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/echo", headers={
            "X-EB-Gateway-ID": "gw-prod",
            "X-EB-Agent-ID": "worker-7",
            "X-EB-Agent-Key": "gw-prod:worker-7",
        })
        data = resp.json()
        # Format convention: `{gateway_id}:{agent_id}` — exactly one colon, prefix == gateway_id
        assert data["agent_key"] == "gw-prod:worker-7"
        assert data["agent_key"].count(":") == 1
        assert data["agent_key"].split(":") == [data["gateway_id"], data["agent_id"]]
        assert data["agent_key"].startswith(data["gateway_id"] + ":")


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
