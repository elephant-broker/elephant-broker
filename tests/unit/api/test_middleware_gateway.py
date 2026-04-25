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
    # R2-P1.1: matching default_gw to header value so the new mismatch
    # reject does not fire — this test exercises the extraction path,
    # not the reject path. The reject path is covered separately in
    # `test_gateway_reject_mismatch.py`.
    app = _make_app("gw-test")
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
    # R2-P1.1: matching default to header so the new mismatch reject
    # does not fire — this is a passthrough sanity test, not a reject
    # test.
    app = _make_app("gw-1")
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/echo", headers={"X-EB-Gateway-ID": "gw-1"})
        assert resp.status_code == 200


@pytest.mark.asyncio
async def test_extracts_fifth_actor_id_header():
    """G3 (#1174): The 5th X-EB-Actor-Id header is extracted into request.state.actor_id.

    CLAUDE.md Gateway Identity section formerly listed 4 headers; this test + D16
    doc-fix brings it in sync with the shipped code at gateway.py:37.

    R2-P1.1: this test does NOT send X-EB-Gateway-ID, so the new mismatch
    reject does not fire — the default-fallback path is exercised.
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
    The middleware does NOT validate/enforce the agent_key shape itself — it
    extracts agent_key verbatim. (Pre-R2-P1.1 the gateway_id header was also
    extracted verbatim; that part is now bounded by the mismatch reject —
    see `test_gateway_reject_mismatch.py`.) This test documents the
    agent_key shape a compliant caller is expected to send and verifies
    that when the caller does follow the convention, the three state
    fields (`gateway_id`, `agent_id`, `agent_key`) are internally
    consistent on the server side.

    If this invariant ever ships with server-side synthesis (e.g., middleware
    deriving `agent_key` from `gateway_id` + `agent_id` when absent), update
    this test AND the CLAUDE.md Gateway Identity section.
    """
    # R2-P1.1: matching default to header to avoid mismatch reject.
    app = _make_app("gw-prod")
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
async def test_injection_gateway_id_header_rejected_post_R2P1_1_fix(monkeypatch):
    """G7 FLIPPED (#1493 RESOLVED — R2-P1.1): pre-R2-P1.1 the middleware
    silently accepted ANY ``X-EB-Gateway-ID`` value (including
    Cypher-injection payloads, null-byte sequences, megabyte-scale
    strings) and stamped it verbatim onto ``request.state.gateway_id``.
    R2-P1.1 added boundary-level enforcement: any header that does not
    equal the container's startup gateway_id is rejected with 403,
    closing the cross-tenant-via-header bypass.

    This subsumes the original #1493 PROD pin: an injection payload
    in ``X-EB-Gateway-ID`` is now blocked at the boundary, never
    reaching ``request.state``. (The other two headers covered by the
    old pin — ``X-EB-Agent-Key`` and ``X-EB-Session-Key`` — are still
    accepted verbatim; they're not gateway-isolation keys, just
    request-context tags. Their downstream consumers — A6 startup
    safety on gateway_id; RedisKeyBuilder's char checks; etc — apply
    sanitization at use site. A future #1493 follow-up could add a
    suffix pass for those two headers, but it's not the
    cross-gateway-bypass surface the original pin documented.)

    Pre-fix: this test pinned `_make_app()` + injection-payload
    `X-EB-Gateway-ID` header → 200 OK + value stamped on
    ``request.state``. Post-fix: same input → 403 reject because the
    injection payload is not equal to the container's
    ``default_gateway_id``.
    """
    monkeypatch.delenv("EB_ALLOW_CROSS_GATEWAY_HEADER", raising=False)
    app = _make_app("local")  # default_gateway_id="local"
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as client:
        resp = await client.get("/echo", headers={
            "X-EB-Gateway-ID": "'; DROP TABLE users; --",
            "X-EB-Agent-Key": "\x01\x02\x03",
            "X-EB-Session-Key": "a" * 1000,
        })
    # R2-P1.1: injection payload != "local" → 403 reject.
    assert resp.status_code == 403
    body = resp.json()
    assert "Cross-gateway request rejected" in body["detail"]
    # The non-gateway-isolation headers (agent_key, session_key) never
    # reached request.state because the gateway-mismatch reject fired
    # first — proves the injection payload is no longer observable from
    # the rest of the request lifecycle.
