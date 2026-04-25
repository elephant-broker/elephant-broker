"""Gateway identity middleware — extracts 5 identity headers into request.state."""
from __future__ import annotations

import os

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response


class GatewayIdentityMiddleware(BaseHTTPMiddleware):
    """Extract gateway identity from HTTP headers into ``request.state``.

    Headers:
        X-EB-Gateway-ID  → request.state.gateway_id
        X-EB-Agent-Key   → request.state.agent_key
        X-EB-Agent-ID    → request.state.agent_id
        X-EB-Session-Key → request.state.session_key
        X-EB-Actor-Id    → request.state.actor_id

    Falls back to ``default_gateway_id`` when the header is absent. The
    app factory (``elephantbroker/api/app.py``) wires this to
    ``container.config.gateway.gateway_id`` so the middleware fallback is
    always byte-identical to the gateway_id the runtime modules were
    constructed with.

    **Tenant-isolation enforcement (R2-P1.1, #1187 boundary fix):**
    rejects requests where ``X-EB-Gateway-ID`` is set AND does not match
    the container's startup gateway_id. EB is single-tenant-per-process
    per ``docs/DEPLOYMENT.md`` — multi-gateway deployment requires
    multiple EB processes, one per gateway. Cognee's process-singleton
    Qdrant adapter (``database_name`` set once at config init) makes
    multi-gateway-per-process fundamentally unworkable; this middleware
    closes the cross-tenant-via-header bypass that R2-P1's startup-time
    fix could not address (a request arriving with an attacker-supplied
    X-EB-Gateway-ID would otherwise have its identity stamped from the
    header and reach the facade with a mismatched gateway_id).

    Pre-R2-P1.1 the middleware silently accepted any header value
    (TF-FN-014 G7 #1493 PROD pin documented this); G7 is FLIPPED in the
    same R2-P1.1 commit asserting the 403 reject contract.

    **Escape hatch:** ``EB_ALLOW_CROSS_GATEWAY_HEADER=true`` env var
    bypasses the check. Used by integration tests / L2 probes that spin
    up a single process and probe cross-tenant scenarios. NEVER set in
    production.
    """

    def __init__(self, app, default_gateway_id: str = "") -> None:  # type: ignore[override]
        super().__init__(app)
        self._default = default_gateway_id
        self._allow_cross = (
            os.environ.get("EB_ALLOW_CROSS_GATEWAY_HEADER", "").lower() == "true"
        )

    async def dispatch(self, request: Request, call_next) -> Response:  # type: ignore[override]
        header_gw = request.headers.get("X-EB-Gateway-ID") or ""
        # R2-P1.1: reject when caller-supplied gateway header conflicts
        # with the container's configured gateway_id. Empty header still
        # falls back to default (legacy behavior preserved).
        if (
            header_gw
            and self._default
            and header_gw != self._default
            and not self._allow_cross
        ):
            return JSONResponse(
                status_code=403,
                content={
                    "detail": (
                        f"Cross-gateway request rejected: header "
                        f"X-EB-Gateway-ID={header_gw!r} does not match "
                        f"container gateway_id={self._default!r}. "
                        f"EB is single-tenant-per-process; spin up a "
                        f"separate EB process for each gateway. "
                        f"For testing, set EB_ALLOW_CROSS_GATEWAY_HEADER=true."
                    )
                },
            )

        request.state.gateway_id = header_gw or self._default
        request.state.agent_key = request.headers.get("X-EB-Agent-Key") or ""
        request.state.agent_id = request.headers.get("X-EB-Agent-ID") or ""
        request.state.session_key = request.headers.get("X-EB-Session-Key") or ""
        request.state.actor_id = request.headers.get("X-EB-Actor-Id") or ""
        return await call_next(request)
