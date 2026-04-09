"""Gateway identity middleware — extracts 4 identity headers into request.state."""
from __future__ import annotations

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response


class GatewayIdentityMiddleware(BaseHTTPMiddleware):
    """Extract gateway identity from HTTP headers into ``request.state``.

    Headers:
        X-EB-Gateway-ID  → request.state.gateway_id
        X-EB-Agent-Key   → request.state.agent_key
        X-EB-Agent-ID    → request.state.agent_id
        X-EB-Session-Key → request.state.session_key

    Falls back to ``default_gateway_id`` when the header is absent. The
    app factory (``elephantbroker/api/app.py``) wires this to
    ``container.config.gateway.gateway_id`` so the middleware fallback is
    always byte-identical to the gateway_id the runtime modules were
    constructed with. The signature default is the empty string, matching
    the post-Bucket-A ``GatewayConfig.gateway_id`` default — this is a
    hygiene choice, not a behavior choice, since the app factory always
    passes the kwarg explicitly in production wiring.
    """

    def __init__(self, app, default_gateway_id: str = "") -> None:  # type: ignore[override]
        super().__init__(app)
        self._default = default_gateway_id

    async def dispatch(self, request: Request, call_next) -> Response:  # type: ignore[override]
        request.state.gateway_id = request.headers.get("X-EB-Gateway-ID") or self._default
        request.state.agent_key = request.headers.get("X-EB-Agent-Key") or ""
        request.state.agent_id = request.headers.get("X-EB-Agent-ID") or ""
        request.state.session_key = request.headers.get("X-EB-Session-Key") or ""
        request.state.actor_id = request.headers.get("X-EB-Actor-Id") or ""
        return await call_next(request)
