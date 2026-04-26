"""Gateway identity middleware — extracts 5 identity headers into request.state."""
from __future__ import annotations

import logging
import os
import string

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

logger = logging.getLogger(__name__)


# #1493 RESOLVED (R2-P5): X-EB-* header charset enforcement. Allowed
# characters cover the documented identity formats (alphanumeric,
# underscore, hyphen, colon for `agent_key = {gw}:{agentId}`). Anything
# outside the set is rejected at request boundary so injection payloads
# (Cypher fragments, control bytes, megabyte-scale strings) never reach
# downstream consumers (RedisKeyBuilder, Cypher stamp sites, metric
# labels). Length cap prevents header-stuffing memory attacks.
_HEADER_ALLOWED_CHARSET: frozenset[str] = frozenset(
    string.ascii_letters + string.digits + "_-:",
)
_HEADER_MAX_LEN: int = 255

# X-EB-Gateway-ID has stricter rules: in addition to the per-header
# charset above, it MUST also reject `:*?[]` per A6 (TF-FN-017
# startup-safety check). The shared set already excludes `*?[]`; the
# only delta is `:` — which is forbidden in gateway_id but allowed in
# agent_key. So gateway_id charset = letters + digits + `_-` only.
_GATEWAY_ID_FORBIDDEN: frozenset[str] = frozenset(":*?[]")


def _validate_header(name: str, value: str, *, gateway_id_strict: bool) -> str | None:
    """Validate one X-EB-* header value.

    Returns an error-detail string on rejection, or ``None`` on success.
    Empty value is always valid (means header was not sent — middleware
    falls back to default elsewhere).
    """
    if not value:
        return None
    if len(value) > _HEADER_MAX_LEN:
        return (
            f"{name} length {len(value)} exceeds max {_HEADER_MAX_LEN}"
        )
    bad_chars = set(value) - _HEADER_ALLOWED_CHARSET
    if bad_chars:
        return (
            f"{name} contains forbidden characters {sorted(bad_chars)!r}; "
            f"allowed charset: alphanumeric, _, -, :"
        )
    if gateway_id_strict:
        gw_forbidden = set(value) & _GATEWAY_ID_FORBIDDEN
        if gw_forbidden:
            return (
                f"{name} contains gateway_id-forbidden characters "
                f"{sorted(gw_forbidden)!r} (A6 rule from TF-FN-017 startup "
                f"safety: gateway_id must not contain : * ? [ ])"
            )
    return None


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
        # #1493 RESOLVED (R2-P5): per-header charset + length validation.
        # Run BEFORE the gateway_id mismatch reject so injection payloads
        # never have a chance to leak through the escape hatch (e.g.,
        # ``EB_ALLOW_CROSS_GATEWAY_HEADER=true`` would otherwise pass a
        # malformed value to request.state.gateway_id).
        header_gw = request.headers.get("X-EB-Gateway-ID") or ""
        header_agent_key = request.headers.get("X-EB-Agent-Key") or ""
        header_agent_id = request.headers.get("X-EB-Agent-ID") or ""
        header_session_key = request.headers.get("X-EB-Session-Key") or ""
        header_actor_id = request.headers.get("X-EB-Actor-Id") or ""
        for name, value, gw_strict in (
            ("X-EB-Gateway-ID", header_gw, True),
            ("X-EB-Agent-Key", header_agent_key, False),
            ("X-EB-Agent-ID", header_agent_id, False),
            ("X-EB-Session-Key", header_session_key, False),
            ("X-EB-Actor-Id", header_actor_id, False),
        ):
            err = _validate_header(name, value, gateway_id_strict=gw_strict)
            if err is not None:
                logger.warning("Gateway middleware rejected request (400): %s | source=%s", err, request.client)
                return JSONResponse(
                    status_code=400,
                    content={"detail": err},
                )

        # R2-P1.1: reject when caller-supplied gateway header conflicts
        # with the container's configured gateway_id. Empty header still
        # falls back to default (legacy behavior preserved).
        if (
            header_gw
            and self._default
            and header_gw != self._default
            and not self._allow_cross
        ):
            detail = (
                f"Cross-gateway request rejected: header "
                f"X-EB-Gateway-ID={header_gw!r} does not match "
                f"container gateway_id={self._default!r}. "
                f"EB is single-tenant-per-process; spin up a "
                f"separate EB process for each gateway. "
                f"For testing, set EB_ALLOW_CROSS_GATEWAY_HEADER=true."
            )
            logger.warning("Gateway middleware rejected request (403): %s | source=%s", detail, request.client)
            return JSONResponse(
                status_code=403,
                content={"detail": detail},
            )

        if (
            header_gw
            and self._default
            and header_gw != self._default
            and self._allow_cross
        ):
            logger.warning(
                "Cross-gateway header bypass: header=%s default=%s source=%s",
                header_gw, self._default, request.client,
            )
            from elephantbroker.runtime.metrics import METRICS_AVAILABLE
            if METRICS_AVAILABLE:
                from elephantbroker.runtime.metrics import eb_cross_gateway_header_bypass_total
                eb_cross_gateway_header_bypass_total.labels(gateway_id=self._default).inc()

        request.state.gateway_id = header_gw or self._default
        request.state.agent_key = header_agent_key
        request.state.agent_id = header_agent_id
        request.state.session_key = header_session_key
        request.state.actor_id = header_actor_id
        return await call_next(request)
