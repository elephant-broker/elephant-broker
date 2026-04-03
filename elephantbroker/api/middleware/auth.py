"""API key authentication stub — always passes."""
from __future__ import annotations

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request
from starlette.responses import Response


class AuthMiddleware(BaseHTTPMiddleware):
    """Stub: always passes. Real auth in a future phase."""

    async def dispatch(self, request: Request, call_next) -> Response:
        # Placeholder: extract and validate API key here in the future
        return await call_next(request)
