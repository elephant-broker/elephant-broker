"""Tests for error handler and auth middleware."""
import logging
from unittest.mock import AsyncMock, MagicMock

from fastapi import Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from starlette.requests import Request as StarletteRequest

from elephantbroker.api.middleware.auth import AuthMiddleware
from elephantbroker.api.middleware.errors import error_handler_middleware


def _make_request():
    scope = {"type": "http", "method": "GET", "path": "/test", "headers": []}
    return StarletteRequest(scope)


class TestErrorHandlerMiddleware:
    async def test_success_passthrough(self):
        request = _make_request()
        expected = Response(content="ok")
        call_next = AsyncMock(return_value=expected)
        result = await error_handler_middleware(request, call_next)
        assert result is expected

    async def test_key_error_returns_404(self):
        request = _make_request()
        call_next = AsyncMock(side_effect=KeyError("missing"))
        result = await error_handler_middleware(request, call_next)
        assert result.status_code == 404
        assert isinstance(result, JSONResponse)

    async def test_value_error_returns_422(self):
        request = _make_request()
        call_next = AsyncMock(side_effect=ValueError("bad"))
        result = await error_handler_middleware(request, call_next)
        assert result.status_code == 422

    async def test_generic_exception_returns_500(self, caplog):
        """G2 extension: unhandled exception returns 500 AND logs at ERROR with exc_info."""
        request = _make_request()
        call_next = AsyncMock(side_effect=RuntimeError("boom"))
        with caplog.at_level(logging.ERROR, logger="elephantbroker.api.errors"):
            result = await error_handler_middleware(request, call_next)
        assert result.status_code == 500
        assert "Unhandled error on GET /test: boom" in caplog.text

    async def test_response_content_type_json(self):
        request = _make_request()
        call_next = AsyncMock(side_effect=KeyError("x"))
        result = await error_handler_middleware(request, call_next)
        assert result.media_type == "application/json"

    async def test_request_validation_error_returns_422_with_warning_log(self, caplog):
        """G1 (#305): RequestValidationError returns 422 AND emits a WARNING log with full
        request method + path + error details. Pins the debugging affordance for 422s
        originating from plugin schema mismatches.
        """
        request = _make_request()
        call_next = AsyncMock(side_effect=RequestValidationError([
            {"loc": ("body", "x"), "msg": "required", "type": "missing"},
        ]))
        with caplog.at_level(logging.WARNING, logger="elephantbroker.api.errors"):
            result = await error_handler_middleware(request, call_next)
        assert result.status_code == 422
        assert "Validation error on GET /test" in caplog.text

    async def test_permission_error_falls_to_500_documented_gap(self):
        """Pins documented GAP #1170 — PermissionError is NOT mapped to 403; it falls
        through to the generic Exception handler and returns 500.

        If a future fix adds a PermissionError handler at 403 (for cross-gateway delete
        rejections etc.), update this test, the flow plan, and file a TD.
        """
        request = _make_request()
        call_next = AsyncMock(side_effect=PermissionError("denied"))
        result = await error_handler_middleware(request, call_next)
        assert result.status_code == 500


class TestAuthMiddleware:
    async def test_passes_request_through(self):
        """G8 (#1494): Pins PROD risk — AuthMiddleware is a stub. No API key / token / HMAC
        validation. Every request passes through regardless of authorization.

        This is an intentional Phase 3 placeholder; real auth lands in a later phase.
        If real auth is added (API key / JWT / mTLS), update this test and the flow plan.
        """
        middleware = AuthMiddleware(app=MagicMock())
        request = _make_request()
        expected = Response(content="ok")
        call_next = AsyncMock(return_value=expected)
        result = await middleware.dispatch(request, call_next)
        assert result is expected

    async def test_preserves_response(self):
        middleware = AuthMiddleware(app=MagicMock())
        request = _make_request()
        expected = Response(content="preserved", status_code=201)
        call_next = AsyncMock(return_value=expected)
        result = await middleware.dispatch(request, call_next)
        assert result.status_code == 201
