"""Tests for error handler and auth middleware."""
from unittest.mock import AsyncMock, MagicMock

from fastapi import Response
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

    async def test_generic_exception_returns_500(self):
        request = _make_request()
        call_next = AsyncMock(side_effect=RuntimeError("boom"))
        result = await error_handler_middleware(request, call_next)
        assert result.status_code == 500

    async def test_response_content_type_json(self):
        request = _make_request()
        call_next = AsyncMock(side_effect=KeyError("x"))
        result = await error_handler_middleware(request, call_next)
        assert result.media_type == "application/json"


class TestAuthMiddleware:
    async def test_passes_request_through(self):
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
