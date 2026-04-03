"""Structured JSON error middleware."""
from __future__ import annotations

import logging

from fastapi import Request, Response
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from elephantbroker.schemas.base import ErrorDetail

_logger = logging.getLogger("elephantbroker.api.errors")


async def error_handler_middleware(request: Request, call_next) -> Response:
    try:
        return await call_next(request)
    except RequestValidationError as exc:
        # Log validation errors with full detail — critical for debugging 422s
        _logger.warning("Validation error on %s %s: %s", request.method, request.url.path, exc.errors())
        return JSONResponse(status_code=422, content={"detail": exc.errors()})
    except KeyError as exc:
        detail = ErrorDetail(code="not_found", message=str(exc))
        return JSONResponse(status_code=404, content=detail.model_dump())
    except ValueError as exc:
        detail = ErrorDetail(code="validation_error", message=str(exc))
        return JSONResponse(status_code=422, content=detail.model_dump())
    except Exception as exc:
        _logger.error("Unhandled error on %s %s: %s", request.method, request.url.path, exc, exc_info=True)
        detail = ErrorDetail(code="internal_error", message=str(exc))
        return JSONResponse(status_code=500, content=detail.model_dump())
