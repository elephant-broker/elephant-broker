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
    except PermissionError as exc:
        # #1170 RESOLVED (R2-P5): PermissionError now maps to 403 in the
        # middleware fallback path, matching the route-level handlers
        # (memory.py promote_scope/promote_class/update/delete) that
        # already explicitly catch PermissionError and return 403. Pre-fix
        # the middleware fell through to the generic Exception handler
        # and returned 500 — pinned in TF-FN-014 G6 as documented gap.
        # Cross-gateway facade rejections + any future tenant-isolation
        # raises now surface as 403 regardless of whether the route
        # caught them locally or let them propagate to the middleware.
        detail = ErrorDetail(code="forbidden", message=str(exc))
        return JSONResponse(status_code=403, content=detail.model_dump())
    except ValueError as exc:
        detail = ErrorDetail(code="validation_error", message=str(exc))
        return JSONResponse(status_code=422, content=detail.model_dump())
    except Exception as exc:
        _logger.error("Unhandled error on %s %s: %s", request.method, request.url.path, exc, exc_info=True)
        detail = ErrorDetail(code="internal_error", message=str(exc))
        return JSONResponse(status_code=500, content=detail.model_dump())
