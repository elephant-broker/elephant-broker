"""FastAPI application factory."""
from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from elephantbroker.api.middleware.auth import AuthMiddleware
from elephantbroker.api.middleware.errors import error_handler_middleware
from elephantbroker.api.middleware.gateway import GatewayIdentityMiddleware
from elephantbroker.api.routes import (
    actors,
    admin,
    artifacts,
    claims,
    consolidation,
    context,
    goals,
    guards,
    health,
    memory,
    metrics,
    procedures,
    profiles,
    rerank,
    sessions,
    stats,
    trace,
    working_set,
)
from elephantbroker.runtime.container import RuntimeContainer


def create_app(container: RuntimeContainer) -> FastAPI:
    """Create FastAPI app with all routes and middleware.

    Accepts a pre-built RuntimeContainer so tests can inject mocked adapters.
    """
    # #1508 / F2 fix (TD-65 2nd follow-up): register container.close() on FastAPI
    # shutdown via lifespan context manager. Previously no @app.on_event("shutdown")
    # or lifespan= kwarg existed, so container.close() was never invoked on SIGTERM
    # and the 14 "Closing adapter: ..." INFO logs in container.py:755-813 were dead
    # code (devops Layer B/C verified 0 hits across a full day's shutdowns). Now:
    # TestClient (and uvicorn in prod) trigger lifespan startup + shutdown
    # automatically, driving close() → Redis locks released, adapter connections
    # torn down cleanly, F2 logs actually emit. See IMPLEMENTED-PR-7-merge.md for
    # the discovery cycle.
    @asynccontextmanager
    async def lifespan(_app: FastAPI):
        # No startup actions — container is constructed before create_app() runs.
        yield
        await container.close()

    app = FastAPI(
        title="ElephantBroker",
        version="0.4.0",
        description="Unified Cognitive Runtime",
        lifespan=lifespan,
    )
    app.state.container = container

    # Middleware (applied in reverse order — gateway runs first)
    app.add_middleware(AuthMiddleware)
    # Middleware fallback must equal the container's gateway_id exactly. Bucket A
    # (commit d850186) changed GatewayConfig.gateway_id default from "local" to ""
    # and added the EB_ALLOW_DEFAULT_GATEWAY_ID opt-out. The prior shim here
    # re-fabricated "local" as the middleware fallback when config was empty,
    # which caused a store/lookup mismatch: write paths stamped DataPoints with
    # gateway_id="local" (from the middleware default) while read paths used the
    # engine's construction-time gateway_id="" (from the config), and the strict
    # Cypher filter rejected the mismatch. Passing the config value through
    # unchanged keeps both sides byte-identical.
    default_gw = ""
    if hasattr(container, "config") and container.config and hasattr(container.config, "gateway"):
        default_gw = container.config.gateway.gateway_id
    app.add_middleware(GatewayIdentityMiddleware, default_gateway_id=default_gw)
    app.middleware("http")(error_handler_middleware)

    # Log validation errors with full detail (helps debug 422s from plugins)
    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(request: Request, exc: RequestValidationError):
        logger = logging.getLogger("elephantbroker.api")
        logger.warning("Validation error on %s %s: %s", request.method, request.url.path, exc.errors())
        return JSONResponse(status_code=422, content={"detail": exc.errors()})

    # OTEL instrumentation (additive, no-op without endpoint)
    try:
        from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
        FastAPIInstrumentor.instrument_app(app)
    except Exception:
        pass

    # Routes
    app.include_router(health.router, prefix="/health", tags=["health"])
    app.include_router(memory.router, prefix="/memory", tags=["memory"])
    app.include_router(context.router, prefix="/context", tags=["context"])
    app.include_router(actors.router, prefix="/actors", tags=["actors"])
    app.include_router(goals.router, prefix="/goals", tags=["goals"])
    app.include_router(procedures.router, prefix="/procedures", tags=["procedures"])
    app.include_router(claims.router, prefix="/claims", tags=["claims"])
    app.include_router(artifacts.router, prefix="/artifacts", tags=["artifacts"])
    app.include_router(profiles.router, prefix="/profiles", tags=["profiles"])
    app.include_router(trace.router, prefix="/trace", tags=["trace"])
    app.include_router(stats.router, prefix="/stats", tags=["stats"])
    app.include_router(sessions.router, prefix="/sessions", tags=["sessions"])
    app.include_router(working_set.router, prefix="/working-set", tags=["working-set"])
    app.include_router(rerank.router, prefix="/rerank", tags=["rerank"])
    app.include_router(guards.router, tags=["guards"])
    app.include_router(consolidation.router, prefix="/consolidation", tags=["consolidation"])
    app.include_router(metrics.router, tags=["metrics"])
    app.include_router(admin.router, prefix="/admin", tags=["admin"])

    return app
