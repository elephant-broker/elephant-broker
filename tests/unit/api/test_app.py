"""Tests for create_app() factory."""
import sys
from unittest.mock import AsyncMock, MagicMock, patch

from fastapi import FastAPI

from elephantbroker.api.app import create_app
from elephantbroker.runtime.container import RuntimeContainer


def _make_container():
    c = RuntimeContainer()
    # All modules set to MagicMock so routes don't crash on attribute access
    for attr in vars(c):
        if not attr.startswith("_") and getattr(c, attr) is None:
            setattr(c, attr, MagicMock())
    return c


class TestCreateApp:
    def test_returns_fastapi_instance(self):
        app = create_app(_make_container())
        assert isinstance(app, FastAPI)

    def test_title_and_version(self):
        app = create_app(_make_container())
        assert app.title == "ElephantBroker"
        assert app.version == "0.4.0"

    def test_container_attached_to_state(self):
        container = _make_container()
        app = create_app(container)
        assert app.state.container is container

    def test_all_11_route_prefixes_registered(self):
        app = create_app(_make_container())
        paths = {route.path for route in app.routes if hasattr(route, "path")}
        expected_prefixes = [
            "/health", "/memory", "/context", "/actors", "/goals",
            "/procedures", "/claims", "/artifacts", "/profiles", "/trace", "/stats",
        ]
        for prefix in expected_prefixes:
            assert any(p.startswith(prefix) for p in paths), f"Missing prefix: {prefix}"

    def test_lifespan_invokes_container_close_on_shutdown(self):
        """#1508 / F2 fix (TD-65 2nd follow-up): `create_app()` registers
        `container.close()` via the FastAPI `lifespan=` kwarg. When the app
        shuts down (SIGTERM in prod; TestClient `__exit__` in tests), the
        lifespan's yielded teardown runs, driving `container.close()` —
        which in turn emits the 14 F2 close-adapter INFO logs.

        Pre-fix: there was no `@app.on_event("shutdown")` AND no
        `lifespan=` kwarg — `container.close()` was never invoked in
        production (devops Layer B/C verified via journal grep). Redis
        distributed locks orphaned on pod restart; adapter connections
        torn down only via process exit, not graceful close.

        Post-fix: this test drives the lifespan via TestClient context
        manager and asserts `container.close` was awaited once.
        """
        from starlette.testclient import TestClient

        container = _make_container()
        container.close = AsyncMock()
        app = create_app(container)
        # TestClient as context manager triggers lifespan startup on __enter__
        # and shutdown on __exit__. Our lifespan yields immediately (no startup
        # work) and awaits container.close() on the post-yield side.
        with TestClient(app):
            pass
        container.close.assert_awaited_once()

    def test_middleware_registered(self):
        app = create_app(_make_container())
        # The error_handler_middleware is registered via app.middleware("http"),
        # which wraps it in a BaseHTTPMiddleware. Check the middleware stack.
        middleware_classes = [m.cls.__name__ if hasattr(m, "cls") else str(m) for m in app.user_middleware]
        assert any("Auth" in name for name in middleware_classes), f"AuthMiddleware not found in {middleware_classes}"

    def test_middleware_execution_order(self):
        """G4 (#306): GatewayIdentityMiddleware must run BEFORE AuthMiddleware on the
        request path so Auth sees request.state.gateway_id already stamped.

        Starlette's ``add_middleware`` INSERTS at position 0, so later-added middleware
        sits at a LOWER index in ``user_middleware``. The middleware stack is built by
        iterating the list front-to-back (wrapping inside-out), which makes the FIRST
        entry the OUTERMOST wrapper — i.e., the first to see an incoming request.

        Given app.py order: ``add_middleware(Auth)`` then ``add_middleware(Gateway)`` →
        ``user_middleware`` = ``[BaseHTTP(error_handler), Gateway, Auth]`` (BaseHTTP
        added last via ``app.middleware("http")`` also inserts at 0). Execution order:
        BaseHTTP → Gateway → Auth → route. The mechanical condition for Gateway-first
        execution is therefore ``gateway_idx < auth_idx`` in the user_middleware list.
        """
        app = create_app(_make_container())
        classes = [m.cls.__name__ for m in app.user_middleware]
        gateway_idx = classes.index("GatewayIdentityMiddleware")
        auth_idx = classes.index("AuthMiddleware")
        assert gateway_idx < auth_idx, (
            f"GatewayIdentityMiddleware must appear BEFORE AuthMiddleware in user_middleware "
            f"(lower index = runs sooner on request). Current order: {classes}"
        )

    def test_otel_instrumentation_silent_skip_when_unavailable(self):
        """G5 (#308): If opentelemetry-instrumentation-fastapi is not installed (or its
        module is mocked unavailable), create_app() must not raise — the try/except block
        in app.py:68-73 silently skips OTEL setup.

        Pins the graceful-degradation contract: operators who don't install OTEL extras
        should still get a working app, just without auto-instrumentation.
        """
        with patch.dict(sys.modules, {"opentelemetry.instrumentation.fastapi": None}):
            app = create_app(_make_container())
            assert isinstance(app, FastAPI)
