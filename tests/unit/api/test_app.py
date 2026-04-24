"""Tests for create_app() factory."""
from unittest.mock import AsyncMock, MagicMock

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
