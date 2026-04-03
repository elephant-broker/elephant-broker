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

    def test_middleware_registered(self):
        app = create_app(_make_container())
        # The error_handler_middleware is registered via app.middleware("http"),
        # which wraps it in a BaseHTTPMiddleware. Check the middleware stack.
        middleware_classes = [m.cls.__name__ if hasattr(m, "cls") else str(m) for m in app.user_middleware]
        assert any("Auth" in name for name in middleware_classes), f"AuthMiddleware not found in {middleware_classes}"
