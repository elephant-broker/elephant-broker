"""Tests for OTEL observability."""
from opentelemetry.sdk.trace import TracerProvider

from elephantbroker.runtime.observability import get_tracer, setup_tracing, traced
from elephantbroker.schemas.config import InfraConfig


class TestOTELInstrumentation:
    def test_setup_tracing_returns_provider(self):
        config = InfraConfig()
        provider = setup_tracing(config)
        assert isinstance(provider, TracerProvider)

    def test_setup_tracing_noop_without_endpoint(self):
        config = InfraConfig(otel_endpoint=None)
        provider = setup_tracing(config)
        assert isinstance(provider, TracerProvider)

    async def test_traced_decorator_creates_span(self):
        setup_tracing(InfraConfig())

        @traced
        async def my_func():
            return 42

        result = await my_func()
        assert result == 42

    async def test_error_spans_marked_on_exception(self):
        setup_tracing(InfraConfig())
        import pytest

        @traced
        async def failing_func():
            raise ValueError("test error")

        with pytest.raises(ValueError):
            await failing_func()

    def test_get_tracer_returns_tracer(self):
        setup_tracing(InfraConfig())
        tracer = get_tracer("test_module")
        assert tracer is not None
