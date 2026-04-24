"""Tests for OTEL observability."""
import logging
import sys
from unittest.mock import patch

import pytest
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter

from elephantbroker.runtime.observability import (
    VERBOSE,
    GatewayLoggerAdapter,
    get_tracer,
    register_verbose_level,
    setup_tracing,
    traced,
)
from elephantbroker.schemas.config import InfraConfig


@pytest.fixture
def in_memory_spans():
    """Set up an in-memory span exporter on a fresh TracerProvider.

    Installs a SimpleSpanProcessor wrapping an InMemorySpanExporter so tests
    can introspect span attributes / status / events directly after invoking
    `@traced` functions. Replaces the global provider for the test duration.
    """
    setup_tracing(InfraConfig())  # fresh provider, no external exporter
    exporter = InMemorySpanExporter()
    provider = trace.get_tracer_provider()
    if hasattr(provider, "add_span_processor"):
        provider.add_span_processor(SimpleSpanProcessor(exporter))
    yield exporter
    exporter.clear()


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

        @traced
        async def failing_func():
            raise ValueError("test error")

        with pytest.raises(ValueError):
            await failing_func()

    def test_get_tracer_returns_tracer(self):
        setup_tracing(InfraConfig())
        tracer = get_tracer("test_module")
        assert tracer is not None

    # ------------------------------------------------------------------
    # TF-FN-015 additions
    # ------------------------------------------------------------------

    def test_setup_tracing_with_endpoint_attaches_exporter(self):
        """G1 (#315): when `otel_endpoint` is set, `setup_tracing` constructs an
        OTLPSpanExporter with that endpoint and attaches it via SimpleSpanProcessor.

        We patch the exporter class at its source module so we don't need the real
        package installed; asserting the class was called with the right endpoint
        proves the code path ran (vs being silently skipped on ImportError).
        """
        with patch(
            "opentelemetry.exporter.otlp.proto.grpc.trace_exporter.OTLPSpanExporter"
        ) as mock_exporter:
            provider = setup_tracing(InfraConfig(otel_endpoint="http://localhost:4317"))
        assert isinstance(provider, TracerProvider)
        mock_exporter.assert_called_once_with(endpoint="http://localhost:4317")

    def test_setup_tracing_resource_has_gateway_id(self):
        """G2 (#580): the TracerProvider's Resource carries gateway.id + service.name
        attributes so downstream OTEL collectors and Jaeger can filter by gateway.
        """
        provider = setup_tracing(InfraConfig(), gateway_id="gw-prod")
        attrs = dict(provider.resource.attributes)
        assert attrs.get("gateway.id") == "gw-prod"
        assert attrs.get("service.name") == "elephantbroker"

    def test_setup_tracing_otlp_package_missing_logs_warning(self, caplog):
        """G3: if `opentelemetry-exporter-otlp-proto-grpc` is not installed, the
        import in `setup_tracing` raises ImportError which is caught and logged
        at WARNING. Config with endpoint still succeeds; traces just don't export.
        """
        # Force the import to fail by stubbing the module entry as None.
        with patch.dict(sys.modules, {
            "opentelemetry.exporter.otlp.proto.grpc.trace_exporter": None,
        }):
            with caplog.at_level(logging.WARNING, logger="elephantbroker.observability"):
                provider = setup_tracing(InfraConfig(otel_endpoint="http://localhost:4317"))
        assert isinstance(provider, TracerProvider)
        assert "opentelemetry-exporter-otlp-proto-grpc" in caplog.text

    async def test_traced_span_has_module_and_method_attrs(self, in_memory_spans):
        """G4 (#314): `@traced` spans carry `module` + `method` attributes so
        operators can filter OTEL traces by originating function.
        """
        @traced
        async def some_func():
            return 1

        await some_func()
        spans = in_memory_spans.get_finished_spans()
        assert len(spans) == 1
        # `method` is fn.__name__; `module` is fn.__module__.
        assert spans[0].attributes["method"] == "some_func"
        assert spans[0].attributes["module"] == some_func.__wrapped__.__module__

    async def test_traced_extracts_identity_kwargs_to_span(self, in_memory_spans):
        """G5 (#314 / #580): `@traced` harvests identity kwargs (gateway_id,
        agent_key, agent_id, session_id, session_key) into span attributes so
        Jaeger-style search can locate spans by any identity dimension.
        """
        @traced
        async def my_func(**kwargs):
            return 1

        await my_func(
            gateway_id="gw-a",
            agent_key="gw-a:main",
            session_id="sid-123",
        )
        spans = in_memory_spans.get_finished_spans()
        assert len(spans) == 1
        attrs = spans[0].attributes
        assert attrs["gateway_id"] == "gw-a"
        assert attrs["agent_key"] == "gw-a:main"
        assert attrs["session_id"] == "sid-123"

    async def test_traced_span_error_status_recorded(self, in_memory_spans):
        """G6 (#314): on raise, `@traced` sets span status to ERROR and records
        the exception as a span event so failures are observable without logs.
        """
        @traced
        async def failing():
            raise ValueError("boom")

        with pytest.raises(ValueError):
            await failing()
        spans = in_memory_spans.get_finished_spans()
        assert len(spans) == 1
        assert spans[0].status.status_code == trace.StatusCode.ERROR
        exception_events = [e for e in spans[0].events if e.name == "exception"]
        assert exception_events, "Expected at least one 'exception' span event"
        assert exception_events[0].attributes["exception.type"] == "ValueError"

    def test_register_verbose_level_creates_level_15(self):
        """G7 (#324): `register_verbose_level` registers logging level 15 between
        DEBUG (10) and INFO (20), and attaches `.verbose()` to Logger instances.
        """
        register_verbose_level()
        assert VERBOSE == 15
        assert logging.DEBUG < VERBOSE < logging.INFO
        test_logger = logging.getLogger("test-verbose-handler")
        assert hasattr(test_logger, "verbose")

    def test_gateway_logger_adapter_prefix_both_populated(self):
        """G8-a (#325a): when both `gateway_id` and `agent_key` extras are set,
        the log message is prefixed with `[gw][agent_key]`.
        """
        adapter = GatewayLoggerAdapter(
            logging.getLogger("tf-fn-015-g8a"),
            {"gateway_id": "gw", "agent_key": "gw:main"},
        )
        msg, _ = adapter.process("hello", {})
        assert msg == "[gw][gw:main] hello"

    def test_gateway_logger_adapter_prefix_empty_agent_key(self):
        """G8-b (#325b): when only `gateway_id` is set (empty `agent_key`),
        the prefix is `[gw]` only — agent key bracket is omitted.
        """
        adapter = GatewayLoggerAdapter(
            logging.getLogger("tf-fn-015-g8b"),
            {"gateway_id": "gw", "agent_key": ""},
        )
        msg, _ = adapter.process("hello", {})
        assert msg == "[gw] hello"

    def test_gateway_logger_adapter_no_prefix_when_both_empty(self):
        """G8-c (#325c): when both extras are empty, the message passes through
        unmodified — no empty `[][]` cosmetic artifact.
        """
        adapter = GatewayLoggerAdapter(
            logging.getLogger("tf-fn-015-g8c"),
            {"gateway_id": "", "agent_key": ""},
        )
        msg, _ = adapter.process("hello", {})
        assert msg == "hello"

    async def test_traced_does_not_extract_self_gateway_id_documented_prod_risk(self, in_memory_spans):
        """Pins documented PROD risk #1510 — `@traced` extracts identity ONLY from
        kwargs. Methods that stash `self._gateway_id` (e.g., MemoryStoreFacade)
        produce spans WITHOUT the `gateway_id` attribute when they don't pass it
        as a kwarg.

        If `@traced` is updated to fall back to reading `self._gateway_id`, update
        this test, the TF-FN-015 plan, and file a TD.
        """
        class MyModule:
            def __init__(self) -> None:
                self._gateway_id = "gw-stashed-on-self"

            @traced
            async def method(self):
                return 1

        instance = MyModule()
        await instance.method()  # NO kwargs supplied
        spans = in_memory_spans.get_finished_spans()
        assert len(spans) == 1
        # `@traced` reads kwargs only; `self._gateway_id` is invisible to it.
        assert "gateway_id" not in spans[0].attributes
