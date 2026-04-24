"""OpenTelemetry instrumentation — tracing setup, helpers, and gateway-aware logging."""
from __future__ import annotations

import functools
import logging
from collections.abc import Callable
from typing import Any, TypeVar

from opentelemetry import trace
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.trace import StatusCode, Tracer

from elephantbroker.schemas.config import InfraConfig

F = TypeVar("F", bound=Callable[..., Any])

_provider: TracerProvider | None = None

VERBOSE = 15


def register_verbose_level() -> None:
    """Register custom VERBOSE logging level (15, between DEBUG and INFO)."""
    logging.addLevelName(VERBOSE, "VERBOSE")

    def verbose(self: logging.Logger, message: str, *args: Any, **kws: Any) -> None:
        if self.isEnabledFor(VERBOSE):
            self._log(VERBOSE, message, args, **kws)

    logging.Logger.verbose = verbose  # type: ignore[attr-defined]


def setup_tracing(config: InfraConfig, gateway_id: str = "") -> TracerProvider:
    """Configure OTEL tracing with gateway identity resource attributes."""
    global _provider

    resource = Resource.create({
        "service.name": "elephantbroker",
        "gateway.id": gateway_id,
    })
    provider = TracerProvider(resource=resource)

    if config.otel_endpoint:
        try:
            from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter

            exporter = OTLPSpanExporter(endpoint=config.otel_endpoint)
            provider.add_span_processor(SimpleSpanProcessor(exporter))
        except ImportError:
            logging.getLogger("elephantbroker.observability").warning(
                "OTEL endpoint configured (%s) but opentelemetry-exporter-otlp-proto-grpc "
                "is not installed. Traces will not be exported. Install with: "
                "pip install opentelemetry-exporter-otlp-proto-grpc",
                config.otel_endpoint,
            )

    trace.set_tracer_provider(provider)
    _provider = provider
    return provider


def setup_otel_logging(config: InfraConfig, gateway_id: str = ""):
    """Configure OTEL LoggerProvider for TraceLedger event export to ClickHouse.

    Returns ``(Logger, LoggerProvider)`` tuple if configured, ``None``
    otherwise. The TraceLedger uses the Logger to emit LogRecords
    alongside in-memory storage; the caller (``RuntimeContainer.from_config``)
    retains the LoggerProvider so ``container.close()`` can call
    ``provider.shutdown()`` on SIGTERM and flush the BatchLogRecordProcessor
    buffer before the pod exits (#1181 RESOLVED — TF-FN-019 G11).

    Requires ``EB_OTEL_ENDPOINT`` and ``EB_TRACE_OTEL_LOGS_ENABLED=true``.
    """
    if not config.otel_endpoint:
        return None
    trace_cfg = getattr(config, "trace", None)
    if not trace_cfg or not trace_cfg.otel_logs_enabled:
        return None
    try:
        from opentelemetry.exporter.otlp.proto.grpc._log_exporter import OTLPLogExporter
        from opentelemetry.sdk._logs import LoggerProvider
        from opentelemetry.sdk._logs.export import BatchLogRecordProcessor

        resource = Resource.create({
            "service.name": "elephantbroker",
            "gateway.id": gateway_id,
        })
        provider = LoggerProvider(resource=resource)
        exporter = OTLPLogExporter(endpoint=config.otel_endpoint)
        provider.add_log_record_processor(BatchLogRecordProcessor(exporter))
        return provider.get_logger("elephantbroker.trace"), provider
    except ImportError:
        logging.getLogger("elephantbroker.observability").warning(
            "OTEL endpoint configured (%s) but OTEL log exporter is not installed. "
            "Trace events will not be exported to ClickHouse.",
            config.otel_endpoint,
        )
        return None


def get_tracer(module_name: str) -> Tracer:
    """Return a module-scoped tracer."""
    return trace.get_tracer(f"elephantbroker.{module_name}")


def traced(fn: F) -> F:
    """Async decorator that wraps a function in an OTEL span.

    Extracts gateway identity from kwargs into span attributes.
    Sets span status to ERROR on exception.
    """
    module = fn.__module__ or "unknown"
    name = fn.__qualname__ or fn.__name__

    @functools.wraps(fn)
    async def wrapper(*args: Any, **kwargs: Any) -> Any:
        tracer = get_tracer(module)
        with tracer.start_as_current_span(name) as span:
            span.set_attribute("module", module)
            span.set_attribute("method", fn.__name__)
            # Extract identity attributes
            for attr_name in ("session_id", "gateway_id", "agent_key", "agent_id", "session_key"):
                val = kwargs.get(attr_name)
                if val is not None:
                    span.set_attribute(attr_name, str(val))
            try:
                return await fn(*args, **kwargs)
            except Exception as exc:
                span.set_status(StatusCode.ERROR, str(exc))
                span.record_exception(exc)
                raise

    return wrapper  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Gateway-aware log adapter
# ---------------------------------------------------------------------------

class GatewayLoggerAdapter(logging.LoggerAdapter):
    """Prepends ``[gateway_id][agent_key]`` to all log messages."""

    def process(self, msg: str, kwargs: Any) -> tuple[str, Any]:
        gw = self.extra.get("gateway_id", "")
        ak = self.extra.get("agent_key", "")
        prefix = f"[{gw}]" if gw else ""
        if ak:
            prefix += f"[{ak}]"
        if prefix:
            msg = f"{prefix} {msg}"
        return msg, kwargs
