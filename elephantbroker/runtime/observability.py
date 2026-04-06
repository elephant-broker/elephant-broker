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


def setup_tracing(config: InfraConfig, gateway_id: str = "local") -> TracerProvider:
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


def setup_otel_logging(config: InfraConfig, gateway_id: str = "local"):
    """Configure OTEL LoggerProvider for TraceLedger event export to ClickHouse.

    Returns an OTEL Logger instance if configured, None otherwise.
    The TraceLedger uses this to emit LogRecords alongside in-memory storage.
    Requires EB_OTEL_ENDPOINT and EB_TRACE_OTEL_LOGS_ENABLED=true.
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
        return provider.get_logger("elephantbroker.trace")
    except ImportError:
        logging.getLogger("elephantbroker.observability").warning(
            "OTEL endpoint configured (%s) but OTEL log exporter is not installed. "
            "Trace events will not be exported to ClickHouse.",
            config.otel_endpoint,
        )
        return None


class GatewayLogFilter(logging.Filter):
    """Injects ``gateway_id`` into every LogRecord for structured log parsing."""

    def __init__(self, gateway_id: str) -> None:
        super().__init__()
        self._gateway_id = gateway_id

    def filter(self, record: logging.LogRecord) -> bool:
        record.gateway_id = self._gateway_id  # type: ignore[attr-defined]
        # Inject active OTEL trace/span IDs if available
        try:
            span = trace.get_current_span()
            ctx = span.get_span_context()
            record.trace_id = format(ctx.trace_id, "032x") if ctx.is_valid else ""  # type: ignore[attr-defined]
            record.span_id = format(ctx.span_id, "016x") if ctx.is_valid else ""  # type: ignore[attr-defined]
        except Exception:
            record.trace_id = ""  # type: ignore[attr-defined]
            record.span_id = ""  # type: ignore[attr-defined]
        return True


def setup_json_logging(config: InfraConfig, gateway_id: str = "local") -> None:
    """Configure root logger with structured JSON output.

    Uses python-json-logger when log_format='json' (production default).
    Falls back to plain text for local dev (log_format='text').
    Injects gateway_id, trace_id, span_id into every record.
    """
    log_format = getattr(config, "log_format", "json")
    level_name = config.log_level.upper()
    log_level = VERBOSE if level_name == "VERBOSE" else getattr(logging, level_name, logging.INFO)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    # Remove existing handlers so we own the full handler chain
    root_logger.handlers.clear()

    gw_filter = GatewayLogFilter(gateway_id)

    if log_format == "json":
        try:
            from pythonjsonlogger.json import JsonFormatter
            handler = logging.StreamHandler()
            fmt = JsonFormatter(
                fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
                rename_fields={"asctime": "timestamp", "levelname": "level", "name": "logger"},
            )
            handler.setFormatter(fmt)
            handler.addFilter(gw_filter)
            root_logger.addHandler(handler)
            return
        except ImportError:
            logging.getLogger("elephantbroker.observability").warning(
                "python-json-logger not installed; falling back to text logging. "
                "Install with: pip install python-json-logger"
            )

    # Plain text fallback
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s"))
    handler.addFilter(gw_filter)
    root_logger.addHandler(handler)


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
# Gateway-aware log adapter (kept for backward compatibility)
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
