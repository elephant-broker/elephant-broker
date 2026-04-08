"""Tests for OtelTraceQueryClient."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from elephantbroker.runtime.consolidation.otel_trace_query_client import OtelTraceQueryClient
from elephantbroker.schemas.config import ClickHouseConfig
from elephantbroker.schemas.trace import TraceEventType


class TestOtelTraceQueryClient:
    def test_not_available_when_disabled(self):
        config = ClickHouseConfig(enabled=False)
        client = OtelTraceQueryClient(config)
        assert client.available is False

    async def test_get_tool_sequences_returns_empty_when_unavailable(self):
        config = ClickHouseConfig(enabled=False)
        client = OtelTraceQueryClient(config)
        result = await client.get_tool_sequences("gw-1")
        assert result == []

    def test_close_no_error_when_no_client(self):
        config = ClickHouseConfig(enabled=False)
        client = OtelTraceQueryClient(config)
        client.close()  # Should not raise

    def test_available_property(self):
        config = ClickHouseConfig(enabled=False)
        client = OtelTraceQueryClient(config)
        assert client.available is False
        assert client._client is None


class TestOtelTraceQueryClientDegradedOps:
    """F6 (TODO-3-611): degraded-op wiring on ClickHouse failures."""

    def test_optional_constructor_args_default_to_none(self):
        """Backwards-compat: existing callers passing only the config still work."""
        client = OtelTraceQueryClient(ClickHouseConfig(enabled=False))
        assert client._trace is None
        assert client._metrics is None
        assert client._init_failure is None  # disabled config = no failure

    def test_constructor_accepts_trace_ledger_and_metrics(self):
        trace = MagicMock()
        metrics = MagicMock()
        client = OtelTraceQueryClient(
            ClickHouseConfig(enabled=False),
            trace_ledger=trace,
            metrics=metrics,
        )
        assert client._trace is trace
        assert client._metrics is metrics

    def test_import_failure_records_metric_and_stashes_reason(self, monkeypatch):
        """When clickhouse_connect import fails, metric fires + init_failure recorded."""
        import builtins
        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "clickhouse_connect":
                raise ImportError("not installed in test env")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)
        metrics = MagicMock()
        client = OtelTraceQueryClient(
            ClickHouseConfig(enabled=True, host="x", port=9000, database="x"),
            metrics=metrics,
        )
        assert client._client is None
        metrics.inc_degraded_op.assert_called_once_with(
            component="clickhouse_trace_query",
            operation="connect_import",
        )
        assert client._init_failure is not None
        assert client._init_failure[0] == "connect_import"

    async def test_init_failure_emits_one_shot_trace_event_on_first_query(self, monkeypatch):
        """The deferred trace event fires once on the first async query call, not in __init__."""
        import builtins
        real_import = builtins.__import__

        def fake_import(name, *args, **kwargs):
            if name == "clickhouse_connect":
                raise ImportError("not installed")
            return real_import(name, *args, **kwargs)

        monkeypatch.setattr(builtins, "__import__", fake_import)
        trace = MagicMock()
        trace.append_event = AsyncMock()
        client = OtelTraceQueryClient(
            ClickHouseConfig(enabled=True),
            trace_ledger=trace,
        )

        result1 = await client.get_tool_sequences("gw-1")
        result2 = await client.get_tool_sequences("gw-1")

        assert result1 == [] and result2 == []
        # Event should fire exactly once (one-shot semantics)
        assert trace.append_event.call_count == 1
        emitted_event = trace.append_event.call_args.args[0]
        assert emitted_event.event_type == TraceEventType.DEGRADED_OPERATION
        assert emitted_event.payload["component"] == "clickhouse_trace_query"
        assert emitted_event.payload["operation"] == "connect_import"
        # F6 symmetry (Bucket F-R2, TODO-3-112): init-failure payload must
        # carry gateway_id like the query-failure payload below. The test
        # passes "gw-1" to get_tool_sequences so that is what the event
        # should be stamped with.
        assert emitted_event.payload["gateway_id"] == "gw-1"

    async def test_query_failure_emits_event_and_metric(self):
        """A query exception fires both metric + degraded trace event each time."""
        config = ClickHouseConfig(enabled=False)  # avoids real connection
        trace = MagicMock()
        trace.append_event = AsyncMock()
        metrics = MagicMock()
        client = OtelTraceQueryClient(config, trace_ledger=trace, metrics=metrics)

        # Inject a fake client that raises on query
        fake_client = MagicMock()
        fake_client.query.side_effect = RuntimeError("BOOM")
        client._client = fake_client

        result = await client.get_tool_sequences("gw-1")

        assert result == []
        metrics.inc_degraded_op.assert_called_once_with(
            component="clickhouse_trace_query",
            operation="query",
        )
        trace.append_event.assert_called_once()
        emitted_event = trace.append_event.call_args.args[0]
        assert emitted_event.event_type == TraceEventType.DEGRADED_OPERATION
        assert emitted_event.payload["operation"] == "query"
        assert emitted_event.payload["gateway_id"] == "gw-1"
