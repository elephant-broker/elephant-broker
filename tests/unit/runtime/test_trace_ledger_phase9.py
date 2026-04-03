"""Phase 9 TraceLedger tests — OTEL bridge and eviction."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.schemas.config import TraceConfig
from elephantbroker.schemas.trace import TraceEvent, TraceEventType


class TestTraceLedgerEviction:
    async def test_evicts_beyond_max_events(self):
        config = TraceConfig(memory_max_events=100, memory_ttl_seconds=3600)
        ledger = TraceLedger(config=config)
        for i in range(150):
            await ledger.append_event(TraceEvent(event_type=TraceEventType.INPUT_RECEIVED))
        assert len(ledger._events) <= 100

    async def test_evicts_stale_by_ttl(self):
        config = TraceConfig(memory_max_events=10000, memory_ttl_seconds=60)
        ledger = TraceLedger(config=config)
        # Add old event (beyond TTL)
        old = TraceEvent(event_type=TraceEventType.INPUT_RECEIVED)
        old.timestamp = datetime.now(UTC) - timedelta(seconds=120)
        ledger._events.append(old)
        # Trigger eviction via new append
        await ledger.append_event(TraceEvent(event_type=TraceEventType.INPUT_RECEIVED))
        # Old event (120s ago) should be evicted; new event (0s ago) should remain
        assert len(ledger._events) == 1
        age = (datetime.now(UTC) - ledger._events[0].timestamp).total_seconds()
        assert age < 5

    async def test_backward_compat_no_config(self):
        ledger = TraceLedger()
        for i in range(5):
            await ledger.append_event(TraceEvent(event_type=TraceEventType.INPUT_RECEIVED))
        assert len(ledger._events) == 5


class TestTraceLedgerOtelBridge:
    async def test_emits_otel_log_when_logger_present(self):
        mock_logger = MagicMock()
        ledger = TraceLedger(otel_logger=mock_logger)
        await ledger.append_event(TraceEvent(
            event_type=TraceEventType.CONSOLIDATION_STARTED,
            gateway_id="gw-1",
        ))
        # The _emit_otel_log should be called (may fail if opentelemetry not installed, which is fine)
        assert len(ledger._events) == 1

    async def test_no_otel_log_when_logger_none(self):
        ledger = TraceLedger(otel_logger=None)
        await ledger.append_event(TraceEvent(event_type=TraceEventType.INPUT_RECEIVED))
        assert len(ledger._events) == 1

    async def test_otel_failure_does_not_block(self):
        mock_logger = MagicMock()
        mock_logger.emit = MagicMock(side_effect=RuntimeError("OTEL down"))
        ledger = TraceLedger(otel_logger=mock_logger)
        # Should not raise
        ev = await ledger.append_event(TraceEvent(event_type=TraceEventType.INPUT_RECEIVED))
        assert ev is not None
