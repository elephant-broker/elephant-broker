"""Tests for TraceLedger."""
import uuid

from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.schemas.trace import TraceEvent, TraceEventType, TraceQuery


class TestTraceLedger:
    async def test_append_event(self):
        ledger = TraceLedger()
        event = TraceEvent(event_type=TraceEventType.INPUT_RECEIVED)
        result = await ledger.append_event(event)
        assert result.id == event.id

    async def test_query_by_session(self):
        ledger = TraceLedger()
        sid = uuid.uuid4()
        await ledger.append_event(TraceEvent(event_type=TraceEventType.INPUT_RECEIVED, session_id=sid))
        await ledger.append_event(TraceEvent(event_type=TraceEventType.INPUT_RECEIVED, session_id=uuid.uuid4()))
        results = await ledger.query_trace(TraceQuery(session_id=sid))
        assert len(results) == 1

    async def test_query_by_event_type(self):
        ledger = TraceLedger()
        await ledger.append_event(TraceEvent(event_type=TraceEventType.INPUT_RECEIVED))
        await ledger.append_event(TraceEvent(event_type=TraceEventType.CLAIM_MADE))
        results = await ledger.query_trace(TraceQuery(event_types=[TraceEventType.CLAIM_MADE]))
        assert len(results) == 1

    async def test_events_are_ordered(self):
        ledger = TraceLedger()
        e1 = TraceEvent(event_type=TraceEventType.INPUT_RECEIVED)
        e2 = TraceEvent(event_type=TraceEventType.CLAIM_MADE)
        await ledger.append_event(e1)
        await ledger.append_event(e2)
        results = await ledger.query_trace(TraceQuery())
        assert results[0].id == e1.id
        assert results[1].id == e2.id

    async def test_get_evidence_chain(self):
        ledger = TraceLedger()
        target = uuid.uuid4()
        await ledger.append_event(TraceEvent(event_type=TraceEventType.CLAIM_MADE, claim_ids=[target]))
        await ledger.append_event(TraceEvent(event_type=TraceEventType.INPUT_RECEIVED))
        chain = await ledger.get_evidence_chain(target)
        assert len(chain) == 1

    async def test_query_multiple_filters(self):
        ledger = TraceLedger()
        sid = uuid.uuid4()
        await ledger.append_event(TraceEvent(event_type=TraceEventType.INPUT_RECEIVED, session_id=sid))
        await ledger.append_event(TraceEvent(event_type=TraceEventType.CLAIM_MADE, session_id=sid))
        await ledger.append_event(TraceEvent(event_type=TraceEventType.INPUT_RECEIVED, session_id=uuid.uuid4()))
        results = await ledger.query_trace(TraceQuery(
            session_id=sid,
            event_types=[TraceEventType.INPUT_RECEIVED],
        ))
        assert len(results) == 1

    async def test_query_timestamp_range(self):
        from datetime import timedelta
        ledger = TraceLedger()
        e1 = TraceEvent(event_type=TraceEventType.INPUT_RECEIVED)
        e2 = TraceEvent(event_type=TraceEventType.INPUT_RECEIVED)
        e3 = TraceEvent(event_type=TraceEventType.INPUT_RECEIVED)
        # Override timestamps
        now = e2.timestamp
        e1.timestamp = now - timedelta(hours=2)
        e3.timestamp = now + timedelta(hours=2)
        await ledger.append_event(e1)
        await ledger.append_event(e2)
        await ledger.append_event(e3)
        results = await ledger.query_trace(TraceQuery(
            from_timestamp=now - timedelta(minutes=1),
            to_timestamp=now + timedelta(minutes=1),
        ))
        assert len(results) == 1
        assert results[0].id == e2.id

    async def test_query_offset_and_limit(self):
        ledger = TraceLedger()
        for _ in range(3):
            await ledger.append_event(TraceEvent(event_type=TraceEventType.INPUT_RECEIVED))
        results = await ledger.query_trace(TraceQuery(offset=1, limit=1))
        assert len(results) == 1
