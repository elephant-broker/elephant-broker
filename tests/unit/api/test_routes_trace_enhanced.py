"""Enhanced tests for trace routes — Phase 10.

Covers: POST /trace/query, GET /trace/session/{id}/timeline,
GET /trace/session/{id}/summary, GET /trace/event-types,
and the group_events_by_turn helper.
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta

import pytest

from elephantbroker.api.routes.trace import group_events_by_turn
from elephantbroker.schemas.trace import TraceEvent, TraceEventType


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ev(
    event_type: TraceEventType,
    session_id: uuid.UUID | None = None,
    ts_offset_sec: float = 0,
    *,
    actor_ids: list[uuid.UUID] | None = None,
    session_key: str | None = None,
    gateway_id: str | None = "local",
) -> TraceEvent:
    """Build a TraceEvent with a timestamp offset from the current time.

    Defaults gateway_id to "local" to match the GatewayIdentityMiddleware
    default — routes enforce gateway isolation so events must carry a gateway_id
    that matches request.state.gateway_id.

    Uses datetime.now(UTC) as base so events are not evicted by
    TraceLedger's TTL-based stale eviction (default 1 hour).
    """
    base = datetime.now(UTC)
    return TraceEvent(
        event_type=event_type,
        session_id=session_id,
        timestamp=base + timedelta(seconds=ts_offset_sec),
        actor_ids=actor_ids or [],
        session_key=session_key,
        gateway_id=gateway_id,
    )


async def _seed_events(container, events: list[TraceEvent]) -> None:
    """Seed trace events directly into the container's ledger."""
    for ev in events:
        await container.trace_ledger.append_event(ev)


# ---------------------------------------------------------------------------
# POST /trace/query
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestTraceQueryEndpoint:
    """Tests for POST /trace/query"""

    async def test_filter_by_single_event_type(self, client, container):
        sid = uuid.uuid4()
        await _seed_events(container, [
            _ev(TraceEventType.FACT_EXTRACTED, sid, 0),
            _ev(TraceEventType.RETRIEVAL_PERFORMED, sid, 1),
            _ev(TraceEventType.FACT_EXTRACTED, sid, 2),
        ])
        r = await client.post("/trace/query", json={
            "session_id": str(sid),
            "event_types": ["fact_extracted"],
        })
        assert r.status_code == 200
        data = r.json()
        assert len(data) == 2
        assert all(e["event_type"] == "fact_extracted" for e in data)

    async def test_filter_by_multiple_event_types(self, client, container):
        sid = uuid.uuid4()
        await _seed_events(container, [
            _ev(TraceEventType.FACT_EXTRACTED, sid, 0),
            _ev(TraceEventType.RETRIEVAL_PERFORMED, sid, 1),
            _ev(TraceEventType.GUARD_TRIGGERED, sid, 2),
        ])
        r = await client.post("/trace/query", json={
            "session_id": str(sid),
            "event_types": ["fact_extracted", "guard_triggered"],
        })
        assert r.status_code == 200
        data = r.json()
        assert len(data) == 2
        types = {e["event_type"] for e in data}
        assert types == {"fact_extracted", "guard_triggered"}

    async def test_filter_by_time_range(self, client, container):
        sid = uuid.uuid4()
        # Use current time as base — must match _ev() which uses datetime.now(UTC)
        base = datetime.now(UTC)
        await _seed_events(container, [
            _ev(TraceEventType.FACT_EXTRACTED, sid, 0),
            _ev(TraceEventType.FACT_EXTRACTED, sid, 60),
            _ev(TraceEventType.FACT_EXTRACTED, sid, 120),
        ])
        r = await client.post("/trace/query", json={
            "session_id": str(sid),
            "from_timestamp": (base + timedelta(seconds=30)).isoformat(),
            "to_timestamp": (base + timedelta(seconds=90)).isoformat(),
        })
        assert r.status_code == 200
        data = r.json()
        assert len(data) == 1

    async def test_pagination_offset_limit(self, client, container):
        sid = uuid.uuid4()
        await _seed_events(container, [
            _ev(TraceEventType.FACT_EXTRACTED, sid, i) for i in range(10)
        ])
        r = await client.post("/trace/query", json={
            "session_id": str(sid),
            "offset": 3,
            "limit": 2,
        })
        assert r.status_code == 200
        data = r.json()
        assert len(data) == 2

    async def test_filter_by_actor_ids(self, client, container):
        sid = uuid.uuid4()
        actor1 = uuid.uuid4()
        actor2 = uuid.uuid4()
        await _seed_events(container, [
            _ev(TraceEventType.FACT_EXTRACTED, sid, 0, actor_ids=[actor1]),
            _ev(TraceEventType.FACT_EXTRACTED, sid, 1, actor_ids=[actor2]),
            _ev(TraceEventType.FACT_EXTRACTED, sid, 2, actor_ids=[actor1, actor2]),
        ])
        r = await client.post("/trace/query", json={
            "session_id": str(sid),
            "actor_ids": [str(actor1)],
        })
        assert r.status_code == 200
        data = r.json()
        # actor1 appears in event 0 and event 2
        assert len(data) == 2

    async def test_combined_filters(self, client, container):
        sid = uuid.uuid4()
        actor = uuid.uuid4()
        await _seed_events(container, [
            _ev(TraceEventType.FACT_EXTRACTED, sid, 0, actor_ids=[actor]),
            _ev(TraceEventType.RETRIEVAL_PERFORMED, sid, 1, actor_ids=[actor]),
            _ev(TraceEventType.FACT_EXTRACTED, sid, 2),
        ])
        r = await client.post("/trace/query", json={
            "session_id": str(sid),
            "event_types": ["fact_extracted"],
            "actor_ids": [str(actor)],
        })
        assert r.status_code == 200
        data = r.json()
        assert len(data) == 1
        assert data[0]["event_type"] == "fact_extracted"

    async def test_empty_query_returns_all(self, client, container):
        # Clear ledger to start fresh
        container.trace_ledger._events.clear()
        sid = uuid.uuid4()
        await _seed_events(container, [
            _ev(TraceEventType.FACT_EXTRACTED, sid, i) for i in range(3)
        ])
        r = await client.post("/trace/query", json={})
        assert r.status_code == 200
        data = r.json()
        assert len(data) >= 3

    async def test_filter_by_session_key(self, client, container):
        container.trace_ledger._events.clear()
        sid = uuid.uuid4()
        await _seed_events(container, [
            _ev(TraceEventType.FACT_EXTRACTED, sid, 0, session_key="agent:main:main"),
            _ev(TraceEventType.FACT_EXTRACTED, sid, 1, session_key="agent:sub:worker"),
            _ev(TraceEventType.FACT_EXTRACTED, sid, 2, session_key="agent:main:main"),
        ])
        r = await client.post("/trace/query", json={
            "session_key": "agent:main:main",
        })
        assert r.status_code == 200
        data = r.json()
        assert len(data) == 2

    async def test_filter_by_gateway_id(self, client, container):
        """Route enforces gateway isolation: only events matching the
        middleware's gateway_id ("local") are returned, even if the
        caller requests a different gateway_id."""
        container.trace_ledger._events.clear()
        sid = uuid.uuid4()
        await _seed_events(container, [
            _ev(TraceEventType.FACT_EXTRACTED, sid, 0, gateway_id="local"),
            _ev(TraceEventType.FACT_EXTRACTED, sid, 1, gateway_id="other-gw"),
            _ev(TraceEventType.FACT_EXTRACTED, sid, 2, gateway_id="local"),
        ])
        # Even though we ask for "other-gw", the route overrides to "local"
        r = await client.post("/trace/query", json={
            "gateway_id": "other-gw",
        })
        assert r.status_code == 200
        data = r.json()
        # Should return the 2 "local" events, not the 1 "other-gw" event
        assert len(data) == 2


# ---------------------------------------------------------------------------
# GET /trace/session/{id}/timeline
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestSessionTimeline:
    """Tests for GET /trace/session/{id}/timeline"""

    async def test_groups_events_by_after_turn_completed(self, client, container):
        container.trace_ledger._events.clear()
        sid = uuid.uuid4()
        await _seed_events(container, [
            _ev(TraceEventType.FACT_EXTRACTED, sid, 0),
            _ev(TraceEventType.RETRIEVAL_PERFORMED, sid, 1),
            _ev(TraceEventType.AFTER_TURN_COMPLETED, sid, 2),
            _ev(TraceEventType.FACT_EXTRACTED, sid, 3),
            _ev(TraceEventType.AFTER_TURN_COMPLETED, sid, 4),
        ])
        r = await client.get(f"/trace/session/{sid}/timeline")
        assert r.status_code == 200
        groups = r.json()
        assert len(groups) == 2
        assert groups[0]["turn_index"] == 0
        assert groups[0]["event_count"] == 3
        assert groups[1]["turn_index"] == 1
        assert groups[1]["event_count"] == 2

    async def test_bootstrap_events_in_turn_zero(self, client, container):
        container.trace_ledger._events.clear()
        sid = uuid.uuid4()
        await _seed_events(container, [
            _ev(TraceEventType.BOOTSTRAP_COMPLETED, sid, 0),
            _ev(TraceEventType.FACT_EXTRACTED, sid, 1),
            _ev(TraceEventType.AFTER_TURN_COMPLETED, sid, 2),
        ])
        r = await client.get(f"/trace/session/{sid}/timeline")
        assert r.status_code == 200
        groups = r.json()
        assert len(groups) == 1
        assert groups[0]["turn_index"] == 0
        # Bootstrap event should be in the first turn group
        types = groups[0]["event_type_counts"]
        assert "bootstrap_completed" in types

    async def test_in_progress_turn_included(self, client, container):
        container.trace_ledger._events.clear()
        sid = uuid.uuid4()
        await _seed_events(container, [
            _ev(TraceEventType.FACT_EXTRACTED, sid, 0),
            _ev(TraceEventType.AFTER_TURN_COMPLETED, sid, 1),
            _ev(TraceEventType.FACT_EXTRACTED, sid, 2),
            # No closing AFTER_TURN_COMPLETED — turn is in progress
        ])
        r = await client.get(f"/trace/session/{sid}/timeline")
        assert r.status_code == 200
        groups = r.json()
        assert len(groups) == 2
        assert groups[1]["event_count"] == 1  # In-progress turn

    async def test_empty_session_returns_empty(self, client, container):
        container.trace_ledger._events.clear()
        sid = uuid.uuid4()
        r = await client.get(f"/trace/session/{sid}/timeline")
        assert r.status_code == 200
        assert r.json() == []

    async def test_single_turn_session(self, client, container):
        container.trace_ledger._events.clear()
        sid = uuid.uuid4()
        await _seed_events(container, [
            _ev(TraceEventType.BOOTSTRAP_COMPLETED, sid, 0),
            _ev(TraceEventType.RETRIEVAL_PERFORMED, sid, 1),
            _ev(TraceEventType.AFTER_TURN_COMPLETED, sid, 2),
        ])
        r = await client.get(f"/trace/session/{sid}/timeline")
        assert r.status_code == 200
        groups = r.json()
        assert len(groups) == 1
        assert groups[0]["event_count"] == 3

    async def test_event_type_counts_per_group(self, client, container):
        container.trace_ledger._events.clear()
        sid = uuid.uuid4()
        await _seed_events(container, [
            _ev(TraceEventType.FACT_EXTRACTED, sid, 0),
            _ev(TraceEventType.FACT_EXTRACTED, sid, 1),
            _ev(TraceEventType.RETRIEVAL_PERFORMED, sid, 2),
            _ev(TraceEventType.AFTER_TURN_COMPLETED, sid, 3),
        ])
        r = await client.get(f"/trace/session/{sid}/timeline")
        assert r.status_code == 200
        groups = r.json()
        assert len(groups) == 1
        counts = groups[0]["event_type_counts"]
        assert counts["fact_extracted"] == 2
        assert counts["retrieval_performed"] == 1
        assert counts["after_turn_completed"] == 1


# ---------------------------------------------------------------------------
# GET /trace/session/{id}/summary
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestSessionSummary:
    """Tests for GET /trace/session/{id}/summary"""

    async def test_computes_event_counts(self, client, container):
        container.trace_ledger._events.clear()
        sid = uuid.uuid4()
        await _seed_events(container, [
            _ev(TraceEventType.FACT_EXTRACTED, sid, 0),
            _ev(TraceEventType.FACT_EXTRACTED, sid, 1),
            _ev(TraceEventType.RETRIEVAL_PERFORMED, sid, 2),
        ])
        r = await client.get(f"/trace/session/{sid}/summary")
        assert r.status_code == 200
        summary = r.json()
        assert summary["total_events"] == 3
        assert summary["event_counts"]["fact_extracted"] == 2
        assert summary["event_counts"]["retrieval_performed"] == 1

    async def test_identifies_error_events(self, client, container):
        container.trace_ledger._events.clear()
        sid = uuid.uuid4()
        await _seed_events(container, [
            _ev(TraceEventType.DEGRADED_OPERATION, sid, 0),
            _ev(TraceEventType.FACT_EXTRACTED, sid, 1),
            _ev(TraceEventType.DEGRADED_OPERATION, sid, 2),
        ])
        r = await client.get(f"/trace/session/{sid}/summary")
        assert r.status_code == 200
        summary = r.json()
        assert len(summary["error_events"]) == 2
        assert all(e["event_type"] == "degraded_operation" for e in summary["error_events"])

    async def test_counts_facts_and_retrievals(self, client, container):
        container.trace_ledger._events.clear()
        sid = uuid.uuid4()
        await _seed_events(container, [
            _ev(TraceEventType.FACT_EXTRACTED, sid, 0),
            _ev(TraceEventType.FACT_SUPERSEDED, sid, 1),
            _ev(TraceEventType.RETRIEVAL_PERFORMED, sid, 2),
            _ev(TraceEventType.RETRIEVAL_PERFORMED, sid, 3),
            _ev(TraceEventType.DEDUP_TRIGGERED, sid, 4),
        ])
        r = await client.get(f"/trace/session/{sid}/summary")
        assert r.status_code == 200
        s = r.json()
        assert s["facts_extracted"] == 1
        assert s["facts_superseded"] == 1
        assert s["retrieval_count"] == 2
        assert s["dedup_triggered"] == 1

    async def test_duration_computed(self, client, container):
        container.trace_ledger._events.clear()
        sid = uuid.uuid4()
        await _seed_events(container, [
            _ev(TraceEventType.BOOTSTRAP_COMPLETED, sid, 0),
            _ev(TraceEventType.AFTER_TURN_COMPLETED, sid, 120),
        ])
        r = await client.get(f"/trace/session/{sid}/summary")
        assert r.status_code == 200
        s = r.json()
        assert s["duration_seconds"] == pytest.approx(120.0, abs=1.0)

    async def test_bootstrap_detected(self, client, container):
        container.trace_ledger._events.clear()
        sid = uuid.uuid4()
        await _seed_events(container, [
            _ev(TraceEventType.BOOTSTRAP_COMPLETED, sid, 0),
            _ev(TraceEventType.FACT_EXTRACTED, sid, 1),
        ])
        r = await client.get(f"/trace/session/{sid}/summary")
        assert r.status_code == 200
        assert r.json()["bootstrap_completed"] is True

    async def test_guard_counts(self, client, container):
        container.trace_ledger._events.clear()
        sid = uuid.uuid4()
        await _seed_events(container, [
            _ev(TraceEventType.GUARD_TRIGGERED, sid, 0),
            _ev(TraceEventType.GUARD_TRIGGERED, sid, 1),
            _ev(TraceEventType.GUARD_NEAR_MISS, sid, 2),
        ])
        r = await client.get(f"/trace/session/{sid}/summary")
        assert r.status_code == 200
        s = r.json()
        assert s["guard_triggers"] == 2
        assert s["guard_near_misses"] == 1

    async def test_empty_session(self, client, container):
        container.trace_ledger._events.clear()
        sid = uuid.uuid4()
        r = await client.get(f"/trace/session/{sid}/summary")
        assert r.status_code == 200
        s = r.json()
        assert s["total_events"] == 0
        assert s["event_counts"] == {}
        assert s["error_events"] == []
        assert s["duration_seconds"] is None
        assert s["bootstrap_completed"] is False


# ---------------------------------------------------------------------------
# GET /trace/event-types
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestEventTypes:
    """Tests for GET /trace/event-types"""

    async def test_returns_all_types(self, client):
        r = await client.get("/trace/event-types")
        assert r.status_code == 200
        data = r.json()
        returned_types = {item["type"] for item in data}
        expected_types = {et.value for et in TraceEventType}
        assert returned_types == expected_types

    async def test_includes_descriptions(self, client):
        r = await client.get("/trace/event-types")
        assert r.status_code == 200
        data = r.json()
        # Each entry should have "type" and "description" keys
        for item in data:
            assert "type" in item
            assert "description" in item

    async def test_description_for_every_type(self, client):
        from elephantbroker.api.routes.trace_event_descriptions import TRACE_EVENT_DESCRIPTIONS
        r = await client.get("/trace/event-types")
        assert r.status_code == 200
        data = r.json()
        for item in data:
            et_value = item["type"]
            if et_value in TRACE_EVENT_DESCRIPTIONS:
                assert item["description"] == TRACE_EVENT_DESCRIPTIONS[et_value]
                assert item["description"] != "", f"Empty description for {et_value}"


# ---------------------------------------------------------------------------
# group_events_by_turn helper (direct unit tests, sync)
# ---------------------------------------------------------------------------

class TestGroupEventsByTurn:
    """Unit tests for the helper function directly."""

    def test_split_at_after_turn_completed(self):
        sid = uuid.uuid4()
        events = [
            _ev(TraceEventType.FACT_EXTRACTED, sid, 0),
            _ev(TraceEventType.AFTER_TURN_COMPLETED, sid, 1),
            _ev(TraceEventType.RETRIEVAL_PERFORMED, sid, 2),
            _ev(TraceEventType.AFTER_TURN_COMPLETED, sid, 3),
        ]
        groups = group_events_by_turn(events)
        assert len(groups) == 2
        assert groups[0]["turn_index"] == 0
        assert groups[0]["event_count"] == 2
        assert groups[1]["turn_index"] == 1
        assert groups[1]["event_count"] == 2

    def test_fallback_to_ingest_buffer_flush(self):
        sid = uuid.uuid4()
        events = [
            _ev(TraceEventType.FACT_EXTRACTED, sid, 0),
            _ev(TraceEventType.INGEST_BUFFER_FLUSH, sid, 1),
            _ev(TraceEventType.FACT_EXTRACTED, sid, 2),
            _ev(TraceEventType.INGEST_BUFFER_FLUSH, sid, 3),
        ]
        groups = group_events_by_turn(events)
        assert len(groups) == 2
        assert groups[0]["event_count"] == 2
        assert groups[1]["event_count"] == 2

    def test_no_boundaries_single_group(self):
        sid = uuid.uuid4()
        events = [
            _ev(TraceEventType.FACT_EXTRACTED, sid, 0),
            _ev(TraceEventType.RETRIEVAL_PERFORMED, sid, 1),
            _ev(TraceEventType.SCORING_COMPLETED, sid, 2),
        ]
        groups = group_events_by_turn(events)
        assert len(groups) == 1
        assert groups[0]["turn_index"] == 0
        assert groups[0]["event_count"] == 3

    def test_empty_list(self):
        groups = group_events_by_turn([])
        assert groups == []

    def test_consecutive_boundaries(self):
        sid = uuid.uuid4()
        events = [
            _ev(TraceEventType.AFTER_TURN_COMPLETED, sid, 0),
            _ev(TraceEventType.AFTER_TURN_COMPLETED, sid, 1),
            _ev(TraceEventType.FACT_EXTRACTED, sid, 2),
        ]
        groups = group_events_by_turn(events)
        # First boundary closes turn 0 (1 event), second closes turn 1 (1 event),
        # trailing fact is turn 2 (1 event)
        assert len(groups) == 3
        assert groups[0]["event_count"] == 1
        assert groups[1]["event_count"] == 1
        assert groups[2]["event_count"] == 1


# ---------------------------------------------------------------------------
# GET /trace/sessions
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
class TestListSessions:
    """Tests for GET /trace/sessions"""

    async def test_list_sessions_returns_sessions(self, client, container):
        """Add events for 2 different sessions, verify both appear."""
        container.trace_ledger._events.clear()
        sid1 = uuid.uuid4()
        sid2 = uuid.uuid4()
        await _seed_events(container, [
            _ev(TraceEventType.BOOTSTRAP_COMPLETED, sid1, 0, session_key="agent:main:main"),
            _ev(TraceEventType.FACT_EXTRACTED, sid1, 1, session_key="agent:main:main"),
            _ev(TraceEventType.BOOTSTRAP_COMPLETED, sid2, 2, session_key="agent:sub:worker"),
            _ev(TraceEventType.RETRIEVAL_PERFORMED, sid2, 3, session_key="agent:sub:worker"),
            _ev(TraceEventType.FACT_EXTRACTED, sid2, 4, session_key="agent:sub:worker"),
        ])
        r = await client.get("/trace/sessions")
        assert r.status_code == 200
        data = r.json()
        assert data["total_count"] == 2
        assert len(data["sessions"]) == 2
        ids = {s["session_id"] for s in data["sessions"]}
        assert str(sid1) in ids
        assert str(sid2) in ids
        # Check event counts
        by_id = {s["session_id"]: s for s in data["sessions"]}
        assert by_id[str(sid1)]["event_count"] == 2
        assert by_id[str(sid2)]["event_count"] == 3

    async def test_list_sessions_gateway_isolation(self, client, container):
        """Verify only current gateway's sessions returned.

        The test client's middleware sets gateway_id to "local" by default.
        Events with gateway_id="other-gw" must not appear.
        """
        container.trace_ledger._events.clear()
        sid_local = uuid.uuid4()
        sid_other = uuid.uuid4()
        await _seed_events(container, [
            _ev(TraceEventType.FACT_EXTRACTED, sid_local, 0, session_key="s1", gateway_id="local"),
            _ev(TraceEventType.FACT_EXTRACTED, sid_other, 1, session_key="s2", gateway_id="other-gw"),
        ])
        r = await client.get("/trace/sessions")
        assert r.status_code == 200
        data = r.json()
        assert data["total_count"] == 1
        assert data["sessions"][0]["session_id"] == str(sid_local)

    async def test_list_sessions_pagination(self, client, container):
        """Verify limit and offset work correctly."""
        container.trace_ledger._events.clear()
        sids = [uuid.uuid4() for _ in range(5)]
        events = []
        for i, sid in enumerate(sids):
            events.append(
                _ev(TraceEventType.FACT_EXTRACTED, sid, i, session_key=f"sess-{i}")
            )
        await _seed_events(container, events)

        # Get first 2
        r = await client.get("/trace/sessions?limit=2&offset=0")
        assert r.status_code == 200
        data = r.json()
        assert data["total_count"] == 5
        assert len(data["sessions"]) == 2

        # Get next 2
        r = await client.get("/trace/sessions?limit=2&offset=2")
        assert r.status_code == 200
        data = r.json()
        assert data["total_count"] == 5
        assert len(data["sessions"]) == 2

        # Get last 1
        r = await client.get("/trace/sessions?limit=2&offset=4")
        assert r.status_code == 200
        data = r.json()
        assert data["total_count"] == 5
        assert len(data["sessions"]) == 1

    async def test_list_sessions_sorted_by_recency(self, client, container):
        """Verify most recent session is first."""
        container.trace_ledger._events.clear()
        sid_old = uuid.uuid4()
        sid_mid = uuid.uuid4()
        sid_new = uuid.uuid4()
        await _seed_events(container, [
            _ev(TraceEventType.FACT_EXTRACTED, sid_old, 0, session_key="old"),
            _ev(TraceEventType.FACT_EXTRACTED, sid_mid, 60, session_key="mid"),
            _ev(TraceEventType.FACT_EXTRACTED, sid_new, 120, session_key="new"),
        ])
        r = await client.get("/trace/sessions")
        assert r.status_code == 200
        data = r.json()
        sessions = data["sessions"]
        assert len(sessions) == 3
        # Most recent first
        assert sessions[0]["session_id"] == str(sid_new)
        assert sessions[1]["session_id"] == str(sid_mid)
        assert sessions[2]["session_id"] == str(sid_old)
