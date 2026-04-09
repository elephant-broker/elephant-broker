"""Trace routes."""
from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, Query, Request

from elephantbroker.api.deps import get_trace_ledger
from elephantbroker.api.routes.trace_event_descriptions import TRACE_EVENT_DESCRIPTIONS
from elephantbroker.schemas.trace import SessionSummary, TraceEventType, TraceQuery

router = APIRouter()


@router.get("/")
async def list_traces(request: Request, session_id: uuid.UUID | None = None, limit: int = 100):
    ledger = get_trace_ledger(request)
    gw_id = getattr(request.state, "gateway_id", None)
    query = TraceQuery(session_id=session_id, limit=limit, gateway_id=gw_id)
    events = await ledger.query_trace(query)
    return [e.model_dump(mode="json") for e in events]


@router.post("/query")
async def query_traces(query: TraceQuery, request: Request):
    # Enforce gateway isolation: the middleware-provided gateway_id always wins
    # over any caller-supplied value in the request body. `is not None` is
    # required here (not truthiness): post-Bucket-A the default gateway_id is
    # "" (empty string, falsy), and a truthiness check would silently skip the
    # override, allowing a caller to read another tenant's trace events by
    # posting {"gateway_id": "victim-tenant"}. GatewayIdentityMiddleware always
    # sets request.state.gateway_id to a string (possibly ""), so this check
    # only short-circuits when the middleware isn't wired at all.
    gw_id = getattr(request.state, "gateway_id", None)
    if gw_id is not None:
        query.gateway_id = gw_id
    ledger = get_trace_ledger(request)
    events = await ledger.query_trace(query)
    return [e.model_dump(mode="json") for e in events]


@router.get("/session/{session_id}/timeline")
async def session_timeline(session_id: uuid.UUID, request: Request):
    ledger = get_trace_ledger(request)
    gw_id = getattr(request.state, "gateway_id", None)
    query = TraceQuery(session_id=session_id, limit=10000, gateway_id=gw_id)
    events = await ledger.query_trace(query)
    groups = group_events_by_turn(events)
    return groups


@router.get("/session/{session_id}/summary")
async def session_summary(session_id: uuid.UUID, request: Request):
    ledger = get_trace_ledger(request)
    gw_id = getattr(request.state, "gateway_id", None)
    query = TraceQuery(session_id=session_id, limit=10000, gateway_id=gw_id)
    events = await ledger.query_trace(query)

    event_counts: dict[str, int] = {}
    for e in events:
        event_counts[e.event_type.value] = event_counts.get(e.event_type.value, 0) + 1

    error_events = [e.model_dump(mode="json") for e in events
                    if e.event_type == TraceEventType.DEGRADED_OPERATION]

    first_at = min((e.timestamp for e in events), default=None)
    last_at = max((e.timestamp for e in events), default=None)
    duration = (last_at - first_at).total_seconds() if first_at and last_at else None

    summary = SessionSummary(
        session_id=session_id,
        total_events=len(events),
        event_counts=event_counts,
        error_events=error_events,
        first_event_at=first_at,
        last_event_at=last_at,
        duration_seconds=duration,
        turn_count=event_counts.get("after_turn_completed", 0),
        facts_extracted=event_counts.get("fact_extracted", 0),
        facts_superseded=event_counts.get("fact_superseded", 0),
        dedup_triggered=event_counts.get("dedup_triggered", 0),
        retrieval_count=event_counts.get("retrieval_performed", 0),
        compaction_count=event_counts.get("compaction_action", 0),
        guard_triggers=event_counts.get("guard_triggered", 0),
        guard_near_misses=event_counts.get("guard_near_miss", 0),
        context_assembled=event_counts.get("context_assembled", 0),
        scoring_completed=event_counts.get("scoring_completed", 0),
        successful_use_tracked=event_counts.get("successful_use_tracked", 0),
        bootstrap_completed="bootstrap_completed" in event_counts,
    )
    return summary.model_dump(mode="json")


@router.get("/sessions")
async def list_sessions(
    request: Request,
    limit: int = Query(default=100, le=1000),
    offset: int = Query(default=0, ge=0),
):
    """List all sessions for the current gateway, sorted by most recent activity."""
    ledger = get_trace_ledger(request)
    gateway_id = getattr(request.state, "gateway_id", None)
    result = await ledger.list_sessions(gateway_id=gateway_id, limit=limit, offset=offset)
    return result.model_dump(mode="json")


@router.get("/event-types")
async def list_event_types():
    """Reference endpoint — intentionally public, no gateway filtering needed."""
    return [
        {"type": et.value, "description": TRACE_EVENT_DESCRIPTIONS.get(et.value, "")}
        for et in TraceEventType
    ]


@router.get("/{event_id}")
async def get_trace_event(event_id: uuid.UUID, request: Request):
    ledger = get_trace_ledger(request)
    gw_id = getattr(request.state, "gateway_id", None)
    events = await ledger.get_evidence_chain(event_id)
    # `is not None` is required here — see POST /query above. Under the
    # post-Bucket-A "" middleware default, a truthiness check would bypass
    # this filter entirely and leak evidence chains across gateways.
    if gw_id is not None:
        events = [e for e in events if e.gateway_id == gw_id]
    if not events:
        raise HTTPException(status_code=404, detail="Event not found")
    return events[0].model_dump(mode="json")


def group_events_by_turn(events: list) -> list[dict]:
    """Split events into turns at turn-boundary markers."""
    sorted_events = sorted(events, key=lambda e: e.timestamp)

    has_after_turn = any(
        e.event_type == TraceEventType.AFTER_TURN_COMPLETED for e in sorted_events)
    if has_after_turn:
        boundary = TraceEventType.AFTER_TURN_COMPLETED
    elif any(e.event_type == TraceEventType.INGEST_BUFFER_FLUSH for e in sorted_events):
        boundary = TraceEventType.INGEST_BUFFER_FLUSH
    else:
        return [_make_turn_group(0, sorted_events)] if sorted_events else []

    groups: list[dict] = []
    current_group: list = []
    turn_index = 0

    for event in sorted_events:
        current_group.append(event)
        if event.event_type == boundary:
            groups.append(_make_turn_group(turn_index, current_group))
            current_group = []
            turn_index += 1

    if current_group:
        groups.append(_make_turn_group(turn_index, current_group))

    return groups


def _make_turn_group(index: int, events: list) -> dict:
    type_counts: dict[str, int] = {}
    for e in events:
        type_counts[e.event_type.value] = type_counts.get(e.event_type.value, 0) + 1
    return {
        "turn_index": index,
        "start_time": events[0].timestamp.isoformat() if events else None,
        "end_time": events[-1].timestamp.isoformat() if events else None,
        "event_count": len(events),
        "event_type_counts": type_counts,
        "events": [e.model_dump(mode="json") for e in events],
    }
