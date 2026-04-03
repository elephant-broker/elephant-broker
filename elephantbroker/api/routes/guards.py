"""Guard API routes (Phase 7 — §7.11, Amendment 7.1 observability)."""
from __future__ import annotations

import logging
import uuid

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from elephantbroker.api.deps import get_container, get_guard_engine
from elephantbroker.runtime.guards.engine import GuardRulesNotLoadedError
from elephantbroker.schemas.trace import TraceEvent, TraceEventType

logger = logging.getLogger("elephantbroker.api.routes.guards")

router = APIRouter(prefix="/guards", tags=["guards"])


def _gateway_id(request: Request) -> str:
    """Extract gateway_id from request state (set by GatewayIdentityMiddleware)."""
    return getattr(request.state, "gateway_id", "")


@router.get("/active/{session_id}")
async def get_active_rules(session_id: uuid.UUID, request: Request):
    """Get active guard rules, pending approvals, and recent events for a session."""
    engine = get_guard_engine(request)
    if engine is None:
        raise HTTPException(status_code=503, detail="Guard engine not available")

    state = engine._sessions.get(session_id)
    if state is None:
        raise HTTPException(status_code=404, detail={"error": "Session not found", "code": "SESSION_NOT_FOUND"})

    active_rules = []
    if state.rule_registry:
        for rule in state.rule_registry._rules:
            if rule.enabled:
                active_rules.append({
                    "id": rule.id,
                    "pattern": rule.pattern,
                    "outcome": rule.outcome.value,
                    "source": rule.source,
                })

    gw_id = _gateway_id(request)

    pending_approvals = []
    if engine._approvals:
        try:
            reqs = await engine._approvals.get_for_session(session_id, state.agent_id)
            for r in reqs:
                pending_approvals.append({
                    "request_id": str(r.id),
                    "action_summary": r.action_summary,
                    "status": r.status.value,
                    "created_at": r.created_at.isoformat() if r.created_at else None,
                })
        except Exception as exc:
            logger.warning("Failed to fetch pending approvals (gw=%s, session=%s): %s", gw_id, session_id, exc)

    recent_events = []
    try:
        events = await engine.get_guard_history(session_id)
        for e in events[:10]:
            recent_events.append({
                "id": str(e.id),
                "outcome": e.outcome.value,
                "timestamp": e.timestamp.isoformat() if e.timestamp else None,
                "input_summary": e.input_summary,
            })
    except Exception as exc:
        logger.warning("Failed to fetch guard history (gw=%s, session=%s): %s", gw_id, session_id, exc)

    return {
        "active_rules": active_rules,
        "pending_approvals": pending_approvals,
        "recent_events": recent_events,
        "constraints": state.session_constraints,
    }


@router.get("/rules/{session_id}")
async def get_loaded_rules(session_id: uuid.UUID, request: Request):
    """Get currently loaded rules for a session (debugging)."""
    engine = get_guard_engine(request)
    if engine is None:
        raise HTTPException(status_code=503, detail="Guard engine not available")

    state = engine._sessions.get(session_id)
    if state is None:
        raise HTTPException(status_code=404, detail="Session not found")

    rules = []
    if state.rule_registry and hasattr(state.rule_registry, "_rules"):
        for rule in state.rule_registry._rules:
            rules.append({
                "id": rule.id,
                "pattern_type": rule.pattern_type.value,
                "pattern": rule.pattern,
                "outcome": rule.outcome.value,
                "enabled": rule.enabled,
                "source": rule.source,
            })

    return {
        "session_id": str(session_id),
        "rules_count": len(rules),
        "rules": rules,
        "exemplar_count": len(state.guard_policy.redline_exemplars),
        "validator_count": len(state.structural_validators),
        "binding_count": len(state.active_procedure_bindings),
    }


@router.get("/events/{session_id}")
async def get_guard_events(session_id: uuid.UUID, request: Request):
    """Get guard event history for a session."""
    engine = get_guard_engine(request)
    if engine is None:
        raise HTTPException(status_code=503, detail="Guard engine not available")

    events = await engine.get_guard_history(session_id)
    return {"events": [e.model_dump(mode="json") for e in events]}


@router.get("/events/detail/{guard_event_id}")
async def get_guard_event_detail(guard_event_id: uuid.UUID, session_id: uuid.UUID, request: Request):
    """Get detailed guard event by ID with approval status if applicable."""
    engine = get_guard_engine(request)
    if engine is None:
        raise HTTPException(status_code=503, detail="Guard engine not available")

    gw_id = _gateway_id(request)
    events = await engine.get_guard_history(session_id)
    for event in events:
        if event.id == guard_event_id:
            result = {"event": event.model_dump(mode="json")}
            # Check for associated approval
            if engine._approvals:
                state = engine._sessions.get(session_id)
                if state:
                    try:
                        reqs = await engine._approvals.get_for_session(session_id, state.agent_id)
                        for r in reqs:
                            if r.guard_event_id == guard_event_id:
                                result["approval"] = {
                                    "request_id": str(r.id),
                                    "status": r.status.value,
                                    "timeout_at": r.timeout_at.isoformat() if r.timeout_at else None,
                                    "approval_message": r.approval_message,
                                }
                                break
                    except Exception as exc:
                        logger.warning("Failed to fetch approval for event %s (gw=%s): %s", guard_event_id, gw_id, exc)
            return result
    raise HTTPException(status_code=404, detail={"error": "Guard event not found", "code": "EVENT_NOT_FOUND"})


@router.post("/check/{session_id}")
async def run_preflight_check(session_id: uuid.UUID, request: Request):
    """Manually trigger a preflight check (debugging/testing)."""
    engine = get_guard_engine(request)
    if engine is None:
        raise HTTPException(status_code=503, detail="Guard engine not available")

    from elephantbroker.schemas.context import AgentMessage
    body = await request.json()
    messages = [AgentMessage(**m) for m in body.get("messages", [])]
    if not messages:
        raise HTTPException(status_code=400, detail="messages is required")

    try:
        result = await engine.preflight_check(session_id, messages)
    except GuardRulesNotLoadedError as exc:
        # 412 Precondition Failed (not 404): the session exists but guard rules
        # haven't been loaded yet — the client must call bootstrap first.
        # 404 is reserved for missing sessions/events (data not found).
        raise HTTPException(status_code=412, detail=str(exc))
    return result.model_dump(mode="json")


@router.post("/refresh/{session_id}")
async def refresh_guard_rules(session_id: uuid.UUID, request: Request):
    """Trigger a guard rule reload for a session."""
    engine = get_guard_engine(request)
    if engine is None:
        raise HTTPException(status_code=503, detail="Guard engine not available")

    body = await request.json()
    profile_name = body.get("profile_name", "coding")
    procedure_ids = [uuid.UUID(p) for p in body.get("active_procedure_ids", [])]
    session_key = body.get("session_key", "")

    state = engine._sessions.get(session_id)
    if not session_key and state:
        session_key = state.session_key
    agent_id = state.agent_id if state else ""

    await engine.load_session_rules(
        session_id=session_id,
        profile_name=profile_name,
        active_procedure_ids=procedure_ids or None,
        session_key=session_key,
        agent_id=agent_id,
    )
    return {"refreshed": True, "session_id": str(session_id)}


@router.get("/approvals/{request_id}")
async def get_approval_request(request_id: uuid.UUID, request: Request):
    """Get a specific approval request."""
    engine = get_guard_engine(request)
    if engine is None or not engine._approvals:
        raise HTTPException(status_code=503, detail="Approval queue not available")

    body = request.query_params
    agent_id = body.get("agent_id", "")
    result = await engine._approvals.get(request_id, agent_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Approval request not found")
    return result.model_dump(mode="json")


@router.get("/approvals/session/{session_id}")
async def get_session_approvals(session_id: uuid.UUID, request: Request):
    """List all approval requests for a session."""
    engine = get_guard_engine(request)
    if engine is None or not engine._approvals:
        raise HTTPException(status_code=503, detail="Approval queue not available")

    state = engine._sessions.get(session_id)
    agent_id = state.agent_id if state else ""
    reqs = await engine._approvals.get_for_session(session_id, agent_id)
    return {"approvals": [r.model_dump(mode="json") for r in reqs]}


@router.patch("/approvals/{request_id}")
async def update_approval(request_id: uuid.UUID, request: Request):
    """Update approval status (approve/reject). Called by HITL middleware."""
    engine = get_guard_engine(request)
    if engine is None:
        raise HTTPException(status_code=503, detail="Guard engine not available")

    body = await request.json()
    status = body.get("status")
    agent_id = body.get("agent_id", "")

    if not engine._approvals:
        raise HTTPException(status_code=503, detail="Approval queue not available")

    gw_id = _gateway_id(request)

    # Resolve session_key from guard state for goal resolution
    session_key = ""
    session_goal_store = None
    if hasattr(engine, "_goals"):
        session_goal_store = engine._goals
    # Try to find session_key from any session state that owns this approval
    for sid, st in (engine._sessions or {}).items():
        session_key = st.session_key
        break  # Use first available; approval agent_id routes correctly

    if status == "approved":
        result = await engine._approvals.approve(
            request_id, agent_id,
            message=body.get("message"),
            approved_by=body.get("resolved_by"),
            session_goal_store=session_goal_store,
            session_key=session_key,
        )
        logger.info("Approval approved (gw=%s, request=%s, by=%s)", gw_id, request_id, body.get("resolved_by", "external"))
    elif status == "rejected":
        result = await engine._approvals.reject(
            request_id, agent_id,
            reason=body.get("reason", ""),
            rejected_by=body.get("resolved_by"),
            session_goal_store=session_goal_store,
            session_key=session_key,
        )
        logger.info("Approval rejected (gw=%s, request=%s, reason=%s)", gw_id, request_id, body.get("reason", "")[:80])
    else:
        raise HTTPException(status_code=400, detail="Invalid status. Must be 'approved' or 'rejected'")

    if result is None:
        raise HTTPException(status_code=404, detail="Approval request not found")

    return {"request": result.model_dump(mode="json")}


class SweepTimeoutsRequest(BaseModel):
    session_id: uuid.UUID
    agent_id: str = ""
    session_key: str = ""


@router.post("/approvals/sweep-timeouts")
async def sweep_approval_timeouts(body: SweepTimeoutsRequest, request: Request):
    """Check all pending approvals for a session and time out expired ones.

    Returns list of requests that were timed out or auto-approved.
    """
    engine = get_guard_engine(request)
    if engine is None or not engine._approvals:
        raise HTTPException(status_code=503, detail="Approval queue not available")

    reqs = await engine._approvals.get_for_session(body.session_id, body.agent_id)
    swept: list[dict] = []
    for req in reqs:
        if req.status.value != "pending":
            continue
        result = await engine._approvals.check_timeout(
            req.id, body.agent_id,
            timeout_action=req.timeout_action,
            session_goal_store=getattr(engine, "_goals", None),
            session_key=body.session_key,
        )
        if result and result.status.value != "pending":
            swept.append(result.model_dump(mode="json"))

    # Trace + metrics for swept approvals
    if swept:
        gw_id = _gateway_id(request)
        container = get_container(request)
        trace = getattr(container, "trace_ledger", None)
        metrics = getattr(container, "metrics_ctx", None)
        if trace:
            await trace.append_event(TraceEvent(
                event_type=TraceEventType.GUARD_NEAR_MISS,
                session_key=body.session_key,
                session_id=body.session_id,
                gateway_id=gw_id,
                payload={"action": "sweep_timeouts", "swept_count": len(swept)},
            ))
        if metrics:
            for _ in swept:
                metrics.inc_guard_check("timeout_swept")

    return {"swept": swept, "count": len(swept)}
