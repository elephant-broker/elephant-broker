"""Goal routes — persistent goals (Cognee) + session goals (Redis)."""
from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from elephantbroker.api.deps import get_goal_manager, get_container, get_trace_ledger
from elephantbroker.runtime.identity import deterministic_uuid_from
from elephantbroker.schemas.goal import GoalState, GoalStatus
from elephantbroker.schemas.trace import TraceEvent, TraceEventType

router = APIRouter()


class UpdateStatusRequest(BaseModel):
    status: GoalStatus
    evidence: str | None = None
    confidence: float | None = None


class CreateSessionGoalRequest(BaseModel):
    title: str
    description: str = ""
    parent_goal_id: uuid.UUID | None = None
    success_criteria: list[str] = Field(default_factory=list)


class AddBlockerRequest(BaseModel):
    blocker: str


class ProgressRequest(BaseModel):
    evidence: str


def _get_session_goal_store(request: Request):
    return getattr(get_container(request), "session_goal_store", None)


def _get_metrics(request: Request):
    return getattr(get_container(request), "metrics_ctx", None)


# --- Session goals (Redis) — must be registered BEFORE /{goal_id} wildcard ---

@router.get("/session")
async def get_session_goals(
    session_key: str, session_id: uuid.UUID, request: Request,
):
    metrics = _get_metrics(request)
    if metrics:
        metrics.inc_session_goals_tool("list")
    store = _get_session_goal_store(request)
    if store is None:
        return {"goals": []}
    goals = await store.get_goals(session_key, session_id)
    return {"goals": [g.model_dump(mode="json") for g in goals]}


@router.post("/session")
async def create_session_goal(
    body: CreateSessionGoalRequest,
    session_key: str, session_id: uuid.UUID,
    request: Request,
):
    metrics = _get_metrics(request)
    if metrics:
        metrics.inc_session_goals_tool("create")
    store = _get_session_goal_store(request)
    if store is None:
        raise HTTPException(status_code=501, detail="Session goal store not available")
    existing = await store.get_goals(session_key, session_id)
    # Dedup: reject duplicate title (case-insensitive) — B2-BUG04
    title_lower = body.title.strip().lower()
    for g in existing:
        if g.title.strip().lower() == title_lower:
            raise HTTPException(status_code=409, detail="A goal with this title already exists in the session")
    # Validate parent exists if provided (B2-O19)
    if body.parent_goal_id:
        if not any(g.id == body.parent_goal_id for g in existing):
            raise HTTPException(status_code=400, detail="Parent goal not found in session")
        # Sub-goal limit enforcement — session-wide, matches GoalRefinementTask semantics
        container = get_container(request)
        max_sub = getattr(getattr(container, "config", None), "goal_refinement", None)
        max_sub = max_sub.max_subgoals_per_session if max_sub else 10
        sub_count = sum(1 for g in existing if g.parent_goal_id is not None)
        if sub_count >= max_sub:
            raise HTTPException(
                status_code=400,
                detail=f"Sub-goal limit reached ({max_sub}) for session",
            )
    # Derive agent actor UUID from gateway identity for OWNS_GOAL edges
    agent_key = getattr(request.state, "agent_key", "")
    owner_ids = [deterministic_uuid_from(agent_key)] if agent_key else []
    goal = GoalState(
        title=body.title,
        description=body.description,
        parent_goal_id=body.parent_goal_id,
        success_criteria=body.success_criteria,
        gateway_id=getattr(request.state, "gateway_id", ""),
        owner_actor_ids=owner_ids,
    )
    result = await store.add_goal(session_key, session_id, goal)
    trace = get_trace_ledger(request)
    if trace:
        await trace.append_event(TraceEvent(
            event_type=TraceEventType.SESSION_GOAL_CREATED,
            session_id=session_id,
            session_key=session_key,
            goal_ids=[result.id],
            payload={
                "title": result.title,
                "session_key": session_key,
                "parent_goal_id": str(result.parent_goal_id) if result.parent_goal_id else None,
            },
        ))
    return result.model_dump(mode="json")


@router.patch("/session/{goal_id}")
async def update_session_goal_status(
    goal_id: uuid.UUID, body: UpdateStatusRequest,
    session_key: str, session_id: uuid.UUID,
    request: Request,
):
    metrics = _get_metrics(request)
    if metrics:
        metrics.inc_session_goals_tool("update")
    store = _get_session_goal_store(request)
    if store is None:
        raise HTTPException(status_code=501, detail="Session goal store not available")
    updates = {"status": body.status}
    if body.evidence:
        updates["_append_evidence"] = body.evidence
    try:
        result = await store.update_goal(session_key, session_id, goal_id, updates)
    except ValueError as exc:
        raise HTTPException(status_code=403, detail=str(exc))
    if result is None:
        raise HTTPException(status_code=404, detail="Goal not found")
    if metrics and body.status == GoalStatus.COMPLETED:
        metrics.inc_goal_hint("completion")
    trace = get_trace_ledger(request)
    if trace:
        await trace.append_event(TraceEvent(
            event_type=TraceEventType.SESSION_GOAL_UPDATED,
            session_id=session_id,
            session_key=session_key,
            goal_ids=[goal_id],
            payload={"status": body.status.value, "session_key": session_key},
        ))
    return result.model_dump(mode="json")


@router.post("/session/{goal_id}/blocker")
async def add_session_goal_blocker(
    goal_id: uuid.UUID, body: AddBlockerRequest,
    session_key: str, session_id: uuid.UUID,
    request: Request,
):
    metrics = _get_metrics(request)
    if metrics:
        metrics.inc_session_goals_tool("blocker")
    store = _get_session_goal_store(request)
    if store is None:
        raise HTTPException(status_code=501, detail="Session goal store not available")
    result = await store.add_blocker(session_key, session_id, goal_id, body.blocker)
    if result is None:
        raise HTTPException(status_code=404, detail="Goal not found")
    if metrics:
        metrics.inc_goal_hint("blocker")
    trace = get_trace_ledger(request)
    if trace:
        await trace.append_event(TraceEvent(
            event_type=TraceEventType.SESSION_GOAL_BLOCKER_ADDED,
            session_id=session_id,
            session_key=session_key,
            goal_ids=[goal_id],
            payload={"blocker": body.blocker, "session_key": session_key},
        ))
    return result.model_dump(mode="json")


@router.post("/session/{goal_id}/progress")
async def record_session_goal_progress(
    goal_id: uuid.UUID, body: ProgressRequest,
    session_key: str, session_id: uuid.UUID,
    request: Request,
):
    metrics = _get_metrics(request)
    if metrics:
        metrics.inc_session_goals_tool("progress")
    store = _get_session_goal_store(request)
    if store is None:
        raise HTTPException(status_code=501, detail="Session goal store not available")
    goals = await store.get_goals(session_key, session_id)
    container = get_container(request)
    delta = getattr(getattr(container, "config", None), "goal_refinement", None)
    delta = delta.progress_confidence_delta if delta else 0.1
    for g in goals:
        if g.id == goal_id:
            g.confidence = min(1.0, g.confidence + delta)
            g.evidence.append(body.evidence)
            await store.set_goals(session_key, session_id, goals)
            if metrics:
                metrics.inc_goal_hint("progress")
            trace = get_trace_ledger(request)
            if trace:
                await trace.append_event(TraceEvent(
                    event_type=TraceEventType.SESSION_GOAL_PROGRESS,
                    session_id=session_id,
                    session_key=session_key,
                    goal_ids=[goal_id],
                    payload={"confidence": g.confidence, "evidence": body.evidence, "session_key": session_key},
                ))
            return g.model_dump(mode="json")
    raise HTTPException(status_code=404, detail="Goal not found")


# --- Persistent goals (Cognee) ---

@router.post("/")
async def create_goal(goal: GoalState, request: Request):
    manager = get_goal_manager(request)
    # Middleware wins unconditionally over caller-supplied goal.gateway_id —
    # tenant-isolation boundary. See TD-41 and actors.py create_actor().
    _state_gw = getattr(request.state, "gateway_id", None)
    if _state_gw is not None:
        goal.gateway_id = _state_gw
    result = await manager.set_goal(goal)
    container = get_container(request)
    metrics = getattr(container, "metrics_ctx", None)
    if metrics:
        metrics.inc_goal_create()
    trace = get_trace_ledger(request)
    if trace:
        await trace.append_event(TraceEvent(
            event_type=TraceEventType.PERSISTENT_GOAL_CREATED,
            gateway_id=getattr(request.state, "gateway_id", ""),
            payload={
                "goal_id": str(result.id),
                "scope": result.scope.value if hasattr(result.scope, "value") else str(result.scope),
                "title": result.title,
            },
        ))
    return result.model_dump(mode="json")


@router.get("/hierarchy")
async def get_hierarchy(root_goal_id: uuid.UUID, request: Request):
    manager = get_goal_manager(request)
    hierarchy = await manager.get_goal_hierarchy(root_goal_id)
    return hierarchy.model_dump(mode="json")


@router.get("/{goal_id}")
async def get_goal(goal_id: uuid.UUID, request: Request):
    manager = get_goal_manager(request)
    hierarchy = await manager.get_goal_hierarchy(goal_id)
    if not hierarchy.root_goals:
        raise HTTPException(status_code=404, detail="Goal not found")
    return hierarchy.root_goals[0].model_dump(mode="json")


@router.put("/{goal_id}")
async def update_goal(goal_id: uuid.UUID, body: UpdateStatusRequest, request: Request):
    manager = get_goal_manager(request)
    result = await manager.update_goal_status(goal_id, body.status, confidence=body.confidence)
    return result.model_dump(mode="json")
