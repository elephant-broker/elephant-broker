"""Procedure routes — CRUD + Phase 5 activation/completion/status."""
from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from elephantbroker.api.deps import get_container, get_procedure_engine
from elephantbroker.schemas.procedure import ProcedureDefinition

router = APIRouter()


class ActivateRequest(BaseModel):
    actor_id: uuid.UUID | None = None


class StepCompleteRequest(BaseModel):
    proof_value: str | None = None


@router.post("/")
async def create_procedure(procedure: ProcedureDefinition, request: Request):
    engine = get_procedure_engine(request)
    procedure.gateway_id = getattr(request.state, "gateway_id", "") or procedure.gateway_id
    result = await engine.store_procedure(procedure)
    return result.model_dump(mode="json")


@router.get("/{procedure_id}")
async def get_procedure(procedure_id: uuid.UUID, request: Request):
    return {"procedure_id": str(procedure_id), "status": "stub"}


@router.put("/{procedure_id}")
async def update_procedure(procedure_id: uuid.UUID, request: Request):
    return {"procedure_id": str(procedure_id), "status": "updated"}


class ActivateRequestV2(BaseModel):
    actor_id: uuid.UUID | None = None
    session_key: str = ""
    session_id: str = ""
    profile_name: str = "coding"


@router.post("/{procedure_id}/activate")
async def activate_procedure(procedure_id: uuid.UUID, body: ActivateRequestV2, request: Request):
    engine = get_procedure_engine(request)
    if engine is None:
        raise HTTPException(status_code=501, detail="Procedure engine not available")
    actor_id = body.actor_id or uuid.uuid4()
    sk = body.session_key or getattr(request.state, "session_key", "")
    sid = body.session_id or ""
    sid_uuid = uuid.UUID(sid) if sid else None
    execution = await engine.activate(procedure_id, actor_id, session_key=sk, session_id=sid_uuid)

    # Record audit event
    container = get_container(request)
    audit = getattr(container, "procedure_audit", None)
    if audit:
        await audit.record_event(
            session_key=sk, session_id=sid,
            procedure_id=str(procedure_id), procedure_name="",
            event_type="activated",
            execution_id=str(execution.execution_id),
        )

    # Phase 7: Refresh guard rules after procedure activation
    lifecycle = getattr(container, "context_lifecycle", None)
    if lifecycle and sk and sid:
        try:
            await lifecycle.refresh_guard_rules(sk, sid, body.profile_name)
        except Exception:
            pass

    return execution.model_dump(mode="json")


@router.post("/{execution_id}/step/{step_id}/complete")
async def complete_step(
    execution_id: uuid.UUID, step_id: uuid.UUID,
    body: StepCompleteRequest, request: Request,
):
    engine = get_procedure_engine(request)
    if engine is None:
        raise HTTPException(status_code=501, detail="Procedure engine not available")
    result = await engine.check_step(execution_id, step_id)
    # Phase 7: check_step returns StepCheckResult, not bool
    if hasattr(result, 'complete'):
        if not result.complete:
            return {"execution_id": str(execution_id), "step_id": str(step_id),
                    "completed": False, "missing_evidence": result.missing_evidence}
    elif not result:
        raise HTTPException(status_code=404, detail="Execution not found")

    # Look up execution context for audit and auto-evidence
    container = get_container(request)
    gw_id = getattr(request.state, "gateway_id", "")
    proc_id = None
    sk = ""
    sid = ""
    execution = engine._executions.get(execution_id)
    if execution:
        proc_id = execution.procedure_id
        sk = execution.session_key or ""
        sid = str(execution.session_id or "")

    # Record audit event
    audit = getattr(container, "procedure_audit", None)
    if audit:
        await audit.record_event(
            session_key=sk, session_id=sid,
            procedure_id=str(proc_id or ""), procedure_name="",
            event_type="step_completed",
            execution_id=str(execution_id),
            step_id=str(step_id),
        )
        if body.proof_value:
            await audit.record_event(
                session_key=sk, session_id=sid,
                procedure_id=str(proc_id or ""), procedure_name="",
                event_type="proof_submitted",
                execution_id=str(execution_id),
                step_id=str(step_id),
                proof_value=body.proof_value,
            )

    # Auto-create ClaimRecord + EvidenceRef so completion check can find them
    if body.proof_value:
        await engine.record_step_evidence(execution_id, step_id, body.proof_value, gateway_id=gw_id)

    return {"execution_id": str(execution_id), "step_id": str(step_id), "completed": True}


@router.get("/session/status")
async def get_session_procedure_status(
    session_key: str = "", session_id: str = "", request: Request = None,
):
    """View all procedures tracked in this session."""
    container = get_container(request)
    audit = getattr(container, "procedure_audit", None)
    if not audit:
        return {"procedures": []}
    events = await audit.get_session_events(session_key, session_id)
    return {"procedures": events}
