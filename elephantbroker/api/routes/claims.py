"""Claims and evidence routes."""
from __future__ import annotations

import uuid

from fastapi import APIRouter, Request

from elephantbroker.api.deps import get_evidence_engine
from elephantbroker.schemas.evidence import ClaimRecord, EvidenceRef

router = APIRouter()


@router.post("/")
async def create_claim(claim: ClaimRecord, request: Request):
    engine = get_evidence_engine(request)
    claim.gateway_id = getattr(request.state, "gateway_id", "") or claim.gateway_id
    result = await engine.record_claim(claim)
    return result.model_dump(mode="json")


@router.get("/{claim_id}")
async def get_claim(claim_id: uuid.UUID, request: Request):
    engine = get_evidence_engine(request)
    state = await engine.get_claim_verification(claim_id)
    return state.model_dump(mode="json")


@router.post("/{claim_id}/evidence")
async def attach_evidence(claim_id: uuid.UUID, evidence: EvidenceRef, request: Request):
    engine = get_evidence_engine(request)
    result = await engine.attach_evidence(claim_id, evidence)
    return result.model_dump(mode="json")


@router.post("/{claim_id}/verify")
async def verify_claim(claim_id: uuid.UUID, request: Request):
    engine = get_evidence_engine(request)
    result = await engine.verify(claim_id)
    return result.model_dump(mode="json")


@router.post("/{claim_id}/reject")
async def reject_claim(claim_id: uuid.UUID, request: Request):
    """Reject a claim with a reason."""
    engine = get_evidence_engine(request)
    body = await request.json()
    reason = body.get("reason", "")
    rejector_actor_id = body.get("rejector_actor_id")
    rejector_uuid = uuid.UUID(rejector_actor_id) if rejector_actor_id else None
    result = await engine.reject(claim_id, reason, rejector_uuid)
    return result.model_dump(mode="json")


@router.get("/procedure/{procedure_id}/completion")
async def check_procedure_completion(procedure_id: uuid.UUID, request: Request):
    """Check completion requirements for a procedure."""
    engine = get_evidence_engine(request)
    result = await engine.check_completion_requirements(procedure_id)
    return result.model_dump(mode="json")
