"""Working set API routes."""
from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from elephantbroker.api.deps import get_working_set_manager

router = APIRouter()


class BuildWorkingSetRequest(BaseModel):
    """Request to build the working set for a session."""
    session_id: uuid.UUID
    session_key: str
    profile_name: str = "coding"
    query: str
    goal_ids: list[uuid.UUID] = Field(default_factory=list)
    token_budget: int | None = Field(default=None, gt=0, le=1000000)  # override profile budget for testing


@router.post("/build")
async def build_working_set(body: BuildWorkingSetRequest, request: Request):
    """Build working set: candidates → rerank → score → select.

    source_type values: "fact", "artifact", "goal", "procedure".
    Phase 6 adds "compact_state".
    """
    manager = get_working_set_manager(request)
    if manager is None:
        raise HTTPException(status_code=501, detail="Working set manager not available")
    snapshot = await manager.build_working_set(
        session_id=body.session_id,
        session_key=body.session_key,
        profile_name=body.profile_name,
        query=body.query,
        goal_ids=body.goal_ids or None,
        token_budget_override=body.token_budget,
    )
    return snapshot.model_dump(mode="json")


@router.get("/{session_id}")
async def get_working_set(session_id: uuid.UUID, request: Request):
    """Get cached working set snapshot for a session."""
    manager = get_working_set_manager(request)
    if manager is None:
        raise HTTPException(status_code=501, detail="Working set manager not available")
    snapshot = await manager.get_working_set(session_id)
    if snapshot is None:
        raise HTTPException(status_code=404, detail="Working set not found")
    return snapshot.model_dump(mode="json")
