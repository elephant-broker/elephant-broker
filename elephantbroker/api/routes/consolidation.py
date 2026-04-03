"""Consolidation management API routes."""
from __future__ import annotations

import json
import logging

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

logger = logging.getLogger("elephantbroker.api.routes.consolidation")

router = APIRouter()


class RunConsolidationRequest(BaseModel):
    profile_id: str | None = None


@router.post("/run")
async def run_consolidation(body: RunConsolidationRequest, request: Request):
    """Trigger a consolidation run for this gateway."""
    from elephantbroker.runtime.consolidation.engine import ConsolidationAlreadyRunningError

    container = request.app.state.container
    engine = getattr(container, "consolidation", None)
    if engine is None:
        raise HTTPException(status_code=501, detail="Consolidation engine not available")

    gateway_id = getattr(request.state, "gateway_id", "local")
    config = getattr(container, "config", None)
    org_id = ""
    if config and hasattr(config, "gateway"):
        org_id = config.gateway.org_id or ""

    try:
        report = await engine.run_consolidation(org_id, gateway_id, body.profile_id)
        return report.model_dump(mode="json")
    except ConsolidationAlreadyRunningError:
        raise HTTPException(status_code=409, detail=f"Consolidation already running for gateway {gateway_id}")


@router.get("/reports")
async def list_reports(request: Request, limit: int = 10):
    """List recent consolidation reports."""
    container = request.app.state.container
    store = getattr(container, "consolidation_report_store", None)
    if store is None:
        return []
    gateway_id = getattr(request.state, "gateway_id", "local")
    reports = await store.list_reports(gateway_id, limit=limit)
    return [r.model_dump(mode="json") for r in reports]


@router.get("/reports/{report_id}")
async def get_report(report_id: str, request: Request):
    """Get a specific consolidation report."""
    container = request.app.state.container
    store = getattr(container, "consolidation_report_store", None)
    if store is None:
        raise HTTPException(status_code=501, detail="Report store not available")
    report = await store.get_report(report_id)
    if report is None:
        raise HTTPException(status_code=404, detail="Report not found")
    return report.model_dump(mode="json")


@router.get("/status")
async def get_status(request: Request):
    """Check if consolidation is currently running."""
    container = request.app.state.container
    redis = getattr(container, "redis", None)
    keys = getattr(container, "redis_keys", None)
    if not redis or not keys:
        return {"running": False}
    try:
        status_raw = await redis.get(keys.consolidation_status())
        if status_raw:
            return json.loads(status_raw)
    except Exception:
        pass
    return {"running": False}


@router.get("/suggestions")
async def list_suggestions(request: Request, approval_status: str | None = None):
    """List Stage 7 procedure suggestions."""
    container = request.app.state.container
    store = getattr(container, "consolidation_report_store", None)
    if store is None:
        return []
    gateway_id = getattr(request.state, "gateway_id", "local")
    return await store.list_suggestions(gateway_id, approval_status=approval_status)


class UpdateSuggestionRequest(BaseModel):
    approval_status: str  # "approved" or "rejected"


@router.patch("/suggestions/{suggestion_id}")
async def update_suggestion(suggestion_id: str, body: UpdateSuggestionRequest, request: Request):
    """Approve or reject a procedure suggestion."""
    container = request.app.state.container
    store = getattr(container, "consolidation_report_store", None)
    if store is None:
        raise HTTPException(status_code=501, detail="Report store not available")
    if body.approval_status not in ("approved", "rejected"):
        raise HTTPException(status_code=422, detail="approval_status must be 'approved' or 'rejected'")
    ok = await store.update_suggestion_status(suggestion_id, body.approval_status)
    if not ok:
        raise HTTPException(status_code=404, detail="Suggestion not found")
    return {"id": suggestion_id, "approval_status": body.approval_status}
