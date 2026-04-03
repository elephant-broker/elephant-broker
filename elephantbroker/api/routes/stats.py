"""Stats routes."""
from __future__ import annotations

from fastapi import APIRouter, Request

from elephantbroker.api.deps import get_stats_engine

router = APIRouter()


@router.get("/by-profile/{profile_id}")
async def get_stats_by_profile(profile_id: str, request: Request):
    engine = get_stats_engine(request)
    if engine is None:
        return {}
    return await engine.get_stats_by_profile(profile_id)
