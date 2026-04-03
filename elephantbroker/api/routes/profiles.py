"""Profile routes."""
from __future__ import annotations

from fastapi import APIRouter, Request

from elephantbroker.api.deps import get_container, get_profile_registry

router = APIRouter()


def _org_id_from_request(request: Request) -> str | None:
    """Get org_id from the runtime's GatewayConfig (not from request headers)."""
    container = get_container(request)
    gw_config = getattr(getattr(container, "config", None), "gateway", None)
    return getattr(gw_config, "org_id", None) if gw_config else None


@router.get("/{profile_id}")
async def get_profile(profile_id: str, request: Request):
    registry = get_profile_registry(request)
    org_id = _org_id_from_request(request)
    policy = await registry.resolve_profile(profile_id, org_id=org_id)
    return policy.model_dump(mode="json")


@router.get("/{profile_id}/resolve")
async def resolve_profile(profile_id: str, request: Request):
    registry = get_profile_registry(request)
    org_id = _org_id_from_request(request)
    policy = await registry.get_effective_policy(profile_id, org_id=org_id)
    return {"policy": policy.model_dump(mode="json"), "weights": policy.scoring_weights.model_dump(mode="json")}
