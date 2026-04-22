"""Profile routes."""
from __future__ import annotations

from fastapi import APIRouter, Request

from elephantbroker.api.deps import get_container, get_gateway_org_id, get_profile_registry

router = APIRouter()


# TODO-6-381 (Round 3, Blind Spot LOW): the local `_org_id_from_request` helper
# was a third inline duplicate of the gateway-org_id extraction pattern. It
# has been removed; both routes below now call the shared
# `api/deps.py::get_gateway_org_id(container)` helper introduced in 0c67977
# (Round 2, TODO-6-751).


@router.get("/{profile_id}")
async def get_profile(profile_id: str, request: Request):
    registry = get_profile_registry(request)
    org_id = get_gateway_org_id(get_container(request))
    policy = await registry.resolve_profile(profile_id, org_id=org_id)
    return policy.model_dump(mode="json")


@router.get("/{profile_id}/resolve")
async def resolve_profile(profile_id: str, request: Request):
    registry = get_profile_registry(request)
    org_id = get_gateway_org_id(get_container(request))
    policy = await registry.get_effective_policy(profile_id, org_id=org_id)
    return {"policy": policy.model_dump(mode="json"), "weights": policy.scoring_weights.model_dump(mode="json")}
