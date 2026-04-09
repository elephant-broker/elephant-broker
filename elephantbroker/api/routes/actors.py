"""Actor routes."""
from __future__ import annotations

import uuid

from fastapi import APIRouter, HTTPException, Request

from elephantbroker.api.deps import get_actor_registry
from elephantbroker.schemas.actor import ActorRef

router = APIRouter()


@router.post("/")
async def create_actor(actor: ActorRef, request: Request):
    registry = get_actor_registry(request)
    # Middleware wins UNCONDITIONALLY over caller-supplied actor.gateway_id —
    # this is a tenant-isolation boundary. The old reverse-OR pattern
    # (`... or actor.gateway_id`) fell through to the caller value whenever
    # the middleware value was falsy ("" post-Bucket-A), allowing cross-tenant
    # spoofing. `is not None` is required — truthiness is the bug. See TD-41.
    _state_gw = getattr(request.state, "gateway_id", None)
    if _state_gw is not None:
        actor.gateway_id = _state_gw
    result = await registry.register_actor(actor)
    return result.model_dump(mode="json")


@router.get("/{actor_id}")
async def get_actor(actor_id: uuid.UUID, request: Request):
    registry = get_actor_registry(request)
    result = await registry.resolve_actor(actor_id)
    if result is None:
        raise HTTPException(status_code=404, detail="Actor not found")
    return result.model_dump(mode="json")


@router.get("/{actor_id}/relationships")
async def get_relationships(actor_id: uuid.UUID, request: Request):
    registry = get_actor_registry(request)
    results = await registry.get_relationships(actor_id)
    return [r.model_dump(mode="json") for r in results]


@router.get("/{actor_id}/authority-chain")
async def get_authority_chain(actor_id: uuid.UUID, request: Request):
    registry = get_actor_registry(request)
    chain = await registry.get_authority_chain(actor_id)
    return [a.model_dump(mode="json") for a in chain]
