"""Context lifecycle routes — delegates to ContextLifecycle orchestrator.

Routes that accept AgentMessage use dict-based validation (_parse_body)
instead of FastAPI's default JSON parser, because OpenClaw sends message
content as multipart arrays ([{type: "text", text: "..."}]) and AgentMessage
has ``content: Any`` (no normalization — content passes through as-is).
"""
from __future__ import annotations

import json

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from elephantbroker.api.deps import get_context_lifecycle, get_gateway_org_id
from elephantbroker.schemas.context import (
    AfterTurnParams,
    AssembleParams,
    BootstrapParams,
    BuildOverlayRequest,
    CompactParams,
    IngestBatchParams,
    IngestParams,
    SubagentEndedParams,
    SubagentRollbackRequest,
    SubagentSpawnParams,
)

router = APIRouter()


def _stamp_gateway(body, request: Request):
    """Stamp gateway_id and agent_key from middleware onto body.

    The middleware value ALWAYS wins over any caller-supplied body value — this
    is a tenant-isolation boundary. The pre-fix `if not body.gateway_id`
    truthiness check would allow a caller to pre-populate body.gateway_id with
    a victim tenant's ID and silently skip the stamp. `is not None` is required
    because post-Bucket-A the middleware default is "" (falsy) and a truthiness
    check would bypass the override entirely. GatewayIdentityMiddleware always
    sets both fields on request.state, so the `is None` short-circuit only
    fires when the middleware isn't wired (tests or edge cases). See TD-41.
    """
    gw = getattr(request.state, "gateway_id", None)
    if gw is not None and hasattr(body, "gateway_id"):
        body.gateway_id = gw
    ak = getattr(request.state, "agent_key", None)
    if ak is not None and hasattr(body, "agent_key"):
        body.agent_key = ak


async def _parse_body(request: Request, model: type[BaseModel]):
    """Parse request body using model_validate (dict-based) to trigger field_validators.

    FastAPI's default model_validate_json uses Pydantic's Rust parser which may
    skip Python-level field_validators. Dict-based validation always runs them.
    """
    raw = await request.body()
    data = json.loads(raw)
    return model.model_validate(data)


@router.post("/bootstrap")
async def bootstrap(request: Request):
    body = await _parse_body(request, BootstrapParams)
    _stamp_gateway(body, request)
    lifecycle = get_context_lifecycle(request)
    if lifecycle is None:
        return {"bootstrapped": True, "session_id": body.session_id, "profile": body.profile_name}
    result = await lifecycle.bootstrap(body)
    return result.model_dump(mode="json", exclude_none=True, exclude_unset=True)


@router.post("/ingest")
async def ingest(request: Request):
    body = await _parse_body(request, IngestParams)
    lifecycle = get_context_lifecycle(request)
    if lifecycle is None:
        return {"ingested": True}
    result = await lifecycle.ingest(body)
    return result.model_dump(mode="json")


@router.post("/ingest-batch")
async def ingest_batch(request: Request):
    body = await _parse_body(request, IngestBatchParams)
    lifecycle = get_context_lifecycle(request)
    if lifecycle is None:
        return {"ingested_count": len(body.messages)}
    result = await lifecycle.ingest_batch(body)
    return result.model_dump(mode="json")


@router.post("/assemble")
async def assemble(request: Request):
    body = await _parse_body(request, AssembleParams)
    _stamp_gateway(body, request)
    lifecycle = get_context_lifecycle(request)
    if lifecycle is None:
        return {"messages": [], "estimated_tokens": 0}
    result = await lifecycle.assemble(body)
    return result.model_dump(mode="json", exclude_none=True, exclude_unset=True)


@router.post("/build-overlay")
async def build_overlay(body: BuildOverlayRequest, request: Request):
    lifecycle = get_context_lifecycle(request)
    if lifecycle is None:
        return {"system_prompt": None, "prepend_context": None}
    result = await lifecycle.build_overlay(body.session_key, body.session_id)
    return result.model_dump(mode="json")


@router.post("/compact")
async def compact(body: CompactParams, request: Request):
    lifecycle = get_context_lifecycle(request)
    if lifecycle is None:
        return {"ok": True, "compacted": False, "reason": "module not available"}
    result = await lifecycle.compact(body)
    return result.model_dump(mode="json")


@router.post("/after-turn")
async def after_turn(request: Request):
    body = await _parse_body(request, AfterTurnParams)
    lifecycle = get_context_lifecycle(request)
    if lifecycle is None:
        return {"processed": True}
    await lifecycle.after_turn(body)
    return {"processed": True}


@router.post("/subagent/spawn")
async def subagent_spawn(body: SubagentSpawnParams, request: Request):
    lifecycle = get_context_lifecycle(request)
    if lifecycle is None:
        return {"parent_session_key": body.parent_session_key, "child_session_key": body.child_session_key}
    result = await lifecycle.prepare_subagent_spawn(body)
    return result.model_dump(mode="json")


@router.post("/subagent/ended")
async def subagent_ended(body: SubagentEndedParams, request: Request):
    lifecycle = get_context_lifecycle(request)
    if lifecycle is None:
        return {"acknowledged": True}
    await lifecycle.on_subagent_ended(body)
    return {"acknowledged": True}


@router.post("/subagent/rollback")
async def subagent_rollback(body: SubagentRollbackRequest, request: Request):
    from elephantbroker.api.deps import get_container
    container = get_container(request)
    redis = getattr(container, "redis", None)
    if redis and body.rollback_key:
        try:
            await redis.delete(body.rollback_key)
        except Exception:
            pass
    return {"rolled_back": True}


@router.post("/dispose")
async def dispose(body: BuildOverlayRequest, request: Request):
    """Deprecated: TS plugin no longer calls this route (GF-15).

    Kept for backward compatibility. dispose() is now a lightweight engine
    teardown; actual session cleanup uses session_end() via /sessions/end.
    """
    import logging
    logging.getLogger(__name__).info(
        "DEPRECATED /context/dispose called for %s — TS plugin should not call this",
        body.session_key,
    )
    lifecycle = get_context_lifecycle(request)
    if lifecycle is None:
        return {"disposed": True}
    await lifecycle.dispose(body.session_key, body.session_id)
    return {"disposed": True}


@router.get("/config")
async def get_config(request: Request, profile: str | None = None):
    from elephantbroker.api.deps import get_container
    container = get_container(request)
    config = getattr(container, "config", None)
    result = {}
    if config:
        if hasattr(config, "context_assembly"):
            result.update(config.context_assembly.model_dump(mode="json"))
        if hasattr(config, "llm"):
            # P6: when ?profile=X is supplied, resolve the profile-level
            # ingest_batch_size override via ProfileRegistry.
            #
            # TODO-6-702 / TODO-6-202 / TODO-6-304 (cluster C-config-swallow):
            # KeyError → 404 (unknown profile is a client error and MUST be
            # diagnosable — silent fallback hides typos behind matching-default
            # values). Other exceptions → WARNING log + global fallback (never
            # 500 the /config endpoint; transient registry/DB faults stay
            # observable via logs). Only the WARN-log format is mirrored from
            # /memory/ingest-messages (commit 7a095bf, TODO-6-701+6-401); the
            # two endpoints do NOT share the same error-handling shape.
            #
            # TODO-6-391 (Round 4, Blind Spot LOW) — GET/POST divergence note:
            # KeyError→404 is intentionally preserved HERE (GET read-only —
            # callers read the response; 404 is a diagnosable client error).
            # /memory/ingest-messages folds KeyError→Exception→WARN+fallback
            # instead (POST fire-and-forget from the TS plugin client; 404
            # would silently drop messages). See routes/memory.py comment
            # block at the try/except for the extended TODO-6-581 rationale.
            effective_batch = config.llm.ingest_batch_size
            if profile and getattr(container, "profile_registry", None):
                try:
                    # TODO-6-751 (Round 2, Feature MEDIUM): pass the gateway's
                    # configured org_id so admin-registered org overrides
                    # reach this resolve_profile() call (was hardcoded None).
                    policy = await container.profile_registry.resolve_profile(
                        profile, org_id=get_gateway_org_id(container),
                    )
                    if policy is not None:
                        effective_batch = container.profile_registry.effective_ingest_batch_size(
                            policy, config.llm,
                        )
                except KeyError:
                    # Unknown profile name — give operators a diagnosable 404
                    # instead of a silent fallback that's indistinguishable
                    # from a matching-default value.
                    raise HTTPException(
                        status_code=404, detail=f"Unknown profile: {profile}",
                    )
                except Exception:
                    # Transient registry/DB errors — keep endpoint up (don't
                    # 500), but log so operators can diagnose the silent
                    # fallback when something actually broke. Inline
                    # `import logging` matches the dispose() precedent above.
                    # TODO-6-382 (Round 3, Blind Spot INFO): format aligned with
                    # /memory/ingest-messages (profile_name=%s + exc_info=True)
                    # so operators can grep both endpoints uniformly. exc_info
                    # captures the stack trace for diagnosability without
                    # inlining a truncated str(exc) in the format string.
                    import logging
                    logging.getLogger(__name__).warning(
                        "context/config: profile resolution failed for profile_name=%s "
                        "(transient error), falling back to global ingest_batch_size",
                        profile,
                        exc_info=True,
                    )
            result["ingest_batch_size"] = effective_batch
            result["ingest_batch_timeout_ms"] = int(config.llm.ingest_batch_timeout_seconds * 1000)
    return result
