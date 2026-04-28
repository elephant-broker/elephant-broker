"""Memory routes."""
from __future__ import annotations

import logging
import uuid

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field

from elephantbroker.api.deps import (
    get_artifact_ingest_pipeline,
    get_container,
    get_gateway_org_id,
    get_ingest_buffer,
    get_memory_store,
    get_procedure_ingest_pipeline,
    get_turn_ingest_pipeline,
)
from elephantbroker.runtime.memory.facade import DedupSkipped
from elephantbroker.schemas.base import Scope
from elephantbroker.schemas.fact import FactAssertion, MemoryClass
from elephantbroker.schemas.pipeline import ArtifactInput, TurnInput
from elephantbroker.schemas.procedure import ProcedureDefinition

logger = logging.getLogger("elephantbroker.api.routes.memory")

router = APIRouter()


# --- Request/Response Models ---


class SearchRequest(BaseModel):
    query: str
    max_results: int = 20
    min_score: float = 0.0
    scope: str | None = None
    actor_id: str | None = None
    memory_class: str | None = None
    session_key: str | None = None
    session_id: str | None = None
    profile_name: str | None = None
    auto_recall: bool = False


class StoreRequest(BaseModel):
    fact: FactAssertion
    session_key: str | None = None
    session_id: uuid.UUID | None = None
    dedup_threshold: float | None = None
    profile_name: str | None = None


class PromoteRequest(BaseModel):
    fact_id: uuid.UUID
    to_scope: Scope


class PromoteClassRequest(BaseModel):
    fact_id: uuid.UUID
    to_class: str


class IngestMessagesRequest(BaseModel):
    session_key: str
    session_id: str | None = None
    messages: list[dict]
    profile_name: str = "coding"


class UpdateFactRequest(BaseModel):
    """Whitelist of user-updatable FactAssertion fields.

    TODO-5-610 — `extra="forbid"` blocks mass-assignment of internal/scoring
    fields (use_count, successful_use_count, freshness_score, last_used_at,
    updated_at, session_key, session_id, provenance_refs, embedding_ref,
    token_size) via `PATCH /memory/{fact_id}`. Immutable fields (id,
    created_at, source_actor_id, gateway_id) are absent from the schema and
    also defended in depth by the facade.update() setattr block.

    TODO-5-802 — `provenance_refs` is intentionally absent from the user-
    editable surface: evidence references are stamped by the ingest/consolidation
    pipeline (or explicit evidence-attachment endpoints), never by the PATCH
    path. Allowing PATCH to rewrite provenance would let a caller detach a
    claim from the receipts that justify it, defeating the evidence-required
    invariant.
    """

    model_config = ConfigDict(extra="forbid")

    text: str | None = Field(default=None, min_length=1)
    category: str | None = None
    scope: Scope | None = None
    confidence: float | None = Field(default=None, ge=0.0, le=1.0)
    memory_class: MemoryClass | None = None
    target_actor_ids: list[uuid.UUID] | None = None
    goal_ids: list[uuid.UUID] | None = None
    decision_domain: str | None = None
    archived: bool | None = None
    autorecall_blacklisted: bool | None = None
    goal_relevance_tags: dict[str, str] | None = None


# --- Existing Endpoints ---


@router.post("/store")
async def store_fact(body: StoreRequest, request: Request):
    ms = get_memory_store(request)
    fact = body.fact
    if body.session_key is not None:
        fact.session_key = body.session_key
    if body.session_id:
        fact.session_id = body.session_id
    # Middleware wins unconditionally over caller-supplied fact.gateway_id —
    # tenant-isolation boundary. See TD-41 and actors.py create_actor().
    _state_gw = getattr(request.state, "gateway_id", None)
    if _state_gw is not None:
        fact.gateway_id = _state_gw
    try:
        result = await ms.store(
            fact,
            dedup_threshold=body.dedup_threshold,
            profile_name=body.profile_name,
        )
    except DedupSkipped as e:
        return JSONResponse(
            status_code=409,
            content={
                "status": "skipped",
                "reason": "near_duplicate_detected",
                "existing_fact_id": e.existing_fact_id,
            },
        )
    return result.model_dump(mode="json")


@router.post("/search")
async def search_memory(body: SearchRequest, request: Request):
    container = get_container(request)
    ms = get_memory_store(request)
    scope = Scope(body.scope) if body.scope else None
    mc = MemoryClass(body.memory_class) if body.memory_class else None

    # Resolve profile for policy-driven search when profile_name is provided
    profile = None
    if body.profile_name and container.profile_registry:
        try:
            gw_cfg = getattr(getattr(container, "config", None), "gateway", None)
            mem_org_id = getattr(gw_cfg, "org_id", None) if gw_cfg else None
            profile = await container.profile_registry.resolve_profile(body.profile_name, org_id=mem_org_id)
        except Exception:
            pass

    # Use retrieval orchestrator with profile policy when available
    if profile and container.retrieval:
        if body.auto_recall:
            policy = profile.autorecall.retrieval
            max_results = profile.autorecall.auto_recall_injection_top_k
            min_score = profile.autorecall.min_similarity
        else:
            policy = profile.retrieval
            max_results = body.max_results
            min_score = body.min_score

        caller_gw = getattr(request.state, "gateway_id", "")
        candidates = await container.retrieval.retrieve_candidates(
            body.query,
            policy=policy,
            scope=body.scope,
            actor_id=body.actor_id,
            memory_class=mc,
            session_key=body.session_key,
            session_id=body.session_id,
            auto_recall=body.auto_recall,
            caller_gateway_id=caller_gw,
        )
        # Return enriched results with score and source (TS SearchResult contract)
        results = []
        for c in candidates[:max_results]:
            if c.score >= min_score:
                item = c.fact.model_dump(mode="json")
                item["score"] = c.score
                item["source"] = c.source
                results.append(item)
        if container.metrics_ctx:
            container.metrics_ctx.inc_retrieval(
                auto_recall=str(body.auto_recall).lower(),
                profile_name=body.profile_name or "unknown",
            )
        return results

    # Fallback: simple facade search (no profile, no orchestrator)
    caller_gw = getattr(request.state, "gateway_id", "")
    results = await ms.search(
        body.query,
        body.max_results,
        body.min_score,
        scope=scope,
        actor_id=body.actor_id,
        memory_class=mc,
        session_key=body.session_key,
        session_id=body.session_id,
        profile_name=body.profile_name or "default",
        auto_recall=body.auto_recall,
        caller_gateway_id=caller_gw,
    )
    # Add score/source for TS SearchResult contract compatibility
    return [
        {**r.model_dump(mode="json"), "score": r.freshness_score or 0.5, "source": "hybrid"}
        for r in results
    ]


@router.get("/read")
async def read_memory(
    request: Request, scope: str = "session", limit: int = 100,
    memory_class: str | None = None,
):
    ms = get_memory_store(request)
    mc = MemoryClass(memory_class) if memory_class else None
    results = await ms.get_by_scope(Scope(scope), limit, memory_class=mc)
    return [r.model_dump(mode="json") for r in results]


@router.get("/status")
async def memory_status(request: Request):
    """Returns backend health, fact count, embedding and LLM availability."""
    container = get_container(request)
    ms = get_memory_store(request)

    # Backend connectivity checks
    neo4j_ok = False
    qdrant_ok = False
    embedding_ok = False
    llm_ok = False
    facts_count = 0

    if container.graph:
        try:
            await container.graph.query_cypher("RETURN 1", {})
            neo4j_ok = True
        except Exception:
            pass
        try:
            records = await container.graph.query_cypher(
                "MATCH (f:FactDataPoint) RETURN count(f) AS cnt", {},
            )
            if records:
                facts_count = records[0].get("cnt", 0)
        except Exception:
            pass

    if container.vector:
        try:
            qdrant_client = await container.vector._get_client()
            await qdrant_client.get_collections()
            qdrant_ok = True
        except Exception:
            pass

    if container.embeddings:
        try:
            await container.embeddings.embed_text("health check")
            embedding_ok = True
        except Exception:
            pass

    llm_client = getattr(container, "llm_client", None)
    if llm_client:
        try:
            await llm_client.complete("respond with OK", "test", max_tokens=5)
            llm_ok = True
        except Exception:
            pass

    return {
        "status": "ok" if (neo4j_ok and qdrant_ok) else "degraded",
        "backend": "elephantbroker",
        "provider": "neo4j+qdrant",
        "model": getattr(container.config, "cognee", None) and container.config.cognee.embedding_model or "unknown",
        "facts_count": facts_count,
        "neo4j_connected": neo4j_ok,
        "qdrant_connected": qdrant_ok,
        "embedding_available": embedding_ok,
        "llm_available": llm_ok,
    }


@router.post("/sync")
async def sync_memory(request: Request):
    return {"synced": True}


# --- Scope promotion (renamed, old alias preserved) ---


@router.post(
    "/promote-scope",
    responses={
        200: {"description": "Fact scope promoted"},
        403: {"description": "Caller gateway does not own this fact"},
        404: {"description": "Fact not found"},
    },
)
async def promote_scope(body: PromoteRequest, request: Request):
    ms = get_memory_store(request)
    caller_gw = getattr(request.state, "gateway_id", "")
    try:
        result = await ms.promote_scope(body.fact_id, body.to_scope, caller_gateway_id=caller_gw)
        return result.model_dump(mode="json")
    except KeyError:
        return JSONResponse(status_code=404, content={"detail": "Fact not found"})
    except PermissionError as e:
        return JSONResponse(status_code=403, content={"detail": str(e)})


@router.post("/promote")
async def promote_fact(body: PromoteRequest, request: Request):
    """Alias for promote-scope (backwards compatibility)."""
    return await promote_scope(body, request)


# --- New CRUD Endpoints ---


@router.get("/{fact_id}")
async def get_fact(fact_id: uuid.UUID, request: Request):
    ms = get_memory_store(request)
    caller_gw = getattr(request.state, "gateway_id", "")
    fact = await ms.get_by_id(fact_id, caller_gateway_id=caller_gw)
    if fact is None:
        return JSONResponse(status_code=404, content={"detail": "Fact not found"})
    return fact.model_dump(mode="json")


@router.delete(
    "/{fact_id}",
    responses={
        204: {"description": "Fact deleted"},
        403: {"description": "Caller gateway does not own this fact"},
        404: {"description": "Fact not found"},
    },
)
async def delete_fact(fact_id: uuid.UUID, request: Request):
    ms = get_memory_store(request)
    caller_gw = getattr(request.state, "gateway_id", "")
    try:
        await ms.delete(fact_id, caller_gateway_id=caller_gw)
        return JSONResponse(status_code=204, content=None)
    except KeyError:
        return JSONResponse(status_code=404, content={"detail": "Fact not found"})
    except PermissionError as e:
        return JSONResponse(status_code=403, content={"detail": str(e)})


@router.patch(
    "/{fact_id}",
    responses={
        200: {"description": "Fact updated"},
        403: {"description": "Caller gateway does not own this fact"},
        404: {"description": "Fact not found"},
        422: {"description": "Invalid or disallowed field in request body"},
    },
)
async def update_fact(fact_id: uuid.UUID, body: UpdateFactRequest, request: Request):
    """Update allowed fact fields. See `UpdateFactRequest` for the whitelist."""
    ms = get_memory_store(request)
    caller_gw = getattr(request.state, "gateway_id", "")
    updates = body.model_dump(exclude_unset=True, mode="python")
    try:
        result = await ms.update(fact_id, updates, caller_gateway_id=caller_gw)
        return result.model_dump(mode="json")
    except KeyError:
        return JSONResponse(status_code=404, content={"detail": "Fact not found"})
    except PermissionError as e:
        return JSONResponse(status_code=403, content={"detail": str(e)})


# --- Class promotion ---


@router.post(
    "/promote-class",
    responses={
        200: {"description": "Fact memory class promoted"},
        403: {"description": "Caller gateway does not own this fact"},
        404: {"description": "Fact not found"},
    },
)
async def promote_class(body: PromoteClassRequest, request: Request):
    ms = get_memory_store(request)
    caller_gw = getattr(request.state, "gateway_id", "")
    try:
        mc = MemoryClass(body.to_class)
        result = await ms.promote_class(body.fact_id, mc, caller_gateway_id=caller_gw)
        return result.model_dump(mode="json")
    except KeyError:
        return JSONResponse(status_code=404, content={"detail": "Fact not found"})
    except PermissionError as e:
        return JSONResponse(status_code=403, content={"detail": str(e)})


# --- Ingest Endpoints ---


@router.post("/ingest-messages")
async def ingest_messages(body: IngestMessagesRequest, request: Request):
    # FULL mode gate: context engine owns extraction via ingest_batch().
    # Do NOT buffer — empty buffer implicitly gates POST /sessions/end too.
    # This couples to container internals intentionally: ContextLifecycle is
    # unconditionally created in RuntimeContainer.from_config() (container.py:551),
    # so `context_lifecycle is not None` is always True in FULL deployments.
    # TODO(TD-15): If tier packaging changes to support MEMORY_ONLY, this gate
    # must be revised — currently it would incorrectly suppress extraction.
    container = get_container(request)
    if container.context_lifecycle is not None:
        if container.metrics_ctx:
            container.metrics_ctx.inc_ingest_gate_skip("full_mode")
            container.metrics_ctx.inc_buffer_flush("gate_skip_full_mode")
        if container.trace_ledger:
            from elephantbroker.schemas.trace import TraceEvent, TraceEventType
            await container.trace_ledger.append_event(TraceEvent(
                event_type=TraceEventType.INGEST_BUFFER_FLUSH,
                session_key=body.session_key,
                session_id=body.session_id,
                gateway_id=getattr(request.state, "gateway_id", ""),
                payload={"action": "gate_skip_full_mode", "session_key": body.session_key,
                         "message_count": len(body.messages)},
            ))
        logger.debug("ingest-messages: FULL mode, skipping buffer (lifecycle active) session_key=%s", body.session_key)
        return JSONResponse(
            status_code=202,
            content={"status": "buffered", "message": "Full mode — extraction via context engine"},
        )

    buffer = get_ingest_buffer(request)
    pipeline = get_turn_ingest_pipeline(request)

    if buffer is None:
        # Buffer not available (no Redis) -- accept but cannot process
        logger.warning("ingest-messages: buffer not available, returning 202")
        return JSONResponse(
            status_code=202,
            content={"status": "buffered", "message": "Buffer not available, messages accepted but not processed"},
        )

    # TODO-6-701 / TODO-6-401: wire per-profile ingest_batch_size from body.profile_name.
    # TODO-6-751 (Round 2, Feature MEDIUM): org_id now read from the gateway config so
    # admin-registered org overrides reach this site (was hardcoded to None).
    # TODO-6-581 (Round 3, Interop MEDIUM): unlike GET /context/config (read-only,
    # caller reads response; 404 = diagnosable client error), this endpoint is
    # fire-and-forget write from the TS plugin client at
    # openclaw-plugins/elephantbroker-memory/src/client.ts:171-183 — `await fetch()`
    # with no status check, no response parsing, no throw. A 404 HTTP response would
    # resolve the promise silently and drop messages. Fold KeyError (unknown profile)
    # into the broader Exception fallback: WARN-log + use the global LLMConfig default
    # so operators still see typos in logs without silently dropping messages. The
    # Round 2 "mirror /context/config" rationale does not transfer across HTTP methods
    # (GET read-only vs POST fire-and-forget-write).
    # TODO-6-382 (Round 3, Blind Spot INFO): WARN log format aligned with /context/config
    # (same format: profile_name=%s + exc_info=True).
    effective_batch_size: int | None = None
    try:
        if container.profile_registry is not None:
            policy = await container.profile_registry.resolve_profile(
                body.profile_name, org_id=get_gateway_org_id(container),
            )
            if policy is not None:
                effective_batch_size = container.profile_registry.effective_ingest_batch_size(
                    policy, container.config.llm,
                )
    except Exception:  # covers KeyError (unknown profile) and transient registry/DB failures
        logger.warning(
            "ingest-messages: profile resolution failed for profile_name=%s "
            "(unknown profile or transient error), falling back to global ingest_batch_size",
            body.profile_name,
            exc_info=True,
        )
        effective_batch_size = None

    batch_ready = await buffer.add_messages(
        body.session_key, body.messages, effective_batch_size=effective_batch_size,
    )

    if batch_ready and pipeline is not None:
        messages = await buffer.flush(body.session_key)
        # Emit buffer flush trace event + metric
        container = get_container(request)
        if container.metrics_ctx:
            container.metrics_ctx.inc_buffer_flush("batch_size")
        if container.trace_ledger:
            from elephantbroker.schemas.trace import TraceEvent, TraceEventType
            await container.trace_ledger.append_event(TraceEvent(
                event_type=TraceEventType.INGEST_BUFFER_FLUSH,
                session_key=body.session_key,
                session_id=body.session_id,
                gateway_id=getattr(request.state, "gateway_id", ""),
                payload={"session_key": body.session_key, "message_count": len(messages), "trigger": "batch_size"},
            ))
        gw_id = getattr(request.state, "gateway_id", "")
        agent_key = getattr(request.state, "agent_key", "")
        result = await pipeline.run(
            session_key=body.session_key,
            messages=messages,
            session_id=body.session_id,
            profile_name=body.profile_name,
            gateway_id=gw_id,
            agent_key=agent_key,
        )
        return JSONResponse(
            status_code=200,
            content={"status": "flushed", "facts_stored": result.facts_stored},
        )

    return JSONResponse(
        status_code=202,
        content={"status": "buffered", "message": "Messages buffered, waiting for batch"},
    )


@router.post("/ingest-turn")
async def ingest_turn(body: TurnInput, request: Request):
    pipeline = get_turn_ingest_pipeline(request)
    if pipeline is None:
        return JSONResponse(
            status_code=503,
            content={"detail": "Turn ingest pipeline not available"},
        )
    gw_id = getattr(request.state, "gateway_id", "")
    agent_key = getattr(request.state, "agent_key", "")
    result = await pipeline.run(
        session_key=body.session_key,
        messages=body.messages,
        session_id=str(body.session_id) if body.session_id else None,
        profile_name=body.profile_name,
        goal_ids=body.goal_ids,
        gateway_id=gw_id,
        agent_key=agent_key,
    )
    return result.model_dump(mode="json")


@router.post("/ingest-artifact")
async def ingest_artifact(body: ArtifactInput, request: Request):
    pipeline = get_artifact_ingest_pipeline(request)
    if pipeline is None:
        return JSONResponse(
            status_code=503,
            content={"detail": "Artifact ingest pipeline not available"},
        )
    # Middleware wins unconditionally over caller-supplied body.gateway_id —
    # tenant-isolation boundary. See TD-41 and actors.py create_actor().
    _state_gw = getattr(request.state, "gateway_id", None)
    if _state_gw is not None:
        body.gateway_id = _state_gw
    result = await pipeline.run(body)
    return result.model_dump(mode="json")


@router.post("/ingest-procedure")
async def ingest_procedure(body: ProcedureDefinition, request: Request):
    pipeline = get_procedure_ingest_pipeline(request)
    if pipeline is None:
        return JSONResponse(
            status_code=503,
            content={"detail": "Procedure ingest pipeline not available"},
        )
    # Middleware wins unconditionally over caller-supplied body.gateway_id —
    # tenant-isolation boundary. See TD-41 and actors.py create_actor().
    _state_gw = getattr(request.state, "gateway_id", None)
    if _state_gw is not None:
        body.gateway_id = _state_gw
    result = await pipeline.run(body)
    return result.model_dump(mode="json")
