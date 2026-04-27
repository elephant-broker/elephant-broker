"""Artifact routes — persistent + session-scoped artifacts."""
from __future__ import annotations

import uuid
from datetime import datetime

from fastapi import APIRouter, Request
from pydantic import BaseModel

from elephantbroker.api.deps import get_artifact_store, get_session_artifact_store
from elephantbroker.schemas.artifact import (
    CreateArtifactRequest,
    SessionArtifact,
    SessionArtifactSearchRequest,
    ToolArtifact,
)

router = APIRouter()


class ArtifactSearchRequest(BaseModel):
    """R2-P9 / #1179: request body for ``POST /artifacts/search``.

    Gained 5 structural filter fields (``tool_name``, ``actor_id``,
    ``goal_id``, ``tags``, ``created_after``) so callers can narrow
    the search at the DB layer instead of pulling everything and
    filtering in Python.
    """

    query: str
    max_results: int = 10
    tool_name: str | None = None
    actor_id: uuid.UUID | None = None
    goal_id: uuid.UUID | None = None
    tags: list[str] | None = None
    created_after: datetime | None = None


@router.post("/")
async def store_artifact(artifact: ToolArtifact, request: Request):
    store = get_artifact_store(request)
    # Middleware wins unconditionally over caller-supplied artifact.gateway_id —
    # tenant-isolation boundary. See TD-41 and actors.py create_actor().
    _state_gw = getattr(request.state, "gateway_id", None)
    if _state_gw is not None:
        artifact.gateway_id = _state_gw
    result = await store.store_artifact(artifact)
    return result.model_dump(mode="json")


@router.get("/{artifact_id}")
async def get_artifact(artifact_id: uuid.UUID, request: Request):
    return {"artifact_id": str(artifact_id), "status": "stub"}


@router.post("/search")
async def search_artifacts(body: ArtifactSearchRequest, request: Request):
    store = get_artifact_store(request)
    # R2-P9 / #1179: thread structural filter kwargs through to the
    # store so the WHERE clause runs at the Cypher layer.
    results = await store.search_artifacts(
        body.query,
        body.max_results,
        tool_name=body.tool_name,
        actor_id=body.actor_id,
        goal_id=body.goal_id,
        tags=body.tags,
        created_after=body.created_after,
    )
    return [r.model_dump(mode="json") for r in results]


# --- Phase 6: Session-scoped artifact endpoints ---


@router.post("/session/search")
async def search_session_artifacts(body: SessionArtifactSearchRequest, request: Request):
    store = get_session_artifact_store(request)
    if store is None:
        return []
    results = await store.search(body.session_key, body.session_id, body.query,
                                  tool_name=body.tool_name, max_results=body.max_results)
    # Increment searched count
    for r in results:
        await store.increment_searched(body.session_key, body.session_id, str(r.artifact_id))
    return [r.model_dump(mode="json") for r in results]


@router.get("/session/{artifact_id}")
async def get_session_artifact(artifact_id: str, request: Request):
    store = get_session_artifact_store(request)
    if store is None:
        return {"error": "session artifact store not available"}
    # Need session_key and session_id from query params
    sk = request.query_params.get("session_key", "")
    sid = request.query_params.get("session_id", "")
    result = await store.get(sk, sid, artifact_id)
    if result is None:
        return {"error": "artifact not found"}
    await store.increment_searched(sk, sid, artifact_id)
    return result.model_dump(mode="json")


@router.post("/create")
async def create_artifact(body: CreateArtifactRequest, request: Request):
    if body.scope == "session":
        store = get_session_artifact_store(request)
        if store is None:
            return {"error": "session artifact store not available"}
        artifact = SessionArtifact(
            tool_name=body.tool_name,
            content=body.content,
            summary=body.summary or body.content[:200],
            session_key=body.session_key,
            session_id=body.session_id,
            tags=body.tags,
            token_estimate=len(body.content) // 4,
        )
        result = await store.store(body.session_key, body.session_id, artifact)
        return result.model_dump(mode="json")
    else:
        # Persistent scope — use ArtifactIngestPipeline
        from elephantbroker.api.deps import get_artifact_ingest_pipeline
        pipeline = get_artifact_ingest_pipeline(request)
        if pipeline is None:
            return {"error": "artifact ingest pipeline not available"}
        from elephantbroker.schemas.pipeline import ArtifactInput
        artifact_input = ArtifactInput(
            tool_name=body.tool_name,
            tool_output=body.content,
            session_key=body.session_key,
        )
        result = await pipeline.run(artifact_input)
        return {"status": "ok", "artifact_id": str(result.artifact_id) if hasattr(result, "artifact_id") else None}
