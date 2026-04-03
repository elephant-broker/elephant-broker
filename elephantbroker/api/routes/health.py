"""Health check routes."""
from __future__ import annotations

import time

from fastapi import APIRouter, Request

from elephantbroker.api.deps import get_container

router = APIRouter()


@router.get("/")
async def health(request: Request):
    container = get_container(request)
    return {"status": "ok", "version": "0.1.0", "tier": container.tier.value}


@router.get("/ready")
async def ready(request: Request):
    container = get_container(request)
    checks: dict[str, dict] = {}

    # Core module checks
    checks["trace_ledger"] = {"status": "ok" if container.trace_ledger is not None else "missing"}
    checks["profile_registry"] = {"status": "ok" if container.profile_registry is not None else "missing"}

    # Neo4j connectivity
    if container.graph:
        t0 = time.monotonic()
        try:
            await container.graph.query_cypher("RETURN 1", {})
            checks["neo4j"] = {"status": "ok", "latency_ms": round((time.monotonic() - t0) * 1000, 2)}
        except Exception as exc:
            checks["neo4j"] = {"status": "error", "latency_ms": round((time.monotonic() - t0) * 1000, 2), "error": str(exc)}
    else:
        checks["neo4j"] = {"status": "not configured"}

    # Qdrant connectivity — list collections (works even when empty, no collection required)
    if container.vector:
        t0 = time.monotonic()
        try:
            qdrant_client = await container.vector._get_client()
            await qdrant_client.get_collections()
            checks["qdrant"] = {"status": "ok", "latency_ms": round((time.monotonic() - t0) * 1000, 2)}
        except Exception as exc:
            checks["qdrant"] = {"status": "error", "latency_ms": round((time.monotonic() - t0) * 1000, 2), "error": str(exc)}
    else:
        checks["qdrant"] = {"status": "not configured"}

    # Embedding service connectivity
    if container.embeddings:
        t0 = time.monotonic()
        try:
            await container.embeddings.embed_text("health check")
            checks["embedding"] = {"status": "ok", "latency_ms": round((time.monotonic() - t0) * 1000, 2)}
        except Exception as exc:
            checks["embedding"] = {"status": "error", "latency_ms": round((time.monotonic() - t0) * 1000, 2), "error": str(exc)}
    else:
        checks["embedding"] = {"status": "not configured"}

    # LLM connectivity
    llm_client = getattr(container, "llm_client", None)
    if llm_client:
        t0 = time.monotonic()
        try:
            await llm_client.complete("respond with OK", "test", max_tokens=5)
            checks["llm"] = {"status": "ok", "latency_ms": round((time.monotonic() - t0) * 1000, 2)}
        except Exception as exc:
            checks["llm"] = {"status": "error", "latency_ms": round((time.monotonic() - t0) * 1000, 2), "error": str(exc)}
    else:
        checks["llm"] = {"status": "not configured"}

    all_ok = all(c.get("status") == "ok" for c in checks.values())
    return {"ready": all_ok, "checks": checks}


@router.get("/live")
async def live():
    return {"alive": True}
