"""Health check routes."""
from __future__ import annotations

import logging
import time

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from elephantbroker.api.deps import get_container

logger = logging.getLogger(__name__)

router = APIRouter()


# ---------------------------------------------------------------------------
# R2-P4 / #9 RESOLVED — LLM probe cache (60s TTL per gateway_id)
#
# Pre-fix: every /ready request invoked ``llm_client.complete()`` with
# ``max_tokens=5`` — K8s readinessProbe (default 1×/sec) burned ~3,600 LLM
# calls/hour per pod purely on health checks. Multi-pod deployments
# multiplied this. The fix caches the LLM probe result per-gateway for 60
# seconds so a tight readinessProbe interval doesn't translate into a
# tight LLM-call cadence.
#
# Cache scope: module-level dict keyed by ``gateway_id``. Multi-tenant
# deployments share a single process today (single-tenant-per-process per
# R2-P1.1), but the keying lets a future multi-tenant config keep tenants
# isolated. Failures are cached the same way successes are — a transient
# LLM outage pins the pod as unhealthy for ≤60s, which prevents probe
# flapping and matches K8s expectations.
# ---------------------------------------------------------------------------
_llm_probe_cache: dict[str, tuple[float, dict]] = {}
_LLM_PROBE_TTL_SEC = 60.0


@router.get("/")
async def health(request: Request):
    container = get_container(request)
    # R2-P4 / #1505 RESOLVED: include gateway_id for operational
    # verification of which tenant this pod is bound to.
    return {
        "status": "ok",
        "version": "0.1.0",
        "tier": container.tier.value,
        "gateway_id": container.gateway_id,
    }


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
            logger.warning("%s health check failed: %s", "Neo4j", exc)
            checks["neo4j"] = {"status": "error", "latency_ms": round((time.monotonic() - t0) * 1000, 2), "error": str(exc)}
    else:
        checks["neo4j"] = {"status": "not configured"}

    # Qdrant connectivity — R2-P4 / #1189 RESOLVED: use the public
    # ``vector.ping()`` method instead of reaching into the private
    # ``_get_client()`` accessor. Decouples the health route from
    # VectorAdapter implementation details.
    if container.vector:
        t0 = time.monotonic()
        try:
            await container.vector.ping()
            checks["qdrant"] = {"status": "ok", "latency_ms": round((time.monotonic() - t0) * 1000, 2)}
        except Exception as exc:
            logger.warning("%s health check failed: %s", "Qdrant", exc)
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
            logger.warning("%s health check failed: %s", "Embedding", exc)
            checks["embedding"] = {"status": "error", "latency_ms": round((time.monotonic() - t0) * 1000, 2), "error": str(exc)}
    else:
        checks["embedding"] = {"status": "not configured"}

    # LLM connectivity — R2-P4 / #9 RESOLVED: cached per-gateway for 60s
    # so K8s readinessProbe loops don't burn tokens. Cache key is the
    # container's gateway_id; cache stores the entire check dict.
    llm_client = getattr(container, "llm_client", None)
    if llm_client:
        gw_id = container.gateway_id
        now = time.monotonic()
        cached = _llm_probe_cache.get(gw_id)
        if cached is not None and (now - cached[0]) < _LLM_PROBE_TTL_SEC:
            checks["llm"] = cached[1]
        else:
            t0 = time.monotonic()
            try:
                await llm_client.complete("respond with OK", "test", max_tokens=5)
                llm_check = {"status": "ok", "latency_ms": round((time.monotonic() - t0) * 1000, 2)}
            except Exception as exc:
                logger.warning("%s health check failed: %s", "LLM", exc)
                llm_check = {"status": "error", "latency_ms": round((time.monotonic() - t0) * 1000, 2), "error": str(exc)}
            _llm_probe_cache[gw_id] = (now, llm_check)
            checks["llm"] = llm_check
    else:
        checks["llm"] = {"status": "not configured"}

    all_ok = all(c.get("status") == "ok" for c in checks.values())
    # R2-P4 / #11 RESOLVED: return HTTP 503 when any sub-check fails so
    # K8s readinessProbe can detect unhealthy pods. Pre-fix the route
    # always returned 200 (FastAPI default), even when ``ready=False``.
    # R2-P4 / #1505 RESOLVED: response body now includes ``gateway_id``
    # for operational verification.
    return JSONResponse(
        status_code=200 if all_ok else 503,
        content={
            "ready": all_ok,
            "status": "ready" if all_ok else "unhealthy",
            "checks": checks,
            "gateway_id": container.gateway_id,
        },
    )


@router.get("/live")
async def live():
    return {"alive": True}
