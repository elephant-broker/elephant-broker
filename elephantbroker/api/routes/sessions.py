"""Session lifecycle routes with gateway identity registration."""
from __future__ import annotations

import logging
import uuid

from cognee.tasks.storage import add_data_points
from fastapi import APIRouter, Request

from elephantbroker.api.deps import get_container
from elephantbroker.runtime.adapters.cognee.datapoints import ActorDataPoint
from elephantbroker.runtime.identity import deterministic_uuid_from
from elephantbroker.schemas.actor import ActorRef, ActorType
from elephantbroker.schemas.config import GatewayConfig
from elephantbroker.schemas.pipeline import SessionEndRequest, SessionStartRequest
from elephantbroker.schemas.trace import TraceEvent, TraceEventType

logger = logging.getLogger("elephantbroker.api.routes.sessions")

router = APIRouter()


@router.post("/start")
async def session_start(body: SessionStartRequest, request: Request):
    container = get_container(request)
    # Middleware wins UNCONDITIONALLY over body.gateway_id — this is a tenant
    # isolation boundary. `is not None` is required: post-Bucket-A the
    # middleware default is "" (falsy) and the old `body.gateway_id or <state>`
    # pattern let a caller spoof another tenant by posting a non-empty
    # body.gateway_id (the `or` picks the truthy LHS). See TD-41.
    gw_id = getattr(request.state, "gateway_id", None)
    if gw_id is None:
        gw_id = body.gateway_id or ""
    agent_id = body.agent_id or getattr(request.state, "agent_id", "")
    agent_key = body.agent_key or (f"{gw_id}:{agent_id}" if agent_id else "")

    agent_actor_id = None
    config = getattr(container, "config", None)
    gw_config = config.gateway if config else GatewayConfig()

    # 1. Register AgentIdentity graph node (idempotent MERGE)
    graph = getattr(container, "graph", None)
    if agent_key and gw_config.register_agent_identity and graph:
        short_name = f"{body.gateway_short_name or gw_id[:8]}:{agent_id}"
        try:
            cypher = (
                "MERGE (n:AgentIdentity {agent_key: $agent_key}) "
                "ON CREATE SET n.registered_at = datetime() "
                "ON MATCH SET n.last_seen_at = datetime() "
                "SET n.agent_id = $agent_id, n.gateway_id = $gw_id, "
                "n.short_name = $short_name, n.gateway_short_name = $gw_short"
            )
            await graph.query_cypher(cypher, {
                "agent_key": agent_key,
                "agent_id": agent_id,
                "gw_id": gw_id,
                "short_name": short_name,
                "gw_short": body.gateway_short_name or gw_id[:8],
            })
        except Exception as exc:
            logger.warning("AgentIdentity MERGE failed: %s", exc)

    # 2. Register agent self-ActorRef (idempotent upsert via add_data_points)
    if agent_key and gw_config.register_agent_actor:
        short_name = f"{body.gateway_short_name or gw_id[:8]}:{agent_id}"
        agent_actor_id = deterministic_uuid_from(agent_key)
        agent_actor = ActorRef(
            id=agent_actor_id,
            type=ActorType.WORKER_AGENT,
            display_name=short_name,
            handles=[agent_key],
            gateway_id=gw_id,
            org_id=uuid.UUID(gw_config.org_id) if gw_config.org_id else None,
            team_ids=[uuid.UUID(gw_config.team_id)] if gw_config.team_id else [],
            authority_level=getattr(gw_config, "agent_authority_level", 0),
        )
        try:
            dp = ActorDataPoint.from_schema(agent_actor)
            await add_data_points([dp])
        except Exception as exc:
            logger.warning("Agent ActorRef registration failed: %s", exc)

    # 3. Store subagent parent mapping
    if body.parent_session_key:
        redis_keys = getattr(container, "redis_keys", None)
        redis = getattr(container, "redis", None)
        if redis_keys and redis:
            try:
                eb_config = getattr(container, "config", None)
                parent_ttl = getattr(eb_config, "consolidation_min_retention_seconds", 172800) if eb_config else 172800
                await redis.setex(
                    redis_keys.session_parent(body.session_key), parent_ttl,
                    body.parent_session_key,
                )
            except Exception as exc:
                logger.warning("Subagent parent mapping failed: %s", exc)

    # 4. Emit trace event with full identity
    trace_event = TraceEvent(
        event_type=TraceEventType.SESSION_BOUNDARY,
        gateway_id=gw_id,
        agent_key=agent_key,
        agent_id=agent_id,
        session_key=body.session_key,
        payload={
            "session_key": body.session_key,
            "session_id": body.session_id,
            "event": "start",
            "parent_session_key": body.parent_session_key,
            "agent_key": agent_key,
        },
    )
    trace_ledger = getattr(container, "trace_ledger", None)
    if trace_ledger:
        await trace_ledger.append_event(trace_event)

    logger.info("Session started: key=%s, id=%s, agent_key=%s", body.session_key, body.session_id, agent_key)

    return {
        "status": "ok",
        "session_key": body.session_key,
        "session_id": body.session_id,
        "agent_key": agent_key,
        "agent_actor_id": str(agent_actor_id) if agent_actor_id else None,
        "trace_event_id": str(trace_event.id),
    }


@router.post("/context-window")
async def session_context_window(request: Request):
    """Accept context window report from TS plugin."""
    from elephantbroker.schemas.context import ContextWindowReport
    body = ContextWindowReport(**(await request.json()))
    container = get_container(request)
    # Middleware wins unconditionally over body.gateway_id — see session_start().
    gw_id = getattr(request.state, "gateway_id", None)
    if gw_id is None:
        gw_id = body.gateway_id or ""

    store = getattr(container, "session_context_store", None)
    if store:
        await store.save_context_window(body.session_key, body.session_id, {
            "context_window_tokens": body.context_window_tokens,
            "provider": body.provider,
            "model": body.model,
        })

    metrics = getattr(container, "metrics_ctx", None)
    if metrics:
        metrics.inc_context_window_reported(body.provider, body.model)

    trace_ledger = getattr(container, "trace_ledger", None)
    if trace_ledger:
        await trace_ledger.append_event(TraceEvent(
            event_type=TraceEventType.CONTEXT_WINDOW_REPORTED,
            gateway_id=gw_id,
            payload={
                "provider": body.provider, "model": body.model,
                "context_window_tokens": body.context_window_tokens,
            },
        ))

    return {"status": "ok"}


@router.post("/token-usage")
async def session_token_usage(request: Request):
    """Accept token usage report from TS plugin."""
    from elephantbroker.schemas.context import TokenUsageReport
    body = TokenUsageReport(**(await request.json()))

    container = get_container(request)
    metrics = getattr(container, "metrics_ctx", None)
    if metrics:
        metrics.observe_token_usage(body.input_tokens, body.output_tokens)

    trace_ledger = getattr(container, "trace_ledger", None)
    if trace_ledger:
        # Middleware wins unconditionally over body.gateway_id — see session_start().
        gw_id = getattr(request.state, "gateway_id", None)
        if gw_id is None:
            gw_id = body.gateway_id or ""
        await trace_ledger.append_event(TraceEvent(
            event_type=TraceEventType.TOKEN_USAGE_REPORTED,
            gateway_id=gw_id,
            payload={
                "input_tokens": body.input_tokens,
                "output_tokens": body.output_tokens,
                "total_tokens": body.total_tokens,
            },
        ))

    return {"status": "ok"}


@router.post("/end")
async def session_end(body: SessionEndRequest, request: Request):
    container = get_container(request)
    # Middleware wins unconditionally over body.gateway_id — see session_start().
    gw_id = getattr(request.state, "gateway_id", None)
    if gw_id is None:
        gw_id = body.gateway_id or ""
    agent_key = body.agent_key or getattr(request.state, "agent_key", "")

    # Force-flush buffer if available.
    # In FULL mode, the P1 gate on /memory/ingest-messages skips buffer.add_messages(),
    # so the buffer is always empty here. We add an explicit guard for defense in depth.
    buffer = getattr(container, "ingest_buffer", None)
    messages = []
    if buffer and getattr(container, "context_lifecycle", None) is None:
        messages = await buffer.force_flush(body.session_key)

    # Run pipeline on flushed messages if available.
    # In FULL mode, messages is always [] due to the guard above, so pipeline.run()
    # is never called — extraction is handled by ContextLifecycle.ingest_batch().
    pipeline = getattr(container, "turn_ingest", None)
    facts_count = 0
    if messages and pipeline:
        try:
            result = await pipeline.run(
                session_key=body.session_key,
                messages=messages,
                session_id=body.session_id,
                gateway_id=gw_id,
                agent_key=agent_key,
            )
            facts_count = result.facts_stored
        except Exception as exc:
            logger.warning("Session end pipeline failed: %s", exc)

    # GF-15: Actual session cleanup via context lifecycle (handles goal flush + guard unload + Redis delete)
    goals_flushed = 0
    context_lifecycle = getattr(container, "context_lifecycle", None)
    if context_lifecycle:
        try:
            cleanup = await context_lifecycle.session_end(body.session_key, body.session_id)
            if isinstance(cleanup, dict):
                goals_flushed = cleanup.get("goals_flushed", 0)
        except Exception as exc:
            logger.warning("Context lifecycle session_end failed: %s", exc)
    else:
        # Non-FULL mode: flush goals directly (no context lifecycle available)
        goal_store = getattr(container, "session_goal_store", None)
        if goal_store:
            try:
                goals_flushed = await goal_store.flush_to_cognee(
                    body.session_key, body.session_id,
                    agent_key=agent_key,
                )
            except Exception as exc:
                logger.warning("Session goal flush failed: %s", exc)

    # Emit trace event
    trace_event = TraceEvent(
        event_type=TraceEventType.SESSION_BOUNDARY,
        gateway_id=gw_id,
        agent_key=agent_key,
        session_key=body.session_key,
        payload={
            "session_key": body.session_key,
            "session_id": body.session_id,
            "event": "end",
            "reason": body.reason,
            "facts_count": facts_count,
            "goals_flushed": goals_flushed,
        },
    )
    trace_ledger = getattr(container, "trace_ledger", None)
    if trace_ledger:
        await trace_ledger.append_event(trace_event)

    return {
        "session_key": body.session_key,
        "session_id": body.session_id,
        "facts_count": facts_count,
        "goals_flushed": goals_flushed,
        "messages_flushed": len(messages),
        "trace_event_id": str(trace_event.id),
    }
