"""FastAPI dependency injection helpers."""
from __future__ import annotations

from fastapi import Request

from elephantbroker.runtime.container import RuntimeContainer


def get_container(request: Request) -> RuntimeContainer:
    return request.app.state.container


def get_memory_store(request: Request):
    return get_container(request).memory_store


def get_actor_registry(request: Request):
    return get_container(request).actor_registry


def get_goal_manager(request: Request):
    return get_container(request).goal_manager


def get_procedure_engine(request: Request):
    return get_container(request).procedure_engine


def get_evidence_engine(request: Request):
    return get_container(request).evidence_engine


def get_artifact_store(request: Request):
    return get_container(request).artifact_store


def get_profile_registry(request: Request):
    return get_container(request).profile_registry


def get_trace_ledger(request: Request):
    return get_container(request).trace_ledger


def get_stats_engine(request: Request):
    return get_container(request).stats


def get_context_assembler(request: Request):
    return get_container(request).context_assembler


def get_compaction_engine(request: Request):
    return get_container(request).compaction_engine


def get_guard_engine(request: Request):
    return get_container(request).guard_engine


def get_working_set_manager(request: Request):
    return get_container(request).working_set_manager


def get_llm_client(request: Request):
    return getattr(get_container(request), "llm_client", None)


def get_turn_ingest_pipeline(request: Request):
    return getattr(get_container(request), "turn_ingest", None)


def get_artifact_ingest_pipeline(request: Request):
    return getattr(get_container(request), "artifact_ingest", None)


def get_procedure_ingest_pipeline(request: Request):
    return getattr(get_container(request), "procedure_ingest", None)


def get_ingest_buffer(request: Request):
    return getattr(get_container(request), "ingest_buffer", None)


def get_rerank_orchestrator(request: Request):
    return getattr(get_container(request), "rerank", None)


def get_session_goal_store(request: Request):
    return getattr(get_container(request), "session_goal_store", None)


def get_gateway_id(request: Request) -> str:
    return getattr(request.state, "gateway_id", "local")


def get_agent_key(request: Request) -> str:
    return getattr(request.state, "agent_key", "")


def get_redis_keys(request: Request):
    return getattr(get_container(request), "redis_keys", None)


def get_context_lifecycle(request: Request):
    return getattr(get_container(request), "context_lifecycle", None)


def get_session_context_store(request: Request):
    return getattr(get_container(request), "session_context_store", None)


def get_session_artifact_store(request: Request):
    return getattr(get_container(request), "session_artifact_store", None)


def get_authority_store(request: Request):
    return getattr(get_container(request), "authority_store", None)


def get_org_override_store(request: Request):
    return getattr(get_container(request), "org_override_store", None)
