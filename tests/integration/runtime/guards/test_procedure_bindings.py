"""Integration tests for procedure red_line_bindings (4 tests).

Tests that procedure bindings loaded from the graph become static BLOCK rules
in the guard engine, that decision_domain is cached, and that unload clears state.
"""
from __future__ import annotations

import json
import uuid

import pytest
from unittest.mock import AsyncMock

from elephantbroker.runtime.guards.engine import RedLineGuardEngine
from elephantbroker.runtime.guards.autonomy import AutonomyClassifier, ToolDomainRegistry
from elephantbroker.runtime.guards.approval_queue import ApprovalQueue
from elephantbroker.runtime.redis_keys import RedisKeyBuilder
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.runtime.metrics import MetricsContext
from elephantbroker.schemas.context import AgentMessage
from elephantbroker.schemas.guards import GuardOutcome, AutonomyLevel
from elephantbroker.schemas.config import GuardConfig, HitlConfig
from elephantbroker.schemas.profile import GuardPolicy
from elephantbroker.schemas.guards import AutonomyPolicy

GATEWAY_ID = "test-gw"


def _sid() -> uuid.UUID:
    return uuid.uuid4()


def _msg(content: str, role: str = "assistant", **meta) -> AgentMessage:
    return AgentMessage(role=role, content=content, metadata=meta)


def _build_engine(mock_redis, mock_embedding, mock_graph, trace_ledger,
                  redis_keys, metrics, approval_queue, profile_registry=None):
    classifier = AutonomyClassifier(
        tool_registry=ToolDomainRegistry(),
        redis=mock_redis,
        redis_keys=redis_keys,
    )
    return RedLineGuardEngine(
        trace_ledger=trace_ledger,
        embedding_service=mock_embedding,
        graph=mock_graph,
        llm_client=None,
        profile_registry=profile_registry,
        redis=mock_redis,
        config=GuardConfig(),
        gateway_id=GATEWAY_ID,
        redis_keys=redis_keys,
        metrics=metrics,
        approval_queue=approval_queue,
        autonomy_classifier=classifier,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_load_with_bindings(
    mock_redis, mock_embedding, mock_graph, trace_ledger,
    redis_keys, metrics, approval_queue,
):
    """load_session_rules with procedure that has red_line_bindings -> rules loaded."""
    proc_id = uuid.uuid4()

    # Mock graph.get_entity to return a procedure with bindings
    mock_graph.get_entity = AsyncMock(return_value={
        "eb_id": str(proc_id),
        "red_line_bindings_json": json.dumps(["no_deploy", "no_delete"]),
        "decision_domain": "code_change",
    })

    engine = _build_engine(
        mock_redis, mock_embedding, mock_graph, trace_ledger,
        redis_keys, metrics, approval_queue,
    )

    sid = _sid()
    await engine.load_session_rules(sid, "coding", active_procedure_ids=[proc_id])

    state = engine._sessions[sid]
    # Bindings should be stored
    assert "no_deploy" in state.active_procedure_bindings
    assert "no_delete" in state.active_procedure_bindings
    # Rules should contain the binding rules
    rule_ids = [r.id for r in state.rule_registry._rules]
    assert "proc_binding:no_deploy" in rule_ids
    assert "proc_binding:no_delete" in rule_ids


@pytest.mark.asyncio
async def test_bindings_become_block(
    mock_redis, mock_embedding, mock_graph, trace_ledger,
    redis_keys, metrics, approval_queue,
):
    """Binding 'no_deploy' creates a keyword BLOCK rule -> 'deploy' in content -> BLOCK."""
    proc_id = uuid.uuid4()

    mock_graph.get_entity = AsyncMock(return_value={
        "eb_id": str(proc_id),
        "red_line_bindings_json": json.dumps(["no_deploy"]),
        "decision_domain": "code_change",
    })

    engine = _build_engine(
        mock_redis, mock_embedding, mock_graph, trace_ledger,
        redis_keys, metrics, approval_queue,
    )

    sid = _sid()
    await engine.load_session_rules(sid, "coding", active_procedure_ids=[proc_id])

    # "no_deploy" is the keyword pattern. Content containing "no_deploy" triggers BLOCK.
    messages = [_msg("Let me handle the no_deploy restriction")]
    result = await engine.preflight_check(sid, messages)
    assert result.outcome == GuardOutcome.BLOCK
    assert any("proc_binding" in r for r in result.matched_rules)


@pytest.mark.asyncio
async def test_domain_cached(
    mock_redis, mock_embedding, mock_graph, trace_ledger,
    redis_keys, metrics, approval_queue,
):
    """Procedure with decision_domain -> cached in session state's active_procedure_domains."""
    proc_id = uuid.uuid4()

    mock_graph.get_entity = AsyncMock(return_value={
        "eb_id": str(proc_id),
        "red_line_bindings_json": "[]",
        "decision_domain": "financial",
    })

    engine = _build_engine(
        mock_redis, mock_embedding, mock_graph, trace_ledger,
        redis_keys, metrics, approval_queue,
    )

    sid = _sid()
    await engine.load_session_rules(sid, "coding", active_procedure_ids=[proc_id])

    state = engine._sessions[sid]
    assert "financial" in state.active_procedure_domains


@pytest.mark.asyncio
async def test_unload_clears(
    mock_redis, mock_embedding, mock_graph, trace_ledger,
    redis_keys, metrics, approval_queue,
):
    """load -> unload -> session state is gone."""
    proc_id = uuid.uuid4()

    mock_graph.get_entity = AsyncMock(return_value={
        "eb_id": str(proc_id),
        "red_line_bindings_json": json.dumps(["no_deploy"]),
        "decision_domain": "code_change",
    })

    engine = _build_engine(
        mock_redis, mock_embedding, mock_graph, trace_ledger,
        redis_keys, metrics, approval_queue,
    )

    sid = _sid()
    await engine.load_session_rules(sid, "coding", active_procedure_ids=[proc_id])
    assert sid in engine._sessions

    await engine.unload_session(sid)
    assert sid not in engine._sessions

    # Subsequent preflight_check should raise GuardRulesNotLoadedError (B2-O20)
    from elephantbroker.runtime.guards.engine import GuardRulesNotLoadedError
    messages = [_msg("test")]
    with pytest.raises(GuardRulesNotLoadedError):
        await engine.preflight_check(sid, messages)
