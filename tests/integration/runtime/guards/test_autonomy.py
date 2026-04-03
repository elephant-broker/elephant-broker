"""Integration tests for autonomy classification and composition (10 tests).

Tests the AutonomyClassifier 3-tier hybrid domain classification and how
autonomy floors compose with safety outcomes in the guard engine.
"""
from __future__ import annotations

import uuid

import pytest
from unittest.mock import AsyncMock

from elephantbroker.runtime.guards.autonomy import AutonomyClassifier, ToolDomainRegistry
from elephantbroker.runtime.guards.engine import RedLineGuardEngine
from elephantbroker.runtime.guards.approval_queue import ApprovalQueue
from elephantbroker.runtime.redis_keys import RedisKeyBuilder
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.runtime.metrics import MetricsContext
from elephantbroker.schemas.guards import (
    AutonomyLevel,
    AutonomyPolicy,
    DecisionDomain,
    GuardActionType,
    GuardCheckInput,
    GuardOutcome,
    ApprovalStatus,
)
from elephantbroker.schemas.config import GuardConfig, HitlConfig
from elephantbroker.schemas.context import AgentMessage
from elephantbroker.schemas.profile import GuardPolicy


GATEWAY_ID = "test-gw"


def _sid() -> uuid.UUID:
    return uuid.uuid4()


def _msg(content: str, role: str = "assistant", **meta) -> AgentMessage:
    return AgentMessage(role=role, content=content, metadata=meta)


def _build_engine(
    mock_redis, mock_embedding, mock_graph, trace_ledger,
    redis_keys, metrics, approval_queue, classifier,
    *, profile_registry=None,
) -> RedLineGuardEngine:
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
# Tier classification tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tier1_known_tool():
    """Tier 1: action_target='send_email' -> domain=COMMUNICATION via static mapping."""
    classifier = AutonomyClassifier(tool_registry=ToolDomainRegistry())
    action = GuardCheckInput(
        action_type=GuardActionType.TOOL_CALL,
        action_target="send_email",
        action_content="send an email to the team",
    )
    domain = classifier.classify_domain(action)
    assert domain == "communication"
    assert classifier._last_tier == 1


@pytest.mark.asyncio
async def test_tier2_fact_domains():
    """Tier 2b: recent_fact_domains=['financial','financial','data_access'] -> financial."""
    classifier = AutonomyClassifier(tool_registry=ToolDomainRegistry())
    action = GuardCheckInput(
        action_type=GuardActionType.MESSAGE_SEND,
        action_content="check the balance",
    )
    domain = classifier.classify_domain(
        action,
        recent_fact_domains=["financial", "financial", "data_access"],
    )
    assert domain == "financial"
    assert classifier._last_tier == 2


@pytest.mark.asyncio
async def test_tier3_procedure_domain():
    """Tier 3: active_procedure_domains (after fact domains, per Amendment 7.2 M5)."""
    classifier = AutonomyClassifier(tool_registry=ToolDomainRegistry())
    action = GuardCheckInput(
        action_type=GuardActionType.MESSAGE_SEND,
        action_content="some generic action",
    )
    domain = classifier.classify_domain(
        action,
        active_procedure_domains=["resource"],
    )
    assert domain == "resource"
    assert classifier._last_tier == 3  # Amendment 7.2: procedure domains moved to Tier 3


@pytest.mark.asyncio
async def test_fallback_uncategorized():
    """No matches across all tiers -> 'uncategorized' (Amendment 7.2 M4)."""
    classifier = AutonomyClassifier(tool_registry=ToolDomainRegistry())
    action = GuardCheckInput(
        action_type=GuardActionType.MESSAGE_SEND,
        action_content="xyzzy plugh",  # no keywords match
    )
    domain = classifier.classify_domain(action)
    assert domain == "uncategorized"  # Amendment 7.2: changed from "general"
    assert classifier._last_tier == 0


@pytest.mark.asyncio
async def test_tier1_priority():
    """Tier 1 (tool mapping) takes priority over Tier 2 (fact domains)."""
    classifier = AutonomyClassifier(tool_registry=ToolDomainRegistry())
    action = GuardCheckInput(
        action_type=GuardActionType.TOOL_CALL,
        action_target="send_email",
        action_content="send financial report",
    )
    domain = classifier.classify_domain(
        action,
        recent_fact_domains=["financial", "financial"],
    )
    # Tier 1 wins: send_email -> communication, not financial from facts
    assert domain == "communication"
    assert classifier._last_tier == 1


# ---------------------------------------------------------------------------
# Autonomy floor composition with guard engine tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_autonomous_plus_pass(
    mock_redis, mock_embedding, mock_graph, trace_ledger,
    redis_keys, metrics, approval_queue,
):
    """AUTONOMOUS floor + safety PASS -> outcome PASS."""
    classifier = AutonomyClassifier(tool_registry=ToolDomainRegistry(), redis=mock_redis, redis_keys=redis_keys)
    engine = _build_engine(mock_redis, mock_embedding, mock_graph, trace_ledger,
                           redis_keys, metrics, approval_queue, classifier)

    sid = _sid()
    await engine.load_session_rules(sid, "coding")
    state = engine._sessions[sid]
    # Set all domains to AUTONOMOUS
    state.guard_policy.autonomy = AutonomyPolicy(
        default_level=AutonomyLevel.AUTONOMOUS,
    )

    messages = [_msg("hello world")]
    result = await engine.preflight_check(sid, messages)
    assert result.outcome == GuardOutcome.PASS


@pytest.mark.asyncio
async def test_hard_stop_always_blocks(
    mock_redis, mock_embedding, mock_graph, trace_ledger,
    redis_keys, metrics, approval_queue,
):
    """HARD_STOP floor -> BLOCK regardless of safety outcome."""
    classifier = AutonomyClassifier(tool_registry=ToolDomainRegistry(), redis=mock_redis, redis_keys=redis_keys)
    engine = _build_engine(mock_redis, mock_embedding, mock_graph, trace_ledger,
                           redis_keys, metrics, approval_queue, classifier)

    sid = _sid()
    await engine.load_session_rules(sid, "coding")
    state = engine._sessions[sid]
    # Set all domains to HARD_STOP
    state.guard_policy.autonomy = AutonomyPolicy(
        default_level=AutonomyLevel.HARD_STOP,
    )

    messages = [_msg("hello world")]
    result = await engine.preflight_check(sid, messages)
    assert result.outcome == GuardOutcome.BLOCK


@pytest.mark.asyncio
async def test_approve_first_pending(
    mock_redis, mock_embedding, mock_graph, trace_ledger,
    redis_keys, metrics, approval_queue,
):
    """APPROVE_FIRST + no existing approval -> REQUIRE_APPROVAL."""
    classifier = AutonomyClassifier(tool_registry=ToolDomainRegistry(), redis=mock_redis, redis_keys=redis_keys)
    engine = _build_engine(mock_redis, mock_embedding, mock_graph, trace_ledger,
                           redis_keys, metrics, approval_queue, classifier)

    sid = _sid()
    await engine.load_session_rules(sid, "coding")
    state = engine._sessions[sid]
    state.guard_policy.autonomy = AutonomyPolicy(
        default_level=AutonomyLevel.APPROVE_FIRST,
    )

    messages = [_msg("process this request")]
    result = await engine.preflight_check(sid, messages)
    assert result.outcome == GuardOutcome.REQUIRE_APPROVAL


@pytest.mark.asyncio
async def test_approve_first_approved(
    mock_redis, mock_embedding, mock_graph, trace_ledger,
    redis_keys, metrics, approval_queue,
):
    """APPROVE_FIRST with existing APPROVED approval -> floor becomes PASS."""
    classifier = AutonomyClassifier(tool_registry=ToolDomainRegistry(), redis=mock_redis, redis_keys=redis_keys)
    engine = _build_engine(mock_redis, mock_embedding, mock_graph, trace_ledger,
                           redis_keys, metrics, approval_queue, classifier)

    sid = _sid()
    await engine.load_session_rules(sid, "coding")
    state = engine._sessions[sid]
    state.guard_policy.autonomy = AutonomyPolicy(
        default_level=AutonomyLevel.APPROVE_FIRST,
    )

    # Mock find_matching to return an approved request
    from elephantbroker.schemas.guards import ApprovalRequest
    approved_req = ApprovalRequest(
        session_id=sid,
        action_summary="process this request",
        status=ApprovalStatus.APPROVED,
        approval_message="looks good",
    )
    approval_queue.find_matching = AsyncMock(return_value=approved_req)

    messages = [_msg("process this request")]
    result = await engine.preflight_check(sid, messages)
    # Approved means floor drops to PASS; safety layers should also pass
    assert result.outcome in (GuardOutcome.PASS, GuardOutcome.INFORM)


@pytest.mark.asyncio
async def test_inform_floor(
    mock_redis, mock_embedding, mock_graph, trace_ledger,
    redis_keys, metrics, approval_queue,
):
    """INFORM + PASS -> final outcome INFORM (floor wins over safety PASS)."""
    classifier = AutonomyClassifier(tool_registry=ToolDomainRegistry(), redis=mock_redis, redis_keys=redis_keys)
    engine = _build_engine(mock_redis, mock_embedding, mock_graph, trace_ledger,
                           redis_keys, metrics, approval_queue, classifier)

    sid = _sid()
    await engine.load_session_rules(sid, "coding")
    state = engine._sessions[sid]
    state.guard_policy.autonomy = AutonomyPolicy(
        default_level=AutonomyLevel.INFORM,
    )

    messages = [_msg("hello world")]
    result = await engine.preflight_check(sid, messages)
    assert result.outcome == GuardOutcome.INFORM
