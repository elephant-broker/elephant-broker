"""Integration tests for the full guard pipeline (15 tests).

Each test wires the real RedLineGuardEngine with real StaticRuleRegistry,
SemanticGuardIndex, and AutonomyClassifier, but mocks external infrastructure.
"""
from __future__ import annotations

import uuid

import pytest

from elephantbroker.runtime.guards.engine import RedLineGuardEngine
from elephantbroker.runtime.guards.autonomy import AutonomyClassifier, ToolDomainRegistry
from elephantbroker.runtime.guards.approval_queue import ApprovalQueue
from elephantbroker.schemas.context import AgentMessage
from elephantbroker.schemas.guards import (
    GuardOutcome,
    GuardResult,
    StaticRule,
    StaticRulePatternType,
    StructuralValidatorSpec,
    GuardActionType,
)
from elephantbroker.schemas.config import GuardConfig, HitlConfig, StrictnessPreset
from elephantbroker.schemas.profile import GuardPolicy, ProfilePolicy


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sid() -> uuid.UUID:
    return uuid.uuid4()


def _msg(content: str, role: str = "assistant", **meta) -> AgentMessage:
    return AgentMessage(role=role, content=content, metadata=meta)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_block_prohibited_keyword(guard_engine):
    """Message containing 'rm -rf /' triggers the builtin_rm_rf regex rule -> BLOCK."""
    sid = _sid()
    await guard_engine.load_session_rules(sid, "coding")
    messages = [_msg("please run rm -rf / on the server")]
    result = await guard_engine.preflight_check(sid, messages)
    assert result.outcome == GuardOutcome.BLOCK
    assert any("rm" in r.lower() for r in result.matched_rules)


@pytest.mark.asyncio
async def test_block_prohibited_regex(guard_engine):
    """Message with 'DROP TABLE users' matches builtin_drop_table regex -> BLOCK."""
    sid = _sid()
    await guard_engine.load_session_rules(sid, "coding")
    messages = [_msg("execute DROP TABLE users")]
    result = await guard_engine.preflight_check(sid, messages)
    assert result.outcome == GuardOutcome.BLOCK
    assert any("drop_table" in r for r in result.matched_rules)


@pytest.mark.asyncio
async def test_block_tool_target_rule(guard_engine):
    """Message with tool_name=shell_exec in metadata -> REQUIRE_APPROVAL via builtin."""
    sid = _sid()
    await guard_engine.load_session_rules(sid, "coding")
    messages = [_msg("run ls -la", tool_name="shell_exec")]
    result = await guard_engine.preflight_check(sid, messages)
    assert result.outcome in (GuardOutcome.REQUIRE_APPROVAL, GuardOutcome.BLOCK)


@pytest.mark.asyncio
async def test_pass_innocuous_message(guard_engine):
    """An innocuous message like 'list the project files' -> PASS."""
    sid = _sid()
    await guard_engine.load_session_rules(sid, "coding")
    messages = [_msg("list the project files")]
    result = await guard_engine.preflight_check(sid, messages)
    # coding profile has INFORM as default autonomy, so outcome may be INFORM or PASS
    assert result.outcome in (GuardOutcome.PASS, GuardOutcome.INFORM)


@pytest.mark.asyncio
async def test_warn_credential_keyword(guard_engine):
    """Message with 'password' matches builtin_password_pattern -> non-PASS outcome.

    Under the coding profile (medium strictness), WARN is non-definitive so
    the safety result falls through as PASS. The autonomy floor (INFORM for
    'general' domain) then dominates via max_outcome. The key assertion is
    that the password rule matched and the result is at least INFORM.
    """
    sid = _sid()
    await guard_engine.load_session_rules(sid, "coding")
    messages = [_msg("here is the password: abc123")]
    result = await guard_engine.preflight_check(sid, messages)
    assert result.outcome != GuardOutcome.PASS
    # Verify the password rule was actually matched
    assert any("password" in r for r in result.matched_rules)


@pytest.mark.asyncio
async def test_multiple_rules_most_restrictive_wins(guard_engine):
    """Message matching both a WARN rule and a BLOCK rule -> BLOCK wins."""
    sid = _sid()
    await guard_engine.load_session_rules(sid, "coding")
    # Contains both "password" (WARN) and "DROP TABLE" (BLOCK)
    messages = [_msg("password for admin: x; now DROP TABLE users")]
    result = await guard_engine.preflight_check(sid, messages)
    assert result.outcome == GuardOutcome.BLOCK


@pytest.mark.asyncio
async def test_semantic_catches_rephrased(guard_engine, mock_embedding):
    """Semantic layer catches rephrased red-line via high-similarity mock embeddings."""
    sid = _sid()

    # Make embedding service return the SAME vector for both exemplar and query,
    # so cosine similarity = 1.0 (above default 0.80 threshold).
    vec = [0.5] * 1536
    mock_embedding.embed_text = lambda text: vec

    await guard_engine.load_session_rules(sid, "coding")
    # Manually inject exemplars into the session state
    state = guard_engine._sessions[sid]
    await state.semantic_index.build_index(["delete customer data"])
    # Force exemplar embeddings to match
    state.semantic_index._exemplar_embeddings = [vec]

    messages = [_msg("erase user database")]
    result = await guard_engine.preflight_check(sid, messages)
    assert result.outcome != GuardOutcome.PASS


@pytest.mark.asyncio
async def test_semantic_passes_unrelated(guard_engine, mock_embedding):
    """Unrelated text should pass the semantic layer when embeddings diverge."""
    sid = _sid()

    call_count = 0

    async def divergent_embed(text):
        nonlocal call_count
        call_count += 1
        # Return orthogonal vectors so cosine similarity ~ 0
        if "weather" in text.lower():
            return [1.0] + [0.0] * 1535
        return [0.0] + [1.0] + [0.0] * 1534

    mock_embedding.embed_text = divergent_embed

    await guard_engine.load_session_rules(sid, "coding")
    state = guard_engine._sessions[sid]
    await state.semantic_index.build_index(["delete all production data"])
    # Reset exemplar embeddings so they get recomputed with our mock
    state.semantic_index._exemplar_embeddings = []

    messages = [_msg("check weather forecast for tomorrow")]
    result = await guard_engine.preflight_check(sid, messages)
    # Outcome should be PASS or INFORM (autonomy), NOT BLOCK
    assert result.outcome in (GuardOutcome.PASS, GuardOutcome.INFORM)


@pytest.mark.asyncio
async def test_bm25_blocks_exact_match(guard_engine, mock_embedding):
    """BM25 scores an exact text match near 1.0 -> BLOCK (above threshold)."""
    sid = _sid()

    # Make embed_text return dissimilar vectors so only BM25 triggers
    async def dissimilar_embed(text):
        return [0.0] * 1536

    mock_embedding.embed_text = dissimilar_embed

    await guard_engine.load_session_rules(sid, "coding")
    state = guard_engine._sessions[sid]
    await state.semantic_index.build_index(["delete customer data permanently"])

    # Send the exact exemplar text
    messages = [_msg("delete customer data permanently")]
    result = await guard_engine.preflight_check(sid, messages)
    assert result.outcome == GuardOutcome.BLOCK
    assert any("bm25" in r for r in result.matched_rules)


@pytest.mark.asyncio
async def test_structural_missing_field(guard_engine):
    """Structural validator requires 'approval_token'; absent -> outcome_on_fail."""
    sid = _sid()
    await guard_engine.load_session_rules(sid, "coding")

    state = guard_engine._sessions[sid]
    state.structural_validators = [
        StructuralValidatorSpec(
            id="require_approval_token",
            action_type=GuardActionType.TOOL_CALL,
            required_fields=["approval_token"],
            outcome_on_fail=GuardOutcome.BLOCK,
            description="Tool calls must include approval_token",
        )
    ]

    # Message with tool_name but no approval_token in metadata
    messages = [_msg("deploy now", tool_name="deploy_service")]
    result = await guard_engine.preflight_check(sid, messages)
    assert result.outcome == GuardOutcome.BLOCK
    assert "require_approval_token" in result.matched_rules


@pytest.mark.asyncio
async def test_structural_field_present(guard_engine):
    """Structural validator passes when required field is present in metadata."""
    sid = _sid()
    await guard_engine.load_session_rules(sid, "coding")

    state = guard_engine._sessions[sid]
    state.structural_validators = [
        StructuralValidatorSpec(
            id="require_approval_token",
            action_type=GuardActionType.TOOL_CALL,
            required_fields=["approval_token"],
            outcome_on_fail=GuardOutcome.BLOCK,
            description="Tool calls must include approval_token",
        )
    ]

    # Message with tool_name AND approval_token in metadata
    messages = [_msg("deploy now", tool_name="safe_tool", approval_token="tok-123")]
    result = await guard_engine.preflight_check(sid, messages)
    # Should not be BLOCK from structural validator
    assert result.outcome in (GuardOutcome.PASS, GuardOutcome.INFORM)


@pytest.mark.asyncio
async def test_pipeline_stops_at_first_definitive(guard_engine):
    """BLOCK at Layer 1 means layers 2-5 are not checked."""
    sid = _sid()
    await guard_engine.load_session_rules(sid, "coding")

    messages = [_msg("execute DROP TABLE accounts")]
    result = await guard_engine.preflight_check(sid, messages)

    assert result.outcome == GuardOutcome.BLOCK
    assert result.triggered_layer == 1
    # Layer 2 (semantic) should not have been evaluated as definitive
    layer_2_results = [lr for lr in result.layer_results if lr.layer == 2]
    assert len(layer_2_results) == 0 or not layer_2_results[0].definitive


@pytest.mark.asyncio
async def test_strictness_loose(guard_engine, trace_ledger, mock_embedding,
                                mock_graph, mock_redis, redis_keys, metrics,
                                approval_queue):
    """Loose strictness uses relaxed thresholds: BM25 multiplier=1.5, no structural validators."""
    from elephantbroker.runtime.profiles.registry import ProfileRegistry

    # Create engine with no profile registry so we can set a custom policy
    classifier = AutonomyClassifier(
        tool_registry=ToolDomainRegistry(),
        redis=mock_redis,
        redis_keys=redis_keys,
    )
    engine = RedLineGuardEngine(
        trace_ledger=trace_ledger,
        embedding_service=mock_embedding,
        graph=mock_graph,
        llm_client=None,
        profile_registry=None,
        redis=mock_redis,
        config=GuardConfig(),
        gateway_id="test-gw",
        redis_keys=redis_keys,
        metrics=metrics,
        approval_queue=approval_queue,
        autonomy_classifier=classifier,
    )

    sid = _sid()
    await engine.load_session_rules(sid, "coding")

    # Override the guard policy to use loose strictness
    state = engine._sessions[sid]
    state.guard_policy.preflight_check_strictness = "loose"

    # A password mention would WARN under medium; under loose it stays WARN
    # (no upgrade). The structural validators are disabled under loose.
    state.structural_validators = [
        StructuralValidatorSpec(
            id="test_struct",
            action_type=GuardActionType.TOOL_CALL,
            required_fields=["token"],
            outcome_on_fail=GuardOutcome.BLOCK,
        )
    ]

    # Even though the structural validator would fail, loose disables it
    messages = [_msg("run task", tool_name="safe_tool")]
    result = await engine.preflight_check(sid, messages)
    # Structural validator should NOT have triggered (disabled under loose)
    assert "test_struct" not in result.matched_rules


@pytest.mark.asyncio
async def test_strictness_strict(guard_engine):
    """Strict strictness upgrades WARN to REQUIRE_APPROVAL."""
    sid = _sid()
    await guard_engine.load_session_rules(sid, "coding")

    state = guard_engine._sessions[sid]
    state.guard_policy.preflight_check_strictness = "strict"

    # "password" matches WARN rule; strict upgrades to require_approval
    messages = [_msg("the password is hunter2")]
    result = await guard_engine.preflight_check(sid, messages)
    assert result.outcome in (GuardOutcome.REQUIRE_APPROVAL, GuardOutcome.BLOCK)


@pytest.mark.asyncio
async def test_disabled_engine_returns_pass(
    trace_ledger, mock_embedding, mock_graph, mock_redis,
    redis_keys, metrics, approval_queue,
):
    """GuardConfig(enabled=False) -> PASS always, regardless of input."""
    classifier = AutonomyClassifier(
        tool_registry=ToolDomainRegistry(),
        redis=mock_redis,
        redis_keys=redis_keys,
    )
    engine = RedLineGuardEngine(
        trace_ledger=trace_ledger,
        embedding_service=mock_embedding,
        graph=mock_graph,
        llm_client=None,
        profile_registry=None,
        redis=mock_redis,
        config=GuardConfig(enabled=False),
        gateway_id="test-gw",
        redis_keys=redis_keys,
        metrics=metrics,
        approval_queue=approval_queue,
        autonomy_classifier=classifier,
    )

    sid = _sid()
    await engine.load_session_rules(sid, "coding")
    messages = [_msg("DROP TABLE users; rm -rf /")]
    result = await engine.preflight_check(sid, messages)
    assert result.outcome == GuardOutcome.PASS
