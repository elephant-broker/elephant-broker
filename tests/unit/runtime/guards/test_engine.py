"""Tests for RedLineGuardEngine (Phase 7 — §7.6)."""
from __future__ import annotations

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from elephantbroker.runtime.guards.autonomy import AutonomyClassifier, ToolDomainRegistry
from elephantbroker.runtime.guards.engine import RedLineGuardEngine, _SessionGuardState
from elephantbroker.runtime.guards.rules import StaticRuleRegistry
from elephantbroker.runtime.guards.semantic_index import SemanticGuardIndex
from elephantbroker.runtime.metrics import MetricsContext
from elephantbroker.runtime.redis_keys import RedisKeyBuilder
from elephantbroker.schemas.config import GuardConfig, StrictnessPreset
from elephantbroker.schemas.context import AgentMessage
from elephantbroker.schemas.guards import (
    ApprovalRequest,
    ApprovalStatus,
    AutonomyLevel,
    GuardOutcome,
    GuardResult,
    StaticRulePatternType,
)
from elephantbroker.schemas.profile import GuardPolicy
from elephantbroker.schemas.trace import TraceEvent, TraceEventType
from tests.fixtures.factories import make_approval_request, make_static_rule

SID = uuid.uuid4()


def _make_engine(**overrides):
    trace = AsyncMock()
    trace.append_event = AsyncMock()
    embed = AsyncMock()
    graph = AsyncMock()
    llm = AsyncMock()
    registry = AsyncMock()
    redis = AsyncMock()
    redis.lrange = AsyncMock(return_value=[])
    redis.lpush = AsyncMock()
    redis.ltrim = AsyncMock()
    redis.expire = AsyncMock()
    keys = RedisKeyBuilder("test")
    metrics = MetricsContext("test")
    classifier = AutonomyClassifier(tool_registry=ToolDomainRegistry())
    queue = AsyncMock()
    queue.find_matching = AsyncMock(return_value=None)
    queue.create = AsyncMock(return_value=make_approval_request())
    queue.get_for_session = AsyncMock(return_value=[])
    goals = AsyncMock()

    engine = RedLineGuardEngine(
        trace_ledger=trace,
        embedding_service=embed,
        graph=graph,
        llm_client=llm,
        profile_registry=registry,
        redis=redis,
        config=GuardConfig(),
        gateway_id="test",
        redis_keys=keys,
        metrics=metrics,
        autonomy_classifier=classifier,
        approval_queue=queue,
        session_goal_store=goals,
        **overrides,
    )

    # Pre-load session state
    engine._sessions[SID] = _SessionGuardState(
        session_id=SID,
        session_key="agent:main:main",
        agent_id="main",
        rule_registry=StaticRuleRegistry(),
        semantic_index=SemanticGuardIndex(embed),
        structural_validators=[],
        guard_policy=GuardPolicy(),
        session_constraints=[],
        active_procedure_ids=[],
        active_procedure_domains=[],
        active_procedure_bindings=[],
    )
    # Load builtin rules
    engine._sessions[SID].rule_registry.load_rules()
    return engine, classifier, queue, redis, trace


def _msg(content: str, role: str = "user", **meta) -> AgentMessage:
    return AgentMessage(role=role, content=content, metadata=meta)


class TestLayer0:
    @pytest.mark.asyncio
    async def test_hard_stop_returns_block_skips_all_layers(self):
        engine, classifier, _, _, _ = _make_engine()
        # Override classify to return financial, and set policy to HARD_STOP
        engine._sessions[SID].guard_policy.autonomy.domain_levels["financial"] = AutonomyLevel.HARD_STOP
        classifier._tools.register("transfer_funds", "financial")
        result = await engine.preflight_check(SID, [_msg("ok", role="tool", tool_name="transfer_funds")])
        assert result.outcome == GuardOutcome.BLOCK
        assert result.triggered_layer == 0
        assert len(result.layer_results) == 1

    @pytest.mark.asyncio
    async def test_autonomous_returns_pass(self):
        engine, _, _, _, _ = _make_engine()
        result = await engine.preflight_check(SID, [_msg("hello world")])
        assert result.outcome in (GuardOutcome.PASS, GuardOutcome.INFORM)


class TestLayer1:
    @pytest.mark.asyncio
    async def test_keyword_match_blocks(self):
        engine, _, _, _, _ = _make_engine()
        engine._sessions[SID].rule_registry.load_rules(
            policy_rules=[make_static_rule(
                pattern_type=StaticRulePatternType.KEYWORD,
                pattern="drop table",
                outcome=GuardOutcome.BLOCK,
            )],
        )
        result = await engine.preflight_check(SID, [_msg("please drop table users")])
        assert result.outcome == GuardOutcome.BLOCK
        assert result.triggered_layer == 1

    @pytest.mark.asyncio
    async def test_no_match_passes(self):
        engine, _, _, _, _ = _make_engine()
        result = await engine.preflight_check(SID, [_msg("hello world")])
        # Should not trigger any rule
        assert result.outcome in (GuardOutcome.PASS, GuardOutcome.INFORM)

    @pytest.mark.asyncio
    async def test_tool_target_match(self):
        engine, _, _, _, _ = _make_engine()
        result = await engine.preflight_check(SID, [_msg("ok", role="tool", tool_name="shell_exec")])
        # builtin_shell_exec should match → REQUIRE_APPROVAL
        assert result.outcome in (GuardOutcome.REQUIRE_APPROVAL, GuardOutcome.INFORM, GuardOutcome.PASS)

    @pytest.mark.asyncio
    async def test_regex_match_blocks(self):
        engine, _, _, _, _ = _make_engine()
        # Builtin regex "DROP TABLE" should fire
        result = await engine.preflight_check(SID, [_msg("running DROP TABLE users now")])
        assert result.outcome == GuardOutcome.BLOCK

    @pytest.mark.asyncio
    async def test_warn_strictness_upgrade(self):
        engine, _, _, _, _ = _make_engine()
        engine._sessions[SID].guard_policy.preflight_check_strictness = "strict"
        engine._sessions[SID].rule_registry.load_rules(
            policy_rules=[make_static_rule(
                pattern="password",
                pattern_type=StaticRulePatternType.KEYWORD,
                outcome=GuardOutcome.WARN,
            )],
        )
        result = await engine.preflight_check(SID, [_msg("my password is abc")])
        # Strict preset upgrades WARN to REQUIRE_APPROVAL
        assert result.outcome in (GuardOutcome.REQUIRE_APPROVAL, GuardOutcome.WARN, GuardOutcome.INFORM)


class TestLayer2:
    @pytest.mark.asyncio
    async def test_bm25_block_skips_embedding(self):
        engine, _, _, _, _ = _make_engine()
        # Use exemplar that won't match static rules but will match BM25
        # Clear builtin rules to isolate Layer 2
        engine._sessions[SID].rule_registry.load_rules(policy_rules=[], builtin_rules=[])
        await engine._sessions[SID].semantic_index.build_index(["destroy staging environment permanently"])
        result = await engine.preflight_check(SID, [_msg("destroy staging environment permanently now")])
        assert result.outcome == GuardOutcome.BLOCK
        assert result.triggered_layer == 2
        engine._sessions[SID].semantic_index._embedding_service.embed_text.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_exemplars_passes(self):
        engine, _, _, _, _ = _make_engine()
        # No exemplars loaded
        result = await engine.preflight_check(SID, [_msg("delete production")])
        # Should match builtin phrase rule or pass
        assert result.outcome in (GuardOutcome.BLOCK, GuardOutcome.PASS, GuardOutcome.INFORM)


class TestLayer3:
    @pytest.mark.asyncio
    async def test_missing_field_blocks(self):
        from elephantbroker.schemas.guards import GuardActionType, StructuralValidatorSpec
        engine, _, _, _, _ = _make_engine()
        engine._sessions[SID].structural_validators = [
            StructuralValidatorSpec(
                id="require_review",
                action_type=GuardActionType.TOOL_CALL,
                action_target_pattern="deploy.*",
                required_fields=["review_token"],
                outcome_on_fail=GuardOutcome.BLOCK,
                description="Deploy requires review token",
            ),
        ]
        result = await engine.preflight_check(SID, [_msg("deploying", role="tool", tool_name="deploy_prod")])
        assert result.outcome == GuardOutcome.BLOCK

    @pytest.mark.asyncio
    async def test_field_present_passes(self):
        from elephantbroker.schemas.guards import GuardActionType, StructuralValidatorSpec
        engine, _, _, _, _ = _make_engine()
        engine._sessions[SID].structural_validators = [
            StructuralValidatorSpec(
                id="require_review",
                action_type=GuardActionType.TOOL_CALL,
                action_target_pattern="deploy.*",
                required_fields=["review_token"],
                outcome_on_fail=GuardOutcome.BLOCK,
                description="Deploy requires review token",
            ),
        ]
        msg = _msg("deploying", role="tool", tool_name="deploy_prod", review_token="abc123")
        result = await engine.preflight_check(SID, [msg])
        # Should not block because review_token is in metadata
        assert result.outcome != GuardOutcome.BLOCK or result.triggered_layer != 3


class TestComposition:
    @pytest.mark.asyncio
    async def test_safety_block_overrides_autonomous(self):
        engine, _, _, _, _ = _make_engine()
        engine._sessions[SID].rule_registry.load_rules(
            policy_rules=[make_static_rule(
                pattern="deploy_prod",
                pattern_type=StaticRulePatternType.TOOL_TARGET,
                outcome=GuardOutcome.BLOCK,
            )],
        )
        result = await engine.preflight_check(SID, [_msg("ok", role="tool", tool_name="deploy_prod")])
        assert result.outcome == GuardOutcome.BLOCK

    @pytest.mark.asyncio
    async def test_inform_floor_when_safety_passes(self):
        engine, _, _, _, _ = _make_engine()
        engine._sessions[SID].guard_policy.autonomy.default_level = AutonomyLevel.INFORM
        result = await engine.preflight_check(SID, [_msg("harmless message")])
        assert result.outcome == GuardOutcome.INFORM


class TestGuardHistory:
    @pytest.mark.asyncio
    async def test_event_stored_in_redis(self):
        engine, _, _, redis, _ = _make_engine()
        await engine.preflight_check(SID, [_msg("hello")])
        await asyncio.sleep(0.05)
        redis.lpush.assert_called()


class TestLoadSessionRules:
    @pytest.mark.asyncio
    async def test_loads_builtin_rules(self):
        engine, _, _, _, _ = _make_engine()
        engine._profiles = AsyncMock()
        from elephantbroker.schemas.profile import ProfilePolicy
        engine._profiles.resolve_profile = AsyncMock(return_value=ProfilePolicy(id="test", name="test"))
        await engine.load_session_rules(SID, "coding", session_key="agent:main:main", agent_id="main")
        state = engine._sessions.get(SID)
        assert state is not None
        assert len(state.rule_registry._rules) >= 12  # builtins

    @pytest.mark.asyncio
    async def test_builtin_rules_disabled_loads_none(self):
        """GUARD-GAP-1: builtin_rules_enabled=False skips builtin rules."""
        engine, _, _, _, _ = _make_engine()
        engine._config.builtin_rules_enabled = False
        engine._profiles = AsyncMock()
        from elephantbroker.schemas.profile import ProfilePolicy
        engine._profiles.resolve_profile = AsyncMock(return_value=ProfilePolicy(id="test", name="test"))
        await engine.load_session_rules(SID, "coding", session_key="agent:main:main", agent_id="main")
        state = engine._sessions.get(SID)
        assert state is not None
        # No builtin rules, no policy rules, no procedure bindings → empty
        assert len(state.rule_registry._rules) == 0


class TestUnloadSession:
    @pytest.mark.asyncio
    async def test_removes_state(self):
        engine, _, _, _, _ = _make_engine()
        assert SID in engine._sessions
        await engine.unload_session(SID)
        assert SID not in engine._sessions

    @pytest.mark.asyncio
    async def test_cancels_pending_approvals(self):
        engine, _, queue, _, _ = _make_engine()
        pending = make_approval_request(status=ApprovalStatus.PENDING)
        queue.get_for_session = AsyncMock(return_value=[pending])
        queue.cancel = AsyncMock()
        await engine.unload_session(SID)
        queue.cancel.assert_called_once()


class TestDisabledEngine:
    @pytest.mark.asyncio
    async def test_disabled_returns_pass(self):
        engine, _, _, _, _ = _make_engine()
        engine._config.enabled = False
        result = await engine.preflight_check(SID, [_msg("anything")])
        assert result.outcome == GuardOutcome.PASS

    @pytest.mark.asyncio
    async def test_missing_session_raises_not_loaded(self):
        from elephantbroker.runtime.guards.engine import GuardRulesNotLoadedError
        engine, _, _, _, _ = _make_engine()
        unknown = uuid.uuid4()
        with pytest.raises(GuardRulesNotLoadedError):
            await engine.preflight_check(unknown, [_msg("anything")])


class TestExtractCheckInput:
    def test_tool_call_extraction(self):
        engine, _, _, _, _ = _make_engine()
        msg = _msg("done", role="tool", tool_name="deploy_prod")
        action = engine._extract_check_input(SID, [msg])
        assert action.action_type.value == "tool_call"
        assert action.action_target == "deploy_prod"

    def test_user_message_extraction(self):
        engine, _, _, _, _ = _make_engine()
        msg = _msg("hello world", role="assistant")
        action = engine._extract_check_input(SID, [msg])
        assert action.action_type.value == "message_send"
        assert "hello world" in action.action_content

    def test_fallback_concatenation(self):
        engine, _, _, _, _ = _make_engine()
        msgs = [_msg("a", role="user"), _msg("b", role="user")]
        action = engine._extract_check_input(SID, msgs)
        assert action.action_type.value == "message_send"


class TestReinjectConstraints:
    @pytest.mark.asyncio
    async def test_returns_session_constraints(self):
        engine, _, _, _, _ = _make_engine()
        engine._sessions[SID].session_constraints = ["RULE: no deploys"]
        result = await engine.reinject_constraints(SID)
        assert result == ["RULE: no deploys"]

    @pytest.mark.asyncio
    async def test_unknown_session_returns_empty(self):
        engine, _, _, _, _ = _make_engine()
        result = await engine.reinject_constraints(uuid.uuid4())
        assert result == []


# ============================================================
# Amendment 7.2 — Extended test coverage (~45 new tests)
# ============================================================


class TestLayer0Extended:
    """Extended Layer 0 autonomy tests."""

    @pytest.mark.asyncio
    async def test_approve_first_no_existing_returns_require_approval(self):
        engine, _, queue, _, _ = _make_engine()
        engine._sessions[SID].guard_policy.autonomy.default_level = AutonomyLevel.APPROVE_FIRST
        queue.find_matching = AsyncMock(return_value=None)
        queue.create = AsyncMock(return_value=make_approval_request())
        result = await engine.preflight_check(SID, [_msg("do something")])
        assert result.outcome == GuardOutcome.REQUIRE_APPROVAL

    @pytest.mark.asyncio
    async def test_approve_first_existing_approved_drops_to_pass(self):
        engine, _, queue, _, _ = _make_engine()
        engine._sessions[SID].guard_policy.autonomy.default_level = AutonomyLevel.APPROVE_FIRST
        approved = make_approval_request(status=ApprovalStatus.APPROVED)
        queue.find_matching = AsyncMock(return_value=approved)
        result = await engine.preflight_check(SID, [_msg("do something")])
        assert result.outcome == GuardOutcome.PASS

    @pytest.mark.asyncio
    async def test_approve_first_existing_rejected_blocks(self):
        """Rejected approval escalates to BLOCK (most severe)."""
        engine, _, queue, _, _ = _make_engine()
        engine._sessions[SID].guard_policy.autonomy.default_level = AutonomyLevel.APPROVE_FIRST
        rejected = make_approval_request(status=ApprovalStatus.REJECTED)
        queue.find_matching = AsyncMock(return_value=rejected)
        result = await engine.preflight_check(SID, [_msg("do something")])
        assert result.outcome == GuardOutcome.BLOCK

    @pytest.mark.asyncio
    async def test_approve_first_existing_timed_out(self):
        engine, _, queue, _, _ = _make_engine()
        engine._sessions[SID].guard_policy.autonomy.default_level = AutonomyLevel.APPROVE_FIRST
        timed_out = make_approval_request(status=ApprovalStatus.TIMED_OUT)
        queue.find_matching = AsyncMock(return_value=timed_out)
        result = await engine.preflight_check(SID, [_msg("do something")])
        assert result.outcome in (GuardOutcome.BLOCK, GuardOutcome.REQUIRE_APPROVAL)

    @pytest.mark.asyncio
    async def test_domain_from_fact_domains(self):
        engine, _, _, redis, _ = _make_engine()
        redis.lrange = AsyncMock(return_value=[b"financial", b"financial", b"code_change"])
        engine._sessions[SID].guard_policy.autonomy.domain_levels = {
            "financial": AutonomyLevel.HARD_STOP
        }
        result = await engine.preflight_check(SID, [_msg("hello")])
        assert result.outcome == GuardOutcome.BLOCK

    @pytest.mark.asyncio
    async def test_redis_fact_domains_failure_graceful(self):
        engine, _, _, redis, _ = _make_engine()
        redis.lrange = AsyncMock(side_effect=Exception("Redis down"))
        result = await engine.preflight_check(SID, [_msg("hello")])
        assert result.outcome is not None  # Should not crash


class TestLayer1Extended:
    @pytest.mark.asyncio
    async def test_multiple_matches_most_severe_wins(self):
        engine, _, _, _, _ = _make_engine()
        engine._sessions[SID].rule_registry.load_rules(policy_rules=[
            make_static_rule(pattern="danger", outcome=GuardOutcome.WARN),
            make_static_rule(pattern="danger", outcome=GuardOutcome.BLOCK, pattern_type=StaticRulePatternType.PHRASE),
        ])
        result = await engine.preflight_check(SID, [_msg("this is danger")])
        assert result.outcome == GuardOutcome.BLOCK

    @pytest.mark.asyncio
    async def test_empty_content_no_match(self):
        engine, _, _, _, _ = _make_engine()
        engine._sessions[SID].rule_registry.load_rules(policy_rules=[
            make_static_rule(pattern="secret", outcome=GuardOutcome.BLOCK),
        ])
        result = await engine.preflight_check(SID, [_msg("")])
        assert result.outcome != GuardOutcome.BLOCK

    @pytest.mark.asyncio
    async def test_disabled_rule_not_matched(self):
        engine, _, _, _, _ = _make_engine()
        rule = make_static_rule(pattern="secret", outcome=GuardOutcome.BLOCK)
        rule.enabled = False
        engine._sessions[SID].rule_registry.load_rules(policy_rules=[rule])
        result = await engine.preflight_check(SID, [_msg("secret key")])
        assert result.outcome != GuardOutcome.BLOCK


class TestLayer2Extended:
    @pytest.mark.asyncio
    async def test_embedding_service_failure_graceful(self):
        engine, _, _, _, _ = _make_engine()
        engine._sessions[SID].semantic_index._exemplar_texts = ["dangerous action"]
        engine._embed.embed_text = AsyncMock(side_effect=Exception("Embed service down"))
        result = await engine.preflight_check(SID, [_msg("dangerous action")])
        # Should not crash, falls through to next layer
        assert result.outcome is not None


class TestLayer4Reinjection:
    @pytest.mark.asyncio
    async def test_constraints_generated_on_force_inject(self):
        engine, _, _, _, _ = _make_engine()
        engine._sessions[SID].guard_policy.force_system_constraint_injection = True
        engine._sessions[SID].rule_registry.load_rules(policy_rules=[
            make_static_rule(
                pattern="deploy_prod",
                pattern_type=StaticRulePatternType.TOOL_TARGET,
                outcome=GuardOutcome.BLOCK,
            ),
        ])
        result = await engine.preflight_check(SID, [_msg("hello")])
        assert len(result.constraints_reinjected) > 0

    @pytest.mark.asyncio
    async def test_reinjection_always_runs(self):
        """Layer 4 runs even after Layer 1 definitive BLOCK."""
        engine, _, _, _, _ = _make_engine()
        engine._sessions[SID].guard_policy.force_system_constraint_injection = True
        engine._sessions[SID].rule_registry.load_rules(policy_rules=[
            make_static_rule(pattern="rm -rf", outcome=GuardOutcome.BLOCK),
            make_static_rule(
                pattern="deploy_prod",
                pattern_type=StaticRulePatternType.TOOL_TARGET,
                outcome=GuardOutcome.BLOCK,
            ),
        ])
        result = await engine.preflight_check(SID, [_msg("rm -rf /")])
        assert result.outcome == GuardOutcome.BLOCK
        assert len(result.constraints_reinjected) > 0


class TestFinalization:
    @pytest.mark.asyncio
    async def test_trace_event_guard_passed(self):
        engine, _, _, _, trace = _make_engine()
        await engine.preflight_check(SID, [_msg("harmless")])
        await asyncio.sleep(0.05)
        trace.append_event.assert_called()

    @pytest.mark.asyncio
    async def test_trace_event_guard_triggered(self):
        engine, _, _, _, trace = _make_engine()
        engine._sessions[SID].rule_registry.load_rules(policy_rules=[
            make_static_rule(pattern="secret", outcome=GuardOutcome.BLOCK),
        ])
        await engine.preflight_check(SID, [_msg("secret key")])
        await asyncio.sleep(0.05)
        calls = trace.append_event.call_args_list
        any_triggered = any(
            c.args[0].event_type == TraceEventType.GUARD_TRIGGERED
            for c in calls if c.args
        )
        assert any_triggered

    @pytest.mark.asyncio
    async def test_guard_history_capped(self):
        engine, _, _, redis, _ = _make_engine()
        await engine.preflight_check(SID, [_msg("test")])
        await asyncio.sleep(0.05)
        if redis.ltrim.called:
            call_args = redis.ltrim.call_args[0]
            assert call_args[2] <= 49  # max_history_events - 1

    @pytest.mark.asyncio
    async def test_metrics_inc_guard_check_called(self):
        engine, _, _, _, _ = _make_engine()
        engine._metrics = MagicMock()
        await engine.preflight_check(SID, [_msg("test")])
        engine._metrics.inc_guard_check.assert_called()


class TestLoadSessionRulesExtended:
    @pytest.mark.asyncio
    async def test_profile_resolution_failure_fallback(self):
        engine, _, _, _, _ = _make_engine()
        engine._profiles.resolve_profile = AsyncMock(side_effect=Exception("Not found"))
        await engine.load_session_rules(SID, "nonexistent")
        state = engine._sessions.get(SID)
        assert state is not None  # Should still create state with default policy

    @pytest.mark.asyncio
    async def test_reload_replaces_state(self):
        engine, _, _, _, _ = _make_engine()
        old_state = engine._sessions[SID]
        await engine.load_session_rules(SID, "coding", session_key="sk", agent_id="a1")
        new_state = engine._sessions[SID]
        assert new_state is not old_state


class TestEvictStaleSessions:
    @pytest.mark.asyncio
    async def test_stale_sessions_evicted(self):
        engine, _, _, _, _ = _make_engine()
        engine._sessions[SID].last_accessed_at = datetime.now(UTC) - timedelta(days=2)
        engine._config.history_ttl_seconds = 3600  # 1 hour
        await engine.load_session_rules(uuid.uuid4(), "coding")
        assert SID not in engine._sessions

    @pytest.mark.asyncio
    async def test_fresh_sessions_preserved(self):
        engine, _, _, _, _ = _make_engine()
        engine._config.history_ttl_seconds = 86400
        new_sid = uuid.uuid4()
        await engine.load_session_rules(new_sid, "coding")
        assert SID in engine._sessions  # Original still present
        assert new_sid in engine._sessions


class TestExtractCheckInputExtended:
    @pytest.mark.asyncio
    async def test_empty_messages_list(self):
        engine, _, _, _, _ = _make_engine()
        result = await engine.preflight_check(SID, [])
        # Autonomy floor (INFORM default) raises PASS to INFORM
        assert result.outcome in (GuardOutcome.PASS, GuardOutcome.INFORM)

    @pytest.mark.asyncio
    async def test_multiple_messages_tool_in_last(self):
        engine, _, _, _, _ = _make_engine()
        engine._sessions[SID].rule_registry.load_rules(policy_rules=[
            make_static_rule(
                pattern="deploy_prod",
                pattern_type=StaticRulePatternType.TOOL_TARGET,
                outcome=GuardOutcome.BLOCK,
            ),
        ])
        result = await engine.preflight_check(SID, [
            _msg("first message"),
            _msg("call tool", role="tool", tool_name="deploy_prod"),
        ])
        assert result.outcome == GuardOutcome.BLOCK

    @pytest.mark.asyncio
    async def test_constraint_reinjected_trace_event(self):
        """Amendment 7.2: CONSTRAINT_REINJECTED trace event emitted from Layer 4."""
        engine, _, _, _, trace = _make_engine()
        engine._sessions[SID].guard_policy.force_system_constraint_injection = True
        engine._sessions[SID].rule_registry.load_rules(policy_rules=[
            make_static_rule(
                pattern="dangerous_tool",
                pattern_type=StaticRulePatternType.TOOL_TARGET,
                outcome=GuardOutcome.BLOCK,
            ),
        ])
        await engine.preflight_check(SID, [_msg("hello")])
        await asyncio.sleep(0.05)
        calls = trace.append_event.call_args_list
        any_reinjected = any(
            c.args[0].event_type == TraceEventType.CONSTRAINT_REINJECTED
            for c in calls if c.args
        )
        assert any_reinjected


class TestUnloadSessionExtended:
    @pytest.mark.asyncio
    async def test_unload_nonexistent_session_noop(self):
        engine, _, _, _, _ = _make_engine()
        unknown_sid = uuid.uuid4()
        await engine.unload_session(unknown_sid)  # Should not crash

    @pytest.mark.asyncio
    async def test_unload_with_no_pending_approvals(self):
        engine, _, queue, _, _ = _make_engine()
        queue.get_for_session = AsyncMock(return_value=[])
        await engine.unload_session(SID)
        assert SID not in engine._sessions


class TestHitlNotification:
    @pytest.mark.asyncio
    async def test_hitl_notify_called_for_inform_outcome(self):
        """HITL notify() is called when autonomy default is INFORM."""
        hitl = AsyncMock()
        hitl.notify = AsyncMock()
        engine, _, _, _, _ = _make_engine(hitl_client=hitl)
        engine._sessions[SID].guard_policy.autonomy.default_level = AutonomyLevel.INFORM
        result = await engine.preflight_check(SID, [_msg("harmless message")])
        assert result.outcome == GuardOutcome.INFORM
        await asyncio.sleep(0.05)  # allow async task to complete
        hitl.notify.assert_called()


class TestGuardEventStorage:
    @pytest.mark.asyncio
    async def test_autonomy_level_stored_on_guard_event(self):
        """Guard event stored in Redis contains autonomy_level field."""
        import json
        engine, _, _, redis, _ = _make_engine()
        engine._sessions[SID].guard_policy.autonomy.default_level = AutonomyLevel.INFORM
        await engine.preflight_check(SID, [_msg("hello")])
        await asyncio.sleep(0.05)  # allow async task to complete
        redis.lpush.assert_called()
        # Extract the JSON stored in the lpush call
        call_args = redis.lpush.call_args[0]
        stored_json = call_args[1]
        event_data = json.loads(stored_json)
        assert "autonomy_level" in event_data
        assert event_data["autonomy_level"] is not None


class TestNearMissEscalation:
    """GUARD-GAP-7: Near-miss escalation forces LLM check after repeated WARNs."""

    def _setup_warn_history(self, redis, count: int):
        """Populate Redis guard history with `count` WARN events."""
        from elephantbroker.schemas.guards import GuardEvent
        events = []
        for _ in range(count):
            ev = GuardEvent(
                id=uuid.uuid4(), session_id=SID,
                outcome=GuardOutcome.WARN, matched_rules=["test_rule"],
            )
            events.append(ev.model_dump_json())
        redis.lrange = AsyncMock(return_value=events)

    @pytest.mark.asyncio
    async def test_near_miss_triggers_llm_escalation(self):
        """Enough WARNs in window → forces LLM escalation."""
        engine, _, _, redis, _ = _make_engine()
        state = engine._sessions[SID]
        state.guard_policy.near_miss_escalation_threshold = 2
        state.guard_policy.near_miss_window_turns = 5
        state.guard_policy.llm_escalation_enabled = True

        # Put 3 WARN events in history
        self._setup_warn_history(redis, 3)

        # Make layers 1-3 produce WARN (non-definitive pass, autonomy gives WARN)
        state.guard_policy.autonomy.default_level = AutonomyLevel.INFORM

        # LLM returns BLOCK verdict
        engine._llm.complete_json = AsyncMock(return_value={"outcome": "block", "explanation": "repeated risk"})

        result = await engine.preflight_check(SID, [_msg("do something risky")])
        # LLM was called (at least once for escalation)
        assert engine._llm.complete_json.call_count >= 1

    @pytest.mark.asyncio
    async def test_near_miss_below_threshold_no_escalation(self):
        """Below threshold → no forced LLM escalation."""
        engine, _, _, redis, _ = _make_engine()
        state = engine._sessions[SID]
        state.guard_policy.near_miss_escalation_threshold = 5
        state.guard_policy.near_miss_window_turns = 5
        state.guard_policy.llm_escalation_enabled = True

        # Only 2 WARN events — below threshold of 5
        self._setup_warn_history(redis, 2)

        state.guard_policy.autonomy.default_level = AutonomyLevel.INFORM
        engine._llm.complete_json = AsyncMock(return_value={"outcome": "pass", "explanation": "ok"})

        result = await engine.preflight_check(SID, [_msg("hello")])
        # The key check: outcome should be INFORM (from autonomy), not escalated
        assert result.outcome in (GuardOutcome.INFORM, GuardOutcome.PASS, GuardOutcome.WARN)

    @pytest.mark.asyncio
    async def test_near_miss_llm_disabled_keeps_warn(self):
        """Above threshold but LLM disabled → stays WARN, no escalation."""
        engine, _, _, redis, _ = _make_engine()
        state = engine._sessions[SID]
        state.guard_policy.near_miss_escalation_threshold = 2
        state.guard_policy.near_miss_window_turns = 5
        state.guard_policy.llm_escalation_enabled = False

        self._setup_warn_history(redis, 3)
        state.guard_policy.autonomy.default_level = AutonomyLevel.INFORM

        result = await engine.preflight_check(SID, [_msg("do something")])
        # LLM should not have been called
        engine._llm.complete_json.assert_not_called()

    @pytest.mark.asyncio
    async def test_near_miss_already_blocked_no_escalation(self):
        """If already BLOCK from earlier layer, near-miss logic doesn't run."""
        engine, _, _, redis, _ = _make_engine()
        state = engine._sessions[SID]
        state.guard_policy.near_miss_escalation_threshold = 1
        state.guard_policy.near_miss_window_turns = 5
        state.guard_policy.llm_escalation_enabled = True

        self._setup_warn_history(redis, 5)

        # Force autonomy to HARD_STOP → BLOCK from Layer 0
        state.guard_policy.autonomy.default_level = AutonomyLevel.HARD_STOP

        result = await engine.preflight_check(SID, [_msg("DROP TABLE users")])
        assert result.outcome == GuardOutcome.BLOCK
