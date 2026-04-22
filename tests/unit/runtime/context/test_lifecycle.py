"""Tests for ContextLifecycle orchestrator."""
from __future__ import annotations

import hashlib
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elephantbroker.runtime.context.lifecycle import (
    TOOL_ALIASES,
    ContextLifecycle,
)
from elephantbroker.runtime.metrics import MetricsContext
from elephantbroker.runtime.redis_keys import RedisKeyBuilder
from elephantbroker.schemas.artifact import SessionArtifact
from elephantbroker.schemas.config import ElephantBrokerConfig
from elephantbroker.schemas.context import (
    AfterTurnParams,
    AgentMessage,
    AssembleParams,
    AssembleResult,
    BootstrapParams,
    CompactParams,
    CompactResult,
    IngestBatchParams,
    IngestParams,
    SessionContext,
    SubagentEndedParams,
    SubagentSpawnParams,
    SystemPromptOverlay,
)
from elephantbroker.schemas.goal import GoalState
from elephantbroker.schemas.trace import TraceEventType
from elephantbroker.schemas.working_set import WorkingSetSnapshot
from tests.fixtures.factories import (
    make_profile_policy,
    make_session_context,
    make_working_set_item,
    make_working_set_snapshot,
)

SK = "agent:main:main"
SID = str(uuid.uuid4())


def _make_redis_mock():
    """Create a Redis mock with safe defaults (returns None for .get())."""
    redis = AsyncMock()
    redis.get = AsyncMock(return_value=None)
    redis.setex = AsyncMock()
    redis.sadd = AsyncMock()
    redis.expire = AsyncMock()
    redis.rpush = AsyncMock()
    redis.lrange = AsyncMock(return_value=[])
    redis.delete = AsyncMock()
    return redis


def _make_lifecycle(*, fresh_bootstrap: bool = False, **overrides):
    """Build a ContextLifecycle with all dependencies mocked.

    Args:
        fresh_bootstrap: If True, session_store.get returns None so bootstrap
            creates a new SessionContext instead of reusing an existing one (GF-15).
    """
    from elephantbroker.schemas.profile import SuccessfulUseThresholds

    profile = make_profile_policy()

    profile_reg = AsyncMock()
    profile_reg.resolve_profile = AsyncMock(return_value=profile)
    # T-2: scanner threshold resolver is sync. AsyncMock's auto-generated
    # attribute returns a MagicMock that doesn't behave as a real
    # SuccessfulUseThresholds instance (its gate attrs are MagicMocks,
    # not floats). Stub with a real default instance so every scanner
    # path in after_turn / _track_successful_use works out-of-the-box.
    # Individual tests can override by reassigning on the returned lc.
    profile_reg.effective_successful_use_thresholds = MagicMock(
        return_value=SuccessfulUseThresholds(),
    )

    session_store = AsyncMock()
    session_store.get = AsyncMock(return_value=None if fresh_bootstrap else make_session_context())
    session_store.save = AsyncMock()
    session_store.delete = AsyncMock()
    session_store.get_compact_state = AsyncMock(return_value=None)
    session_store._effective_ttl = lambda p: 86400

    defaults = {
        "profile_registry": profile_reg,
        "trace_ledger": AsyncMock(),
        "session_context_store": session_store,
        "session_artifact_store": AsyncMock(),
        "working_set_manager": AsyncMock(),
        "context_assembler": AsyncMock(),
        "compaction_engine": AsyncMock(),
        "guard_engine": AsyncMock(),
        "memory_store": AsyncMock(),
        "turn_ingest": AsyncMock(),
        "session_goal_store": AsyncMock(),
        "procedure_engine": AsyncMock(),
        "redis": _make_redis_mock(),
        "redis_keys": RedisKeyBuilder("test"),
        "metrics": MagicMock(spec=MetricsContext),
        "config": ElephantBrokerConfig(),
    }
    defaults.update(overrides)
    return ContextLifecycle(**defaults)


# ======================================================================
# 1-4  bootstrap
# ======================================================================


class TestBootstrap:
    async def test_creates_session_context_with_profile_and_org_team(self):
        """#1: bootstrap creates SessionContext with profile + org_id/team_id."""
        config = ElephantBrokerConfig()
        config.gateway.org_id = "org-42"
        config.gateway.team_id = "team-7"
        lc = _make_lifecycle(fresh_bootstrap=True, config=config)

        params = BootstrapParams(session_key=SK, session_id=SID, profile_name="coding")
        result = await lc.bootstrap(params)

        assert result.bootstrapped is True
        saved_ctx: SessionContext = lc._session_store.save.call_args[0][0]
        assert saved_ctx.org_id == "org-42"
        assert saved_ctx.team_ids == ["team-7"]
        assert saved_ctx.profile.name == "Test Profile"

    async def test_bootstrap_subagent_with_parent_session_key(self):
        """#2: bootstrap with is_subagent + parent_session_key."""
        lc = _make_lifecycle(fresh_bootstrap=True)
        params = BootstrapParams(
            session_key=SK, session_id=SID,
            is_subagent=True, parent_session_key="agent:parent:main",
        )
        result = await lc.bootstrap(params)

        assert result.bootstrapped is True
        saved_ctx: SessionContext = lc._session_store.save.call_args[0][0]
        assert saved_ctx.parent_session_key == "agent:parent:main"

    async def test_bootstrap_subagent_redis_fallback(self):
        """#3: bootstrap subagent Redis fallback when parent_session_key not provided."""
        redis = AsyncMock()
        redis.get = AsyncMock(return_value="agent:redis-parent:main")
        lc = _make_lifecycle(fresh_bootstrap=True, redis=redis)

        params = BootstrapParams(
            session_key=SK, session_id=SID,
            is_subagent=True, parent_session_key=None,
        )
        result = await lc.bootstrap(params)

        assert result.bootstrapped is True
        saved_ctx: SessionContext = lc._session_store.save.call_args[0][0]
        assert saved_ctx.parent_session_key == "agent:redis-parent:main"
        redis.get.assert_called_once()

    async def test_bootstrap_sets_agent_key(self):
        """bootstrap with agent_key in params sets lifecycle._agent_key."""
        lc = _make_lifecycle(fresh_bootstrap=True)
        params = BootstrapParams(
            session_key=SK, session_id=SID, agent_key="gw-test:agent-42",
        )
        await lc.bootstrap(params)
        assert lc._agent_key == "gw-test:agent-42"

    async def test_bootstrap_hasattr_guard_check_present(self):
        """#4: bootstrap calls guard.load_session_rules when method exists."""
        guard = AsyncMock()
        guard.load_session_rules = AsyncMock()
        lc = _make_lifecycle(fresh_bootstrap=True, guard_engine=guard)

        params = BootstrapParams(session_key=SK, session_id=SID)
        await lc.bootstrap(params)

        guard.load_session_rules.assert_called_once()
        call_kwargs = guard.load_session_rules.call_args.kwargs
        assert str(call_kwargs["session_id"]) == SID
        assert call_kwargs["profile_name"] == "coding"
        assert call_kwargs["session_key"] == SK

    async def test_bootstrap_hasattr_guard_check_absent(self):
        """#4b: bootstrap skips guard call when load_session_rules is absent."""
        guard = MagicMock(spec=[])  # no attributes
        lc = _make_lifecycle(fresh_bootstrap=True, guard_engine=guard)

        params = BootstrapParams(session_key=SK, session_id=SID)
        result = await lc.bootstrap(params)
        assert result.bootstrapped is True

    async def test_bootstrap_calls_procedure_restore(self):
        """#5: bootstrap calls procedure_engine.restore_executions() (TD-6)."""
        proc_engine = AsyncMock()
        proc_engine.restore_executions = AsyncMock()
        lc = _make_lifecycle(fresh_bootstrap=True, procedure_engine=proc_engine)

        params = BootstrapParams(session_key=SK, session_id=SID)
        await lc.bootstrap(params)
        proc_engine.restore_executions.assert_called_once_with(SK, SID)


# ======================================================================
# 5-9  ingest / ingest_batch
# ======================================================================


class TestIngest:
    async def test_ingest_delegates_to_ingest_batch(self):
        """#5: ingest delegates to ingest_batch (degraded mode)."""
        lc = _make_lifecycle()
        msg = AgentMessage(role="user", content="hello")
        params = IngestParams(session_id=SID, session_key=SK, message=msg)
        result = await lc.ingest(params)
        assert result.ingested is True


class TestIngestBatch:
    async def test_stores_messages_in_redis_list(self):
        """#6: ingest_batch stores messages in Redis LIST."""
        redis = AsyncMock()
        lc = _make_lifecycle(redis=redis)

        msgs = [AgentMessage(role="user", content="hello")]
        params = IngestBatchParams(session_id=SID, session_key=SK, messages=msgs)
        await lc.ingest_batch(params)

        redis.rpush.assert_called_once()
        key = redis.rpush.call_args[0][0]
        assert "session_messages" in key
        redis.expire.assert_called_once()

    async def test_captures_tool_artifacts_over_200_chars(self):
        """#7: ingest_batch captures tool artifacts (>200 chars)."""
        artifact_store = AsyncMock()
        artifact_store.get_by_hash = AsyncMock(return_value=None)
        lc = _make_lifecycle(session_artifact_store=artifact_store)

        long_content = "x" * 300
        msgs = [AgentMessage(role="tool", content=long_content, name="grep")]
        params = IngestBatchParams(session_id=SID, session_key=SK, messages=msgs)
        await lc.ingest_batch(params)

        artifact_store.store.assert_called_once()
        stored_artifact: SessionArtifact = artifact_store.store.call_args[0][2]
        assert stored_artifact.tool_name == "grep"
        assert stored_artifact.content == long_content

    async def test_skips_short_tool_output(self):
        """#7b: short tool output (<200 chars) is NOT captured."""
        artifact_store = AsyncMock()
        lc = _make_lifecycle(session_artifact_store=artifact_store)

        msgs = [AgentMessage(role="tool", content="short", name="grep")]
        params = IngestBatchParams(session_id=SID, session_key=SK, messages=msgs)
        await lc.ingest_batch(params)

        artifact_store.store.assert_not_called()

    async def test_dedup_artifacts_by_hash(self):
        """#8: ingest_batch dedup artifacts by hash."""
        artifact_store = AsyncMock()
        artifact_store.get_by_hash = AsyncMock(
            return_value=SessionArtifact(tool_name="x", content="x"),
        )
        lc = _make_lifecycle(session_artifact_store=artifact_store)

        content = "x" * 300
        msgs = [AgentMessage(role="tool", content=content, name="grep")]
        params = IngestBatchParams(session_id=SID, session_key=SK, messages=msgs)
        await lc.ingest_batch(params)

        content_hash = hashlib.sha256(content.encode()).hexdigest()
        artifact_store.get_by_hash.assert_called_once_with(SK, SID, content_hash)
        artifact_store.store.assert_not_called()

    async def test_annotation_eb_turn(self):
        """#9: ingest_batch annotates messages with eb_turn metadata."""
        ctx = make_session_context(turn_count=5)
        session_store = AsyncMock()
        session_store.get = AsyncMock(return_value=ctx)
        session_store.save = AsyncMock()
        session_store._effective_ttl = lambda p: 86400
        lc = _make_lifecycle(session_context_store=session_store)

        msgs = [AgentMessage(role="user", content="hello")]
        params = IngestBatchParams(session_id=SID, session_key=SK, messages=msgs)
        await lc.ingest_batch(params)

        assert msgs[0].metadata["eb_turn"] == "5"
        # turn_count is NOT incremented here — after_turn() is the sole increment path
        # (PR #11 R1 TODO #2: avoid double-increment in simulation mode)
        assert ctx.turn_count == 5


class TestIngestBatchTouchKeys:
    """Amendment 6.1: TTL refresh on every ingest_batch."""

    async def test_ingest_batch_calls_touch_session_keys(self):
        """Amendment 6.1: ingest_batch must refresh all session key TTLs."""
        pipe = MagicMock()
        pipe.expire = MagicMock()
        pipe.execute = AsyncMock(return_value=[1] * 10)
        redis = _make_redis_mock()
        redis.pipeline = MagicMock(return_value=pipe)
        lc = _make_lifecycle(redis=redis)

        params = IngestBatchParams(session_key=SK, session_id=SID, messages=[AgentMessage(role="user", content="hello")])
        await lc.ingest_batch(params)

        redis.pipeline.assert_called()
        assert pipe.expire.call_count == 10  # 8 base + 2 Phase 7 (guard_history, fact_domains)

    async def test_ingest_batch_subagent_touches_parent(self):
        """Subagent sessions must include parent keys in touch."""
        pipe = MagicMock()
        pipe.expire = MagicMock()
        pipe.execute = AsyncMock(return_value=[1] * 11)
        redis = _make_redis_mock()
        redis.pipeline = MagicMock(return_value=pipe)
        redis.get = AsyncMock(return_value="agent:parent:main")  # parent found

        # Session context with parent
        ctx = make_session_context(parent_session_key="agent:parent:main")
        session_store = AsyncMock()
        session_store.get = AsyncMock(return_value=ctx)
        session_store.save = AsyncMock()
        session_store._effective_ttl = lambda p: 86400

        lc = _make_lifecycle(redis=redis, session_context_store=session_store)
        params = IngestBatchParams(session_key=SK, session_id=SID, messages=[AgentMessage(role="user", content="hi")])
        await lc.ingest_batch(params)

        assert pipe.expire.call_count == 11  # 10 base (8+2 Phase 7) + session_parent

    async def test_ingest_batch_touch_failure_does_not_block(self):
        """Touch failure must not prevent ingest from completing."""
        redis = _make_redis_mock()
        redis.pipeline = MagicMock(side_effect=Exception("pipeline error"))
        lc = _make_lifecycle(redis=redis)

        params = IngestBatchParams(session_key=SK, session_id=SID, messages=[AgentMessage(role="user", content="hello")])
        result = await lc.ingest_batch(params)
        assert result.ingested_count == 1  # Still succeeds

    async def test_ingest_batch_passes_profile_to_artifact_store(self):
        """BUG-4 caller: artifact store must receive session profile."""
        artifact_store = AsyncMock()
        artifact_store.get_by_hash = AsyncMock(return_value=None)
        artifact_store.store = AsyncMock()

        lc = _make_lifecycle(session_artifact_store=artifact_store)

        # Tool message with content >= 200 chars triggers _should_capture_artifact
        tool_msg = AgentMessage(role="tool", content="x" * 300, name="psql")
        params = IngestBatchParams(session_key=SK, session_id=SID, messages=[tool_msg])
        await lc.ingest_batch(params)

        # Verify store was called with profile kwarg
        assert artifact_store.store.called
        _, kwargs = artifact_store.store.call_args
        assert "profile" in kwargs


# ======================================================================
# 10-15  assemble
# ======================================================================


class TestAssemble:
    async def test_calls_wsm_and_assembler_returns_result(self):
        """#10: assemble calls WSM + assembler, returns AssembleResult."""
        snapshot = make_working_set_snapshot(items=[])
        wsm = AsyncMock()
        wsm.build_working_set = AsyncMock(return_value=snapshot)
        assembler = AsyncMock()
        assembler.assemble_from_snapshot = AsyncMock(
            return_value=AssembleResult(estimated_tokens=100),
        )
        goal_store = AsyncMock()
        goal_store.get_goals = AsyncMock(return_value=[])
        lc = _make_lifecycle(
            working_set_manager=wsm,
            context_assembler=assembler,
            session_goal_store=goal_store,
        )

        params = AssembleParams(session_id=SID, session_key=SK)
        result = await lc.assemble(params)

        wsm.build_working_set.assert_called_once()
        assembler.assemble_from_snapshot.assert_called_once()
        assert result.estimated_tokens == 100

    async def test_resolves_budget_min_profile_openclaw_window(self):
        """#11: assemble resolves budget min(profile, openclaw, window)."""
        profile = make_profile_policy()
        profile.budgets.max_prompt_tokens = 10000
        ctx = make_session_context(profile=profile)
        session_store = AsyncMock()
        session_store.get = AsyncMock(return_value=ctx)
        session_store.save = AsyncMock()
        session_store._effective_ttl = lambda p: 86400
        wsm = AsyncMock()
        wsm.build_working_set = AsyncMock(return_value=make_working_set_snapshot(items=[]))
        assembler = AsyncMock()
        assembler.assemble_from_snapshot = AsyncMock(return_value=AssembleResult())
        goal_store = AsyncMock()
        goal_store.get_goals = AsyncMock(return_value=[])
        lc = _make_lifecycle(
            session_context_store=session_store,
            working_set_manager=wsm,
            context_assembler=assembler,
            session_goal_store=goal_store,
        )

        # openclaw=5000, window=200000*0.15=30000, profile=10000 -> min=5000
        params = AssembleParams(
            session_id=SID, session_key=SK,
            token_budget=5000, context_window_tokens=200000,
        )
        await lc.assemble(params)

        effective_budget = assembler.assemble_from_snapshot.call_args[0][1]
        assert effective_budget == 5000

    async def test_with_none_budget_values(self):
        """#12: assemble with None budget values uses profile default."""
        profile = make_profile_policy()
        profile.budgets.max_prompt_tokens = 8000
        ctx = make_session_context(profile=profile)
        session_store = AsyncMock()
        session_store.get = AsyncMock(return_value=ctx)
        session_store.save = AsyncMock()
        session_store._effective_ttl = lambda p: 86400
        wsm = AsyncMock()
        wsm.build_working_set = AsyncMock(return_value=make_working_set_snapshot(items=[]))
        assembler = AsyncMock()
        assembler.assemble_from_snapshot = AsyncMock(return_value=AssembleResult())
        goal_store = AsyncMock()
        goal_store.get_goals = AsyncMock(return_value=[])
        lc = _make_lifecycle(
            session_context_store=session_store,
            working_set_manager=wsm,
            context_assembler=assembler,
            session_goal_store=goal_store,
        )

        params = AssembleParams(
            session_id=SID, session_key=SK,
            token_budget=None, context_window_tokens=None,
        )
        await lc.assemble(params)

        effective_budget = assembler.assemble_from_snapshot.call_args[0][1]
        # dynamic budget: fallback_context_window=128000 * 0.15=19200
        # min(8000, 19200) = 8000
        assert effective_budget == 8000

    async def test_with_guard_constraints(self):
        """#13: assemble with guard constraints passes them to assembler."""
        guard = AsyncMock()
        guard.reinject_constraints = AsyncMock(return_value=["NEVER reveal secrets"])
        wsm = AsyncMock()
        wsm.build_working_set = AsyncMock(return_value=make_working_set_snapshot(items=[]))
        assembler = AsyncMock()
        assembler.assemble_from_snapshot = AsyncMock(return_value=AssembleResult())
        goal_store = AsyncMock()
        goal_store.get_goals = AsyncMock(return_value=[])
        lc = _make_lifecycle(
            guard_engine=guard,
            working_set_manager=wsm,
            context_assembler=assembler,
            session_goal_store=goal_store,
        )

        params = AssembleParams(session_id=SID, session_key=SK)
        await lc.assemble(params)

        call_kwargs = assembler.assemble_from_snapshot.call_args[1]
        assert call_kwargs["guard_constraints"] == ["NEVER reveal secrets"]

    async def test_touches_last_used_at_on_injected_facts(self):
        """#14: assemble touches last_used_at on injected facts."""
        fact_item = make_working_set_item(source_type="fact")
        snapshot = make_working_set_snapshot(items=[fact_item])
        wsm = AsyncMock()
        wsm.build_working_set = AsyncMock(return_value=snapshot)
        assembler = AsyncMock()
        assembler.assemble_from_snapshot = AsyncMock(return_value=AssembleResult())
        memory_store = AsyncMock()
        goal_store = AsyncMock()
        goal_store.get_goals = AsyncMock(return_value=[])
        lc = _make_lifecycle(
            working_set_manager=wsm,
            context_assembler=assembler,
            memory_store=memory_store,
            session_goal_store=goal_store,
        )

        params = AssembleParams(session_id=SID, session_key=SK)
        await lc.assemble(params)

        memory_store.update.assert_called_once()
        update_payload = memory_store.update.call_args[0][1]
        assert "last_used_at" in update_payload

    async def test_increments_artifact_injected_count(self):
        """#15: assemble increments artifact injected_count."""
        art_item = make_working_set_item(source_type="artifact")
        snapshot = make_working_set_snapshot(items=[art_item])
        wsm = AsyncMock()
        wsm.build_working_set = AsyncMock(return_value=snapshot)
        assembler = AsyncMock()
        assembler.assemble_from_snapshot = AsyncMock(return_value=AssembleResult())
        artifact_store = AsyncMock()
        goal_store = AsyncMock()
        goal_store.get_goals = AsyncMock(return_value=[])
        lc = _make_lifecycle(
            working_set_manager=wsm,
            context_assembler=assembler,
            session_artifact_store=artifact_store,
            session_goal_store=goal_store,
        )

        params = AssembleParams(session_id=SID, session_key=SK)
        await lc.assemble(params)

        artifact_store.increment_injected.assert_called_once_with(
            SK, SID, str(art_item.source_id),
        )


    async def test_assemble_caches_snapshot_to_redis(self):
        """assemble() writes snapshot to Redis ws_snapshot key for build_overlay/after_turn."""
        snapshot = make_working_set_snapshot(items=[])
        wsm = AsyncMock()
        wsm.build_working_set = AsyncMock(return_value=snapshot)
        assembler = AsyncMock()
        assembler.assemble_from_snapshot = AsyncMock(return_value=AssembleResult())
        redis = _make_redis_mock()
        lc = _make_lifecycle(working_set_manager=wsm, context_assembler=assembler, redis=redis)

        await lc.assemble(AssembleParams(session_id=SID, session_key=SK))

        # Verify snapshot was cached to Redis
        redis.setex.assert_called()
        cached_calls = [c for c in redis.setex.call_args_list
                        if "ws_snapshot" in str(c)]
        assert len(cached_calls) >= 1


# ======================================================================
# 16  build_overlay
# ======================================================================


class TestBuildOverlay:
    async def test_returns_empty_overlay_when_no_snapshot(self):
        """#16: build_overlay returns SystemPromptOverlay (empty if no snapshot)."""
        redis = AsyncMock()
        redis.get = AsyncMock(return_value=None)
        lc = _make_lifecycle(redis=redis)

        result = await lc.build_overlay(SK, SID)

        assert isinstance(result, SystemPromptOverlay)
        assert result.system_prompt is None

    async def test_returns_empty_overlay_when_no_session(self):
        """#16b: no session context -> empty overlay."""
        session_store = AsyncMock()
        session_store.get = AsyncMock(return_value=None)
        lc = _make_lifecycle(session_context_store=session_store)

        result = await lc.build_overlay(SK, SID)
        assert isinstance(result, SystemPromptOverlay)

    async def test_delegates_to_assembler_with_cached_snapshot(self):
        """#16c: with cached snapshot, delegates to assembler."""
        snapshot = make_working_set_snapshot(items=[])
        redis = AsyncMock()
        redis.get = AsyncMock(return_value=snapshot.model_dump_json())
        assembler = AsyncMock()
        expected = SystemPromptOverlay(system_prompt="injected rules")
        assembler.build_system_overlay_from_items = AsyncMock(return_value=expected)
        goal_store = AsyncMock()
        goal_store.get_goals = AsyncMock(return_value=[])
        lc = _make_lifecycle(
            redis=redis,
            context_assembler=assembler,
            session_goal_store=goal_store,
        )

        result = await lc.build_overlay(SK, SID)

        assert result.system_prompt == "injected rules"
        assembler.build_system_overlay_from_items.assert_called_once()


# ======================================================================
# 17-21  compact
# ======================================================================


class TestCompact:
    async def test_reads_messages_from_redis(self):
        """#17: compact reads messages from Redis (not from params)."""
        redis = AsyncMock()
        msg = AgentMessage(role="user", content="test message")
        redis.lrange = AsyncMock(return_value=[msg.model_dump_json()])
        compaction = AsyncMock()
        compaction.compact_with_context = AsyncMock(
            return_value=CompactResult(ok=True, compacted=True),
        )
        goal_store = AsyncMock()
        goal_store.get_goals = AsyncMock(return_value=[])
        lc = _make_lifecycle(
            redis=redis,
            compaction_engine=compaction,
            session_goal_store=goal_store,
        )

        params = CompactParams(session_id=SID, session_key=SK)
        await lc.compact(params)

        redis.lrange.assert_called_once()
        ctx_arg = compaction.compact_with_context.call_args[0][0]
        assert len(ctx_arg.messages) == 1
        assert ctx_arg.messages[0].content == "test message"

    async def test_no_messages_returns_not_compacted(self):
        """#18: compact with no messages -> compacted=False."""
        redis = AsyncMock()
        redis.lrange = AsyncMock(return_value=[])
        compaction = AsyncMock()
        lc = _make_lifecycle(redis=redis, compaction_engine=compaction)

        params = CompactParams(session_id=SID, session_key=SK)
        result = await lc.compact(params)

        assert result.compacted is False
        assert result.ok is True
        compaction.compact_with_context.assert_not_called()

    async def test_delegates_to_compaction_engine(self):
        """#19: compact delegates to compaction engine."""
        redis = AsyncMock()
        msg = AgentMessage(role="user", content="test")
        redis.lrange = AsyncMock(return_value=[msg.model_dump_json()])
        compaction = AsyncMock()
        compaction.compact_with_context = AsyncMock(
            return_value=CompactResult(ok=True, compacted=True, reason="done"),
        )
        goal_store = AsyncMock()
        goal_store.get_goals = AsyncMock(return_value=[])
        lc = _make_lifecycle(
            redis=redis,
            compaction_engine=compaction,
            session_goal_store=goal_store,
        )

        params = CompactParams(session_id=SID, session_key=SK)
        result = await lc.compact(params)

        assert result.compacted is True
        assert result.reason == "done"

    async def test_resets_fact_and_goal_injection_state(self):
        """#20: compact post-compaction resets fact_last_injection_turn and goal_inject_history."""
        ctx = make_session_context(
            fact_last_injection_turn={"f1": 2, "f2": 3},
            goal_inject_history={"g1": {"turn": 1, "status": "active"}},
            compact_count=0,
        )
        session_store = AsyncMock()
        session_store.get = AsyncMock(return_value=ctx)
        session_store.save = AsyncMock()
        session_store._effective_ttl = lambda p: 86400
        redis = AsyncMock()
        msg = AgentMessage(role="user", content="test")
        redis.lrange = AsyncMock(return_value=[msg.model_dump_json()])
        compaction = AsyncMock()
        compaction.compact_with_context = AsyncMock(
            return_value=CompactResult(ok=True, compacted=True),
        )
        goal_store = AsyncMock()
        goal_store.get_goals = AsyncMock(return_value=[])
        lc = _make_lifecycle(
            session_context_store=session_store,
            redis=redis,
            compaction_engine=compaction,
            session_goal_store=goal_store,
        )

        params = CompactParams(session_id=SID, session_key=SK)
        await lc.compact(params)

        assert ctx.fact_last_injection_turn == {}
        assert ctx.goal_inject_history == {}

    async def test_increments_compact_count(self):
        """#21: compact increments compact_count."""
        ctx = make_session_context(compact_count=2)
        session_store = AsyncMock()
        session_store.get = AsyncMock(return_value=ctx)
        session_store.save = AsyncMock()
        session_store._effective_ttl = lambda p: 86400
        redis = AsyncMock()
        msg = AgentMessage(role="user", content="test")
        redis.lrange = AsyncMock(return_value=[msg.model_dump_json()])
        compaction = AsyncMock()
        compaction.compact_with_context = AsyncMock(
            return_value=CompactResult(ok=True, compacted=True),
        )
        goal_store = AsyncMock()
        goal_store.get_goals = AsyncMock(return_value=[])
        lc = _make_lifecycle(
            session_context_store=session_store,
            redis=redis,
            compaction_engine=compaction,
            session_goal_store=goal_store,
        )

        params = CompactParams(session_id=SID, session_key=SK)
        await lc.compact(params)

        assert ctx.compact_count == 3


# ======================================================================
# 22-28  after_turn
# ======================================================================


class TestAfterTurn:
    async def test_no_session_context_skips_tracking(self):
        """#22: after_turn with no session context -> skips tracking."""
        session_store = AsyncMock()
        session_store.get = AsyncMock(return_value=None)
        lc = _make_lifecycle(session_context_store=session_store)

        params = AfterTurnParams(session_id=SID, session_key=SK)
        await lc.after_turn(params)

        session_store.save.assert_not_called()

    async def test_s1_direct_quote_detection(self):
        """#23: after_turn S1 direct quote detection."""
        lc = _make_lifecycle()
        item = make_working_set_item(
            text="PostgreSQL connection pooling configuration setup maximum",
        )
        msgs = [
            AgentMessage(
                role="assistant",
                content="Set up PostgreSQL connection pooling configuration with maximum connections setup",
            ),
        ]
        is_quote, conf = lc._detect_direct_quote(item, msgs, 0)
        assert is_quote is True
        assert conf > 0.4

    async def test_s2_tool_correlation_with_tool_aliases(self):
        """#24: after_turn S2 tool correlation with TOOL_ALIASES."""
        lc = _make_lifecycle()
        item = make_working_set_item(
            text="npm install dependencies package management",
        )
        msgs = [
            AgentMessage(
                role="tool",
                content="installed package dependencies management",
                name="npm",
            ),
        ]
        is_tool, conf = lc._detect_tool_correlation(item, msgs)
        assert is_tool is True
        assert conf > 0.0
        # Verify alias mapping
        assert TOOL_ALIASES["npm"] == "node"

    async def test_running_jaccard(self):
        """#25: after_turn running Jaccard."""
        lc = _make_lifecycle()
        item = make_working_set_item(
            text="kubernetes cluster autoscaling deployment replicas horizontal",
        )
        msgs = [
            AgentMessage(
                role="assistant",
                content="kubernetes cluster autoscaling deployment replicas horizontal scaling",
            ),
        ]
        score = lc._compute_running_jaccard(item, msgs, 0)
        assert score > 0.5

    async def test_updates_successful_use_count_when_confidence_above_threshold(self):
        """#26: after_turn updates successful_use_count when confidence > 0.3."""
        item = make_working_set_item(
            text="kubernetes cluster autoscaling deployment replicas horizontal",
            source_type="fact",
            successful_use_count=0,
            use_count=0,
        )
        snapshot = make_working_set_snapshot(items=[item])
        ctx = make_session_context(
            last_snapshot_id=str(snapshot.snapshot_id),
            fact_last_injection_turn={str(item.id): 0},
            turn_count=2,
        )
        session_store = AsyncMock()
        session_store.get = AsyncMock(return_value=ctx)
        session_store.save = AsyncMock()
        redis = AsyncMock()
        redis.get = AsyncMock(return_value=snapshot.model_dump_json())
        memory_store = AsyncMock()
        config = ElephantBrokerConfig()
        config.successful_use.enabled = True
        lc = _make_lifecycle(
            session_context_store=session_store,
            redis=redis,
            memory_store=memory_store,
            config=config,
        )

        response_msgs = [
            AgentMessage(
                role="assistant",
                content="kubernetes cluster autoscaling deployment replicas horizontal pods",
            ),
        ]
        params = AfterTurnParams(
            session_id=SID, session_key=SK,
            messages=response_msgs, pre_prompt_message_count=0,
        )
        await lc.after_turn(params)

        # Memory store should be updated with successful_use_count
        assert memory_store.update.called
        calls = memory_store.update.call_args_list
        any_success = any(
            "successful_use_count" in str(c) for c in calls
        )
        assert any_success

    async def test_updates_use_count_for_all_facts(self):
        """#27: after_turn updates use_count for ALL facts."""
        item = make_working_set_item(
            text="unrelated database migration schema version control",
            source_type="fact",
            successful_use_count=0,
            use_count=0,
        )
        snapshot = make_working_set_snapshot(items=[item])
        ctx = make_session_context(
            last_snapshot_id=str(snapshot.snapshot_id),
            fact_last_injection_turn={str(item.id): 0},
            turn_count=5,
        )
        session_store = AsyncMock()
        session_store.get = AsyncMock(return_value=ctx)
        session_store.save = AsyncMock()
        redis = AsyncMock()
        redis.get = AsyncMock(return_value=snapshot.model_dump_json())
        memory_store = AsyncMock()
        config = ElephantBrokerConfig()
        config.successful_use.enabled = True
        lc = _make_lifecycle(
            session_context_store=session_store,
            redis=redis,
            memory_store=memory_store,
            config=config,
        )

        response_msgs = [
            AgentMessage(role="assistant", content="deploying kubernetes pods now"),
        ]
        params = AfterTurnParams(
            session_id=SID, session_key=SK,
            messages=response_msgs, pre_prompt_message_count=0,
        )
        await lc.after_turn(params)

        # use_count should be updated even for non-matching facts
        memory_store.update.assert_called()

    async def test_goal_progress_regex(self):
        """#28: after_turn goal progress regex detection."""
        ctx = make_session_context(turn_count=3)
        session_store = AsyncMock()
        session_store.get = AsyncMock(return_value=ctx)
        session_store.save = AsyncMock()
        redis = AsyncMock()
        redis.get = AsyncMock(return_value=None)  # no snapshot
        goal_store = AsyncMock()
        goal = GoalState(title="Deploy service")
        goal_store.get_goals = AsyncMock(return_value=[goal])
        lc = _make_lifecycle(
            session_context_store=session_store,
            redis=redis,
            session_goal_store=goal_store,
        )

        response_msgs = [
            AgentMessage(role="user", content="status?"),
            AgentMessage(
                role="assistant",
                content="I have completed the deployment successfully.",
            ),
        ]
        params = AfterTurnParams(
            session_id=SID, session_key=SK,
            messages=response_msgs, pre_prompt_message_count=1,
        )
        await lc.after_turn(params)

        goal_store.get_goals.assert_called_once()

    async def test_ignored_turns_tracking(self):
        """S6: ignored items track turns_since_inject >= 3."""
        item = make_working_set_item(text="obscure fact nobody uses")
        snapshot = make_working_set_snapshot(items=[item])
        ctx = make_session_context()
        ctx.last_snapshot_id = "snap1"
        ctx.turn_count = 10
        ctx.fact_last_injection_turn = {str(item.id): 5}  # injected turn 5, now turn 10

        redis = _make_redis_mock()
        redis.get = AsyncMock(return_value=snapshot.model_dump_json())
        session_store = AsyncMock()
        session_store.get = AsyncMock(return_value=ctx)
        session_store.save = AsyncMock()
        memory_store = AsyncMock()
        config = ElephantBrokerConfig()
        config.successful_use.enabled = True
        lc = _make_lifecycle(redis=redis, session_context_store=session_store, memory_store=memory_store, config=config)

        # Messages that don't reference the fact at all
        await lc.after_turn(AfterTurnParams(
            session_id=SID, session_key=SK,
            messages=[AgentMessage(role="assistant", content="totally unrelated topic about cats")],
        ))
        # use_count should still be updated (all facts get use_count bump)
        assert memory_store.update.called

    async def test_successful_use_count_incremented_without_config_enabled(self):
        """H1 fix: successful_use_count is incremented on the cheap heuristic
        path even when config.successful_use.enabled is False (the default).
        The enabled flag only gates the expensive RT-1 LLM batch evaluation."""
        item = make_working_set_item(
            text="kubernetes cluster autoscaling deployment replicas horizontal",
            source_type="fact",
            successful_use_count=0,
            use_count=0,
        )
        snapshot = make_working_set_snapshot(items=[item])
        ctx = make_session_context(
            last_snapshot_id=str(snapshot.snapshot_id),
            fact_last_injection_turn={str(item.id): 0},
            turn_count=2,
        )
        session_store = AsyncMock()
        session_store.get = AsyncMock(return_value=ctx)
        session_store.save = AsyncMock()
        redis = AsyncMock()
        redis.get = AsyncMock(return_value=snapshot.model_dump_json())
        memory_store = AsyncMock()
        # Default config: successful_use.enabled == False
        config = ElephantBrokerConfig()
        assert config.successful_use.enabled is False
        lc = _make_lifecycle(
            session_context_store=session_store,
            redis=redis,
            memory_store=memory_store,
            config=config,
        )

        response_msgs = [
            AgentMessage(
                role="assistant",
                content="kubernetes cluster autoscaling deployment replicas horizontal pods",
            ),
        ]
        params = AfterTurnParams(
            session_id=SID, session_key=SK,
            messages=response_msgs, pre_prompt_message_count=0,
        )
        await lc.after_turn(params)

        # Memory store should be updated with successful_use_count
        assert memory_store.update.called
        calls = memory_store.update.call_args_list
        any_success = any(
            "successful_use_count" in str(c) for c in calls
        )
        assert any_success, (
            "successful_use_count should be incremented on the cheap heuristic "
            "path even when config.successful_use.enabled is False"
        )


# ======================================================================
# P4: response-delta boundary (hybrid A+C)
# ======================================================================


class TestAfterTurnP4:
    """P4 tests: honor OpenClaw pre_prompt_message_count when emitted;
    derive response delta via tail-walker when the plugin is silent."""

    def _after_turn_trace_payload(self, trace: AsyncMock) -> dict:
        """Extract AFTER_TURN_COMPLETED payload from the trace mock."""
        matches = [
            c for c in trace.append_event.call_args_list
            if c[0][0].event_type == TraceEventType.AFTER_TURN_COMPLETED
        ]
        assert matches, "Expected AFTER_TURN_COMPLETED trace event"
        return matches[-1][0][0].payload

    async def test_honors_plugin_explicit_zero(self):
        """An explicit pre_prompt_message_count=0 (plugin emitted) means
        'all messages are response-side' — honored verbatim, not derived."""
        trace = AsyncMock()
        lc = _make_lifecycle(trace_ledger=trace)
        msgs = [
            AgentMessage(role="assistant", content="greetings"),
            AgentMessage(role="assistant", content="more reply"),
        ]
        await lc.after_turn(AfterTurnParams(
            session_id=SID, session_key=SK,
            messages=msgs, pre_prompt_message_count=0,
        ))
        payload = self._after_turn_trace_payload(trace)
        assert payload["boundary_source"] == "plugin"
        assert payload["response_messages"] == 2
        assert payload["total_messages"] == 2

    async def test_honors_plugin_nonzero(self):
        """pre_prompt_message_count=2 on 5 messages slices to 3 response msgs,
        regardless of where the user messages sit — plugin signal wins."""
        trace = AsyncMock()
        lc = _make_lifecycle(trace_ledger=trace)
        msgs = [
            AgentMessage(role="user", content="q1"),
            AgentMessage(role="assistant", content="a1"),
            AgentMessage(role="user", content="q2"),
            AgentMessage(role="assistant", content="a2a"),
            AgentMessage(role="assistant", content="a2b"),
        ]
        await lc.after_turn(AfterTurnParams(
            session_id=SID, session_key=SK,
            messages=msgs, pre_prompt_message_count=2,
        ))
        payload = self._after_turn_trace_payload(trace)
        assert payload["boundary_source"] == "plugin"
        assert payload["response_messages"] == 3
        assert payload["total_messages"] == 5

    async def test_derives_response_delta_when_plugin_silent(self):
        """When OpenClaw doesn't emit pre_prompt_message_count (None), the
        runtime walks backward from the tail to the last user message and
        slices after it — defense-in-depth for plugins that haven't wired
        the signal yet."""
        trace = AsyncMock()
        lc = _make_lifecycle(trace_ledger=trace)
        msgs = [
            AgentMessage(role="user", content="old q"),
            AgentMessage(role="assistant", content="old a"),
            AgentMessage(role="user", content="current q"),
            AgentMessage(role="assistant", content="current a"),
            AgentMessage(role="tool", content="tool output"),
        ]
        # pre_prompt_message_count omitted → defaults to None
        await lc.after_turn(AfterTurnParams(
            session_id=SID, session_key=SK, messages=msgs,
        ))
        payload = self._after_turn_trace_payload(trace)
        assert payload["boundary_source"] == "derived"
        # Everything after index 2 (the last user message) is response-side.
        assert payload["response_messages"] == 2
        assert payload["total_messages"] == 5

    def test_extract_response_delta_unit(self):
        """_extract_response_delta walks backward to the last user-role
        message and returns everything after it. If no user message is
        present, the entire list is treated as response-side.

        TODO-6-105 / TODO-6-306: ``_extract_response_delta`` is now an
        instance method (not ``@staticmethod``) so it can emit a WARN log
        and increment ``inc_response_delta_no_user_boundary`` on the
        no-user-role fallback branch. Direct unit calls go through an
        instance built with the shared ``_make_lifecycle()`` helper.
        """
        lc = _make_lifecycle()

        # No user message at all → whole list is response.
        only_assistant = [
            AgentMessage(role="assistant", content="a"),
            AgentMessage(role="assistant", content="b"),
        ]
        assert lc._extract_response_delta(only_assistant) == only_assistant

        # Single user in the middle → slice after it.
        mixed = [
            AgentMessage(role="assistant", content="old"),
            AgentMessage(role="user", content="q"),
            AgentMessage(role="assistant", content="a1"),
            AgentMessage(role="tool", content="t1"),
        ]
        delta = lc._extract_response_delta(mixed)
        assert len(delta) == 2
        assert delta[0].role == "assistant"
        assert delta[1].role == "tool"

        # Multiple users → slice after the LAST one (not the first).
        multi_user = [
            AgentMessage(role="user", content="first"),
            AgentMessage(role="assistant", content="reply1"),
            AgentMessage(role="user", content="latest"),
            AgentMessage(role="assistant", content="reply2"),
        ]
        delta = lc._extract_response_delta(multi_user)
        assert len(delta) == 1
        assert delta[0].content == "reply2"

        # Empty input → empty output.
        assert lc._extract_response_delta([]) == []

    def test_extract_response_delta_no_user_warns_and_increments(self, caplog):
        """TODO-6-105 (Business Logic, LOW) + TODO-6-306 (Blind Spot, LOW):
        when the envelope contains no ``role=="user"`` message, the
        tail-walker returns the full list as a defensive fallback AND
        emits both observability signals:

        - WARN log on ``elephantbroker.runtime.context.lifecycle`` with
          the envelope size so operators tailing journalctl can detect
          the no-user-boundary branch firing.
        - ``MetricsContext.inc_response_delta_no_user_boundary()``
          increment on the ``eb_response_delta_no_user_total{gateway_id}``
          counter so alertmanager can fire on ``rate(...) > 0``.

        The return value is intentionally NOT changed to ``[]`` — preserving
        the defensive fallback (``list(messages)``) keeps downstream scanners
        running; the observability surface is what closes the gap.
        """
        import logging
        metrics = MagicMock(spec=MetricsContext)
        lc = _make_lifecycle(metrics=metrics)

        envelope = [
            AgentMessage(role="assistant", content="a1"),
            AgentMessage(role="tool", content="t1"),
            AgentMessage(role="assistant", content="a2"),
        ]

        with caplog.at_level(
            logging.WARNING, logger="elephantbroker.runtime.context.lifecycle",
        ):
            delta = lc._extract_response_delta(envelope)

        # 1. Fallback return value is the full list (resilience preserved).
        assert delta == envelope
        assert len(delta) == 3

        # 2. Counter incremented exactly once (no labels beyond gateway_id).
        metrics.inc_response_delta_no_user_boundary.assert_called_once_with()

        # 3. WARN log emitted on the lifecycle logger.
        warn_records = [
            rec for rec in caplog.records
            if rec.levelno == logging.WARNING
            and rec.name == "elephantbroker.runtime.context.lifecycle"
            and "_extract_response_delta" in rec.getMessage()
            and "no user-role message" in rec.getMessage()
        ]
        assert len(warn_records) == 1, (
            f"expected exactly one WARN on no-user fallback, got {len(warn_records)}"
        )
        assert "3-message envelope" in warn_records[0].getMessage()


# ======================================================================
# C-boundary-source cluster — DEBUG log + Prometheus counter for the
# P4 hybrid-A+C boundary_source decision (TODO-6-201, TODO-6-302).
# ======================================================================


class TestAfterTurnBoundarySourceObservability:
    """TODO-6-201 (DEBUG log) + TODO-6-302 (Prometheus counter): the P4
    boundary-source decision (empty/plugin/derived) must be visible on
    both the log channel (operator tailing journalctl) and the metric
    channel (alertmanager rule on `source="derived"`)."""

    async def test_empty_branch_logs_and_increments(self, caplog):
        """No messages on the turn → `boundary_source="empty"` → one
        DEBUG log line + one `inc_after_turn_boundary_source("empty")`
        metric increment. Benign; operators should NOT alert on this."""
        import logging
        metrics = MagicMock(spec=MetricsContext)
        lc = _make_lifecycle(metrics=metrics)

        with caplog.at_level(
            logging.DEBUG, logger="elephantbroker.runtime.context.lifecycle",
        ):
            await lc.after_turn(AfterTurnParams(
                session_id=SID, session_key=SK, messages=[],
            ))

        metrics.inc_after_turn_boundary_source.assert_called_once_with("empty")

        debug_records = [
            rec for rec in caplog.records
            if rec.levelno == logging.DEBUG
            and rec.name == "elephantbroker.runtime.context.lifecycle"
            and "boundary_source=empty" in rec.getMessage()
        ]
        assert len(debug_records) == 1, (
            f"expected exactly one DEBUG line for empty branch, got {len(debug_records)}"
        )
        msg = debug_records[0].getMessage()
        assert "response_delta=0" in msg
        assert "total=0" in msg

    async def test_plugin_branch_logs_and_increments(self, caplog):
        """OpenClaw emitted `pre_prompt_message_count` → `boundary_source="plugin"`
        → one DEBUG log + one `inc_after_turn_boundary_source("plugin")`.
        Steady-state hot path; operators should NOT alert on this."""
        import logging
        metrics = MagicMock(spec=MetricsContext)
        lc = _make_lifecycle(metrics=metrics)
        msgs = [
            AgentMessage(role="user", content="q1"),
            AgentMessage(role="assistant", content="a1"),
            AgentMessage(role="user", content="q2"),
            AgentMessage(role="assistant", content="a2"),
        ]

        with caplog.at_level(
            logging.DEBUG, logger="elephantbroker.runtime.context.lifecycle",
        ):
            await lc.after_turn(AfterTurnParams(
                session_id=SID, session_key=SK,
                messages=msgs, pre_prompt_message_count=2,
            ))

        metrics.inc_after_turn_boundary_source.assert_called_once_with("plugin")

        debug_records = [
            rec for rec in caplog.records
            if rec.levelno == logging.DEBUG
            and rec.name == "elephantbroker.runtime.context.lifecycle"
            and "boundary_source=plugin" in rec.getMessage()
        ]
        assert len(debug_records) == 1
        msg = debug_records[0].getMessage()
        assert "response_delta=2" in msg
        assert "total=4" in msg

    async def test_derived_branch_logs_and_increments(self, caplog):
        """OpenClaw silent (no `pre_prompt_message_count`) → tail-walker
        fallback → `boundary_source="derived"` → one DEBUG log + one
        `inc_after_turn_boundary_source("derived")`. **Operator-actionable**
        — this is the value dashboards alert on to catch plugin regressions."""
        import logging
        metrics = MagicMock(spec=MetricsContext)
        lc = _make_lifecycle(metrics=metrics)
        msgs = [
            AgentMessage(role="user", content="old q"),
            AgentMessage(role="assistant", content="old a"),
            AgentMessage(role="user", content="current q"),
            AgentMessage(role="assistant", content="current a"),
        ]

        with caplog.at_level(
            logging.DEBUG, logger="elephantbroker.runtime.context.lifecycle",
        ):
            await lc.after_turn(AfterTurnParams(
                session_id=SID, session_key=SK, messages=msgs,
            ))

        metrics.inc_after_turn_boundary_source.assert_called_once_with("derived")

        debug_records = [
            rec for rec in caplog.records
            if rec.levelno == logging.DEBUG
            and rec.name == "elephantbroker.runtime.context.lifecycle"
            and "boundary_source=derived" in rec.getMessage()
        ]
        assert len(debug_records) == 1
        msg = debug_records[0].getMessage()
        # Everything after index 2 (last user message) is response-side → 1 msg.
        assert "response_delta=1" in msg
        assert "total=4" in msg


# ======================================================================
# PR #11 R1: Additional after_turn tests
# ======================================================================


class TestAfterTurnR1:
    """PR #11 R1 test TODOs for after_turn()."""

    async def test_successful_use_tracked_trace_event(self):
        """TODO-11-001: after_turn emits SUCCESSFUL_USE_TRACKED when signals detected."""
        item = make_working_set_item(
            text="kubernetes cluster deployment orchestration",
            source_type="fact",
            successful_use_count=0,
            use_count=0,
        )
        snapshot = make_working_set_snapshot(items=[item])
        ctx = make_session_context(
            last_snapshot_id=str(snapshot.snapshot_id),
            fact_last_injection_turn={str(item.id): 0},
            turn_count=1,
        )
        session_store = AsyncMock()
        session_store.get = AsyncMock(return_value=ctx)
        session_store.save = AsyncMock()
        redis = AsyncMock()
        redis.get = AsyncMock(return_value=snapshot.model_dump_json())
        memory_store = AsyncMock()
        trace = AsyncMock()
        config = ElephantBrokerConfig()
        config.successful_use.enabled = True
        lc = _make_lifecycle(
            session_context_store=session_store,
            redis=redis,
            memory_store=memory_store,
            trace_ledger=trace,
            config=config,
        )

        response_msgs = [
            AgentMessage(
                role="assistant",
                content="kubernetes cluster deployment orchestration is now complete",
            ),
        ]
        params = AfterTurnParams(
            session_id=SID, session_key=SK,
            messages=response_msgs, pre_prompt_message_count=0,
        )
        await lc.after_turn(params)

        # Find SUCCESSFUL_USE_TRACKED event
        tracked_calls = [
            c for c in trace.append_event.call_args_list
            if c[0][0].event_type == TraceEventType.SUCCESSFUL_USE_TRACKED
        ]
        assert len(tracked_calls) >= 1, "Expected SUCCESSFUL_USE_TRACKED trace event"
        payload = tracked_calls[0][0][0].payload
        assert payload["items_tracked"] >= 1
        assert payload["session_key"] == SK

    async def test_turn_count_increment(self):
        """TODO-11-002: after_turn increments turn_count by exactly 1."""
        ctx = make_session_context(turn_count=5)
        session_store = AsyncMock()
        session_store.get = AsyncMock(return_value=ctx)
        session_store.save = AsyncMock()
        redis = AsyncMock()
        redis.get = AsyncMock(return_value=None)
        lc = _make_lifecycle(
            session_context_store=session_store,
            redis=redis,
        )

        params = AfterTurnParams(
            session_id=SID, session_key=SK,
            messages=[AgentMessage(role="assistant", content="done")],
            pre_prompt_message_count=0,
        )
        await lc.after_turn(params)

        assert ctx.turn_count == 6
        session_store.save.assert_called()

    async def test_degraded_operation_on_empty_session_id(self):
        """TODO-11-003: lifecycle emits DEGRADED_OPERATION when session_id is empty."""
        trace = AsyncMock()
        session_store = AsyncMock()
        session_store.get = AsyncMock(return_value=make_session_context())
        session_store.save = AsyncMock()
        redis = AsyncMock()
        redis.get = AsyncMock(return_value=None)
        lc = _make_lifecycle(
            session_context_store=session_store,
            trace_ledger=trace,
            redis=redis,
        )

        params = AfterTurnParams(
            session_id="", session_key=SK,
            messages=[AgentMessage(role="assistant", content="ok")],
            pre_prompt_message_count=0,
        )
        await lc.after_turn(params)

        degraded_calls = [
            c for c in trace.append_event.call_args_list
            if c[0][0].event_type == TraceEventType.DEGRADED_OPERATION
        ]
        assert len(degraded_calls) >= 1, "Expected DEGRADED_OPERATION trace event"

    async def test_degraded_operation_on_bootstrap(self):
        """DEGRADED_OPERATION emitted on bootstrap() with empty session_id."""
        trace = AsyncMock()
        lc = _make_lifecycle(fresh_bootstrap=True, trace_ledger=trace)
        params = BootstrapParams(session_id="", session_key=SK)
        await lc.bootstrap(params)

        degraded = [c for c in trace.append_event.call_args_list
                    if c[0][0].event_type == TraceEventType.DEGRADED_OPERATION]
        assert len(degraded) >= 1

    async def test_degraded_operation_on_ingest_batch(self):
        """DEGRADED_OPERATION emitted on ingest_batch() with empty session_id."""
        trace = AsyncMock()
        session_store = AsyncMock()
        session_store.get = AsyncMock(return_value=make_session_context())
        session_store.save = AsyncMock()
        session_store._effective_ttl = lambda p: 86400
        lc = _make_lifecycle(session_context_store=session_store, trace_ledger=trace)

        params = IngestBatchParams(session_id="", session_key=SK,
                                   messages=[AgentMessage(role="user", content="hi")])
        await lc.ingest_batch(params)

        degraded = [c for c in trace.append_event.call_args_list
                    if c[0][0].event_type == TraceEventType.DEGRADED_OPERATION]
        assert len(degraded) >= 1

    async def test_degraded_operation_on_assemble(self):
        """DEGRADED_OPERATION emitted on assemble() with empty session_id."""
        trace = AsyncMock()
        lc = _make_lifecycle(trace_ledger=trace)

        params = AssembleParams(session_id="", session_key=SK,
                                messages=[AgentMessage(role="user", content="hi")])
        await lc.assemble(params)

        degraded = [c for c in trace.append_event.call_args_list
                    if c[0][0].event_type == TraceEventType.DEGRADED_OPERATION]
        assert len(degraded) >= 1

    async def test_degraded_operation_on_compact(self):
        """DEGRADED_OPERATION emitted on compact() with empty session_id."""
        trace = AsyncMock()
        lc = _make_lifecycle(trace_ledger=trace)

        params = CompactParams(session_id="", session_key=SK)
        await lc.compact(params)

        degraded = [c for c in trace.append_event.call_args_list
                    if c[0][0].event_type == TraceEventType.DEGRADED_OPERATION]
        assert len(degraded) >= 1


class TestTurnCountMultiTurn:
    """Verify turn_count increments correctly across multiple sequential turns."""

    async def test_turn_count_increments_across_3_turns(self):
        """after_turn() called 3x: turn_count goes 0→1→2→3."""
        ctx = make_session_context(turn_count=0)
        session_store = AsyncMock()
        session_store.get = AsyncMock(return_value=ctx)
        session_store.save = AsyncMock()
        redis = AsyncMock()
        redis.get = AsyncMock(return_value=None)
        lc = _make_lifecycle(session_context_store=session_store, redis=redis)

        for expected in (1, 2, 3):
            params = AfterTurnParams(
                session_id=SID, session_key=SK,
                messages=[AgentMessage(role="assistant", content=f"turn {expected}")],
                pre_prompt_message_count=0,
            )
            await lc.after_turn(params)
            assert ctx.turn_count == expected, f"Expected {expected}, got {ctx.turn_count}"

        assert session_store.save.call_count == 3


class TestBootstrapIdempotencyMultiTurn:
    """GF-15: bootstrap() reuses existing SessionContext on subsequent calls."""

    async def test_bootstrap_creates_once_reuses_twice(self):
        """3 bootstrap() calls: 1st creates, 2nd+3rd reuse existing SessionContext."""
        ctx = make_session_context(turn_count=5)
        session_store = AsyncMock()
        # 1st bootstrap: get returns None → creates new ctx and saves it
        # 2nd bootstrap: get returns ctx → reuse (early return)
        # 3rd bootstrap: get returns ctx → reuse (early return)
        session_store.get = AsyncMock(side_effect=[None, ctx, ctx])
        session_store.save = AsyncMock()
        session_store.get_compact_state = AsyncMock(return_value=None)
        session_store._effective_ttl = lambda p: 86400
        profile_reg = AsyncMock()
        profile_reg.resolve_profile = AsyncMock(return_value=make_profile_policy())
        trace = AsyncMock()
        lc = _make_lifecycle(
            fresh_bootstrap=True,
            session_context_store=session_store,
            profile_registry=profile_reg,
            trace_ledger=trace,
        )

        params = BootstrapParams(session_key=SK, session_id=SID, profile_name="coding")

        r1 = await lc.bootstrap(params)
        r2 = await lc.bootstrap(params)
        r3 = await lc.bootstrap(params)

        assert r1.bootstrapped and r2.bootstrapped and r3.bootstrapped
        # save called only once (first bootstrap creates the ctx)
        session_store.save.assert_called_once()
        # get called 3x total
        assert session_store.get.call_count == 3

        # Verify reuse trace events have is_reuse=True
        bootstrap_events = [
            c[0][0] for c in trace.append_event.call_args_list
            if c[0][0].event_type == TraceEventType.BOOTSTRAP_COMPLETED
        ]
        assert len(bootstrap_events) == 3
        assert bootstrap_events[0].payload.get("is_reuse") is not True  # first: fresh
        assert bootstrap_events[1].payload.get("is_reuse") is True
        assert bootstrap_events[2].payload.get("is_reuse") is True
        # turn_count preserved across reuses
        assert bootstrap_events[1].payload["turn_count"] == 5
        assert bootstrap_events[2].payload["turn_count"] == 5


# ======================================================================
# 29-30  subagent lifecycle
# ======================================================================


class TestSubagentLifecycle:
    async def test_prepare_subagent_spawn_stores_parent_children_mappings(self):
        """#29: prepare_subagent_spawn stores parent+children mappings."""
        redis = AsyncMock()
        lc = _make_lifecycle(redis=redis)

        params = SubagentSpawnParams(
            parent_session_key="agent:parent:main",
            child_session_key="agent:child:sub1",
            ttl_ms=60000,
        )
        result = await lc.prepare_subagent_spawn(params)

        assert result.parent_mapping_stored is True
        assert result.parent_session_key == "agent:parent:main"
        assert result.child_session_key == "agent:child:sub1"
        redis.setex.assert_called_once()
        redis.sadd.assert_called_once()
        redis.expire.assert_called_once()

    async def test_on_subagent_ended_emits_trace_event(self):
        """#30: on_subagent_ended emits trace event."""
        trace = AsyncMock()
        lc = _make_lifecycle(trace_ledger=trace)

        params = SubagentEndedParams(
            child_session_key="agent:child:sub1",
            reason="completed",
        )
        await lc.on_subagent_ended(params)

        trace.append_event.assert_called_once()
        event = trace.append_event.call_args[0][0]
        assert event.event_type == TraceEventType.SUBAGENT_ENDED
        assert event.payload["reason"] == "completed"
        assert event.payload["child_session_key"] == "agent:child:sub1"


# ======================================================================
# 31  dispose
# ======================================================================


class TestDispose:
    async def test_dispose_does_not_delete_session_context(self):
        """GF-15: dispose() is lightweight — does NOT delete SessionContext."""
        session_store = AsyncMock()
        artifact_store = AsyncMock()
        lc = _make_lifecycle(
            session_context_store=session_store,
            session_artifact_store=artifact_store,
        )

        await lc.dispose(SK, SID)

        session_store.delete.assert_not_called()
        artifact_store.delete.assert_not_called()

    async def test_session_end_deletes_session_context_not_artifacts(self):
        """GF-15: session_end() deletes SessionContext but NOT artifacts."""
        session_store = AsyncMock()
        artifact_store = AsyncMock()
        lc = _make_lifecycle(
            session_context_store=session_store,
            session_artifact_store=artifact_store,
        )

        await lc.session_end(SK, SID)

        session_store.delete.assert_called_once_with(SK, SID)
        artifact_store.delete.assert_not_called()

    async def test_dispose_emits_session_boundary_trace(self):
        """GF-15: dispose emits SESSION_BOUNDARY with action=engine_teardown."""
        trace = AsyncMock()
        lc = _make_lifecycle(trace_ledger=trace)

        await lc.dispose(SK, SID)

        trace.append_event.assert_called_once()
        event = trace.append_event.call_args[0][0]
        assert event.event_type == TraceEventType.SESSION_BOUNDARY
        assert event.payload["action"] == "engine_teardown"

    async def test_dispose_does_not_flush_goals(self):
        """GF-15: dispose() is lightweight — no goal flush."""
        goal_store = AsyncMock()
        goal_store.flush_to_cognee = AsyncMock(return_value=0)
        lc = _make_lifecycle(session_goal_store=goal_store)

        await lc.dispose(SK, SID)

        goal_store.flush_to_cognee.assert_not_called()

    async def test_session_end_flushes_session_goals(self):
        """GF-15: session_end() calls session_goal_store.flush_to_cognee."""
        goal_store = AsyncMock()
        goal_store.flush_to_cognee = AsyncMock(return_value=0)
        lc = _make_lifecycle(session_goal_store=goal_store)

        await lc.session_end(SK, SID)

        goal_store.flush_to_cognee.assert_called_once()
        call_args = goal_store.flush_to_cognee.call_args[0]
        assert call_args[0] == SK

    async def test_session_end_uses_stored_bootstrap_session_id(self):
        """GF-15: session_end() with empty sid falls back to stored bootstrap session_id."""
        goal_store = AsyncMock()
        goal_store.flush_to_cognee = AsyncMock(return_value=0)
        lc = _make_lifecycle(fresh_bootstrap=True, session_goal_store=goal_store)

        bootstrap_sid = str(uuid.uuid4())
        await lc.bootstrap(BootstrapParams(
            session_key=SK, session_id=bootstrap_sid, profile_name="coding",
        ))

        # session_end with EMPTY sid — should use stored bootstrap sid
        await lc.session_end(SK, "")

        goal_store.flush_to_cognee.assert_called_once()
        call_args = goal_store.flush_to_cognee.call_args[0]
        assert call_args[0] == SK
        # session_id should be the bootstrap one, not a random fallback
        assert str(call_args[1]) == bootstrap_sid


# ======================================================================
# 32  auto-bootstrap
# ======================================================================


class TestAutoBootstrap:
    async def test_auto_bootstrap_on_ingest_batch_when_no_session_context(self):
        """#32: auto-bootstrap on ingest_batch when no session context."""
        ctx = make_session_context()
        session_store = AsyncMock()
        # 3 session_store.get() calls in sequence:
        # (1) ingest_batch → _load_session_context: None → triggers auto-bootstrap
        # (2) bootstrap → GF-15 idempotency check (session_store.get): None → creates fresh ctx
        # (3) ingest_batch → _load_session_context (retry after bootstrap): returns ctx
        session_store.get = AsyncMock(side_effect=[None, None, ctx])
        session_store.save = AsyncMock()
        session_store.get_compact_state = AsyncMock(return_value=None)
        session_store._effective_ttl = lambda p: 86400
        profile_reg = AsyncMock()
        profile_reg.resolve_profile = AsyncMock(return_value=make_profile_policy())
        lc = _make_lifecycle(
            session_context_store=session_store,
            profile_registry=profile_reg,
        )

        msgs = [AgentMessage(role="user", content="hello")]
        params = IngestBatchParams(session_id=SID, session_key=SK, messages=msgs)
        await lc.ingest_batch(params)

        # resolve_profile called during auto-bootstrap
        profile_reg.resolve_profile.assert_called()
        # get called 3x: see side_effect comment above for call sequence
        assert session_store.get.call_count == 3


# ======================================================================
# 33-35  _filter_goals_for_injection (smart cadence)
# ======================================================================


class TestFilterGoalsForInjection:
    def test_smart_cadence_first_turn_always(self):
        """#33: smart cadence - first turn always injects."""
        lc = _make_lifecycle()
        profile = make_profile_policy()
        ctx = make_session_context(
            turn_count=0,
            goal_inject_history={},
            profile=profile,
        )
        goal = GoalState(title="Deploy service")

        result = lc._filter_goals_for_injection(
            [goal], ctx, profile.assembly_placement,
        )

        assert len(result) == 1
        assert result[0].title == "Deploy service"
        assert str(goal.id) in ctx.goal_inject_history

    def test_smart_cadence_blockers_always(self):
        """#34: smart cadence - blockers always injected."""
        lc = _make_lifecycle()
        profile = make_profile_policy()
        goal = GoalState(title="Deploy service", blockers=["Need credentials"])
        ctx = make_session_context(
            turn_count=10,
            goal_inject_history={
                str(goal.id): {"turn": 9, "status": "active"},
            },
            profile=profile,
        )

        result = lc._filter_goals_for_injection(
            [goal], ctx, profile.assembly_placement,
        )

        assert len(result) == 1

    def test_smart_cadence_reminder_interval(self):
        """#35: smart cadence - reminder interval triggers injection."""
        lc = _make_lifecycle()
        profile = make_profile_policy()
        goal = GoalState(title="Deploy service")
        ctx = make_session_context(
            turn_count=10,
            goal_inject_history={
                str(goal.id): {"turn": 4, "status": "active"},
            },
            profile=profile,
        )

        # Default goal_reminder_interval=5, 10-4=6 >= 5 -> inject
        result = lc._filter_goals_for_injection(
            [goal], ctx, profile.assembly_placement,
        )

        assert len(result) == 1

    def test_smart_cadence_within_interval_skips(self):
        """#35b: within reminder interval with no blockers -> skip."""
        lc = _make_lifecycle()
        profile = make_profile_policy()
        goal = GoalState(title="Deploy service")
        ctx = make_session_context(
            turn_count=10,
            goal_inject_history={
                str(goal.id): {"turn": 8, "status": "active"},
            },
            profile=profile,
        )

        # 10-8=2 < 5 (reminder_interval), no blockers, same status -> skip
        result = lc._filter_goals_for_injection(
            [goal], ctx, profile.assembly_placement,
        )

        assert len(result) == 0

    def test_smart_cadence_status_change_triggers(self):
        """#35c: status change triggers re-injection even within interval."""
        lc = _make_lifecycle()
        profile = make_profile_policy()
        goal = GoalState(title="Deploy service", status="completed")
        ctx = make_session_context(
            turn_count=10,
            goal_inject_history={
                str(goal.id): {"turn": 9, "status": "active"},
            },
            profile=profile,
        )

        result = lc._filter_goals_for_injection(
            [goal], ctx, profile.assembly_placement,
        )

        assert len(result) == 1

    def test_empty_goals_returns_empty(self):
        """Empty goals list returns empty list."""
        lc = _make_lifecycle()
        profile = make_profile_policy()
        ctx = make_session_context(profile=profile)

        result = lc._filter_goals_for_injection(
            [], ctx, profile.assembly_placement,
        )
        assert result == []

    def test_always_cadence_returns_all(self):
        """'always' cadence returns all goals without filtering."""
        from elephantbroker.schemas.profile import AssemblyPlacementPolicy

        lc = _make_lifecycle()
        placement = AssemblyPlacementPolicy(goal_injection_cadence="always")
        ctx = make_session_context()
        goals = [GoalState(title="g1"), GoalState(title="g2")]

        result = lc._filter_goals_for_injection(goals, ctx, placement)
        assert len(result) == 2


# ======================================================================
# Budget resolution (unit tests for _resolve_effective_budget)
# ======================================================================


class TestBudgetResolution:
    def test_profile_only_with_dynamic_fallback(self):
        """Profile budget used when no openclaw or window (with dynamic fallback)."""
        lc = _make_lifecycle()
        profile = make_profile_policy()
        profile.budgets.max_prompt_tokens = 8000

        budget, source = lc._resolve_effective_budget(profile, None, None)
        # dynamic budget: fallback=128000*0.15=19200, min(8000,19200)=8000
        assert budget == 8000

    def test_openclaw_wins_when_smallest(self):
        """OpenClaw budget wins when it is the smallest."""
        lc = _make_lifecycle()
        profile = make_profile_policy()
        profile.budgets.max_prompt_tokens = 10000

        budget, source = lc._resolve_effective_budget(profile, 3000, None)
        assert budget == 3000
        assert source == "openclaw"

    def test_window_fraction_wins_when_smallest(self):
        """Window fraction wins when it is the smallest."""
        lc = _make_lifecycle()
        profile = make_profile_policy()
        profile.budgets.max_prompt_tokens = 10000

        # window=20000 * 0.15 = 3000
        budget, source = lc._resolve_effective_budget(profile, 5000, 20000)
        assert budget == 3000
        assert source == "window"


# ======================================================================
# Helper method unit tests
# ======================================================================


class TestShouldCaptureArtifact:
    def test_captures_over_200_chars(self):
        lc = _make_lifecycle()
        msg = AgentMessage(role="tool", content="x" * 250, name="grep")
        assert lc._should_capture_artifact(msg) is True

    def test_skips_under_200_chars(self):
        lc = _make_lifecycle()
        msg = AgentMessage(role="tool", content="short", name="grep")
        assert lc._should_capture_artifact(msg) is False

    def test_respects_min_content_chars_config(self):
        config = ElephantBrokerConfig()
        config.artifact_capture.min_content_chars = 500
        lc = _make_lifecycle(config=config)
        msg = AgentMessage(role="tool", content="x" * 400, name="grep")
        assert lc._should_capture_artifact(msg) is False

    def test_disabled_artifact_capture(self):
        config = ElephantBrokerConfig()
        config.artifact_capture.enabled = False
        lc = _make_lifecycle(config=config)
        msg = AgentMessage(role="tool", content="x" * 1000, name="grep")
        assert lc._should_capture_artifact(msg) is False


class TestDetectDirectQuote:
    def test_high_overlap_detected(self):
        lc = _make_lifecycle()
        item = make_working_set_item(
            text="PostgreSQL connection pooling configuration maximum connections",
        )
        msgs = [
            AgentMessage(
                role="assistant",
                content="Set up PostgreSQL connection pooling configuration with maximum connections of 100",
            ),
        ]
        is_quote, confidence = lc._detect_direct_quote(item, msgs, 0)
        assert is_quote is True
        assert confidence > 0.4

    def test_no_overlap_not_detected(self):
        lc = _make_lifecycle()
        item = make_working_set_item(text="PostgreSQL connection pooling")
        msgs = [
            AgentMessage(role="assistant", content="Deploying nginx reverse proxy"),
        ]
        is_quote, confidence = lc._detect_direct_quote(item, msgs, 0)
        assert is_quote is False


class TestDetectToolCorrelation:
    def test_tool_alias_expansion(self):
        lc = _make_lifecycle()
        item = make_working_set_item(
            text="npm install package dependencies management",
        )
        msgs = [
            AgentMessage(
                role="tool",
                content="installed package dependencies",
                name="npm",
            ),
        ]
        is_tool, confidence = lc._detect_tool_correlation(item, msgs)
        assert is_tool is True
        assert confidence > 0.0

    def test_no_tool_messages_returns_false(self):
        lc = _make_lifecycle()
        item = make_working_set_item(text="some text about tools")
        msgs = [
            AgentMessage(role="assistant", content="response without tools"),
        ]
        is_tool, confidence = lc._detect_tool_correlation(item, msgs)
        assert is_tool is False
        assert confidence == 0.0


class TestComputeRunningJaccard:
    def test_high_overlap_score(self):
        lc = _make_lifecycle()
        item = make_working_set_item(
            text="kubernetes cluster autoscaling deployment replicas",
        )
        msgs = [
            AgentMessage(
                role="assistant",
                content="kubernetes cluster autoscaling deployment replicas horizontal",
            ),
        ]
        score = lc._compute_running_jaccard(item, msgs, 0)
        assert score > 0.5

    def test_no_overlap_score(self):
        lc = _make_lifecycle()
        item = make_working_set_item(text="kubernetes cluster autoscaling")
        msgs = [
            AgentMessage(role="assistant", content="deployed nginx proxy"),
        ]
        score = lc._compute_running_jaccard(item, msgs, 0)
        assert score < 0.3

    def test_skips_non_assistant_messages(self):
        lc = _make_lifecycle()
        item = make_working_set_item(text="kubernetes cluster autoscaling")
        msgs = [
            AgentMessage(
                role="user",
                content="kubernetes cluster autoscaling exactly same text",
            ),
        ]
        score = lc._compute_running_jaccard(item, msgs, 0)
        assert score == 0.0


class TestScannerCalibration:
    """J-1: validate successful-use scanner threshold calibration.

    Pre-calibration (DIAG-I1 verdict): the scanner returned method="ignored" on 21/21
    candidates in a live test where the agent quoted stored facts verbatim. Two root
    causes were identified:

    - H-alt-2: S1/Jaccard/use_confidence gates were too strict (0.4 / 0.3 / 0.3).
      A response that quoted a single canonical token (e.g., "TimescaleDB") out of a
      multi-phrase fact could never clear 0.4 on S1. Lowered to 0.15 across the board
      (S2 tool_correlation stays at 0.3 — different signal, different noise floor).

    - H-alt-4 heritage (now superseded by T-3): the ``source_type == "fact"``
      guards at the assemble-time ``last_used_at`` touch and in
      ``_track_successful_use`` originally excluded retrieval-sourced facts
      because ``WorkingSetItem.source_type`` overloaded two semantics — both
      DataPoint-type ("fact"/"artifact"/"goal"/…) AND retrieval path
      ("vector"/"keyword"/"graph"/"structural"). J-1 shipped a tactical
      widening via ``FACT_SOURCE_TYPES`` frozenset. T-3 superseded that by
      splitting the two concerns into distinct fields: ``source_type`` now
      carries only the DataPoint-type semantic, ``retrieval_source`` the
      retrieval path. Fact-class items (regardless of retrieval path)
      correctly hit the ``== "fact"`` check today.
    """

    def test_s1_direct_quote_fires_at_lowered_threshold(self):
        """S1 registers a single-phrase hit (ratio ~0.2) that would've been ignored at 0.4.

        Pre-calibration, a fact with 5 extracted phrases whose response only contained
        one matching bigram produced ratio=0.2 → method="ignored". Post-calibration,
        the same input produces method="quote" with confidence=0.2.
        """
        lc = _make_lifecycle()
        # text yields 5 phrases: (postgres timescaledb), (timescaledb compression),
        # (compression hypertables), (postgres timescaledb compression),
        # (timescaledb compression hypertables)
        item = make_working_set_item(
            text="postgres timescaledb compression hypertables",
        )
        msgs = [
            AgentMessage(
                role="assistant",
                content="postgres timescaledb is the right answer here",
            ),
        ]
        is_quote, confidence = lc._detect_direct_quote(item, msgs, 0)
        assert is_quote is True, (
            "S1 should register a single canonical-token quote post-calibration "
            "(ratio 0.2 > new threshold 0.15)"
        )
        # Sanity: we're deliberately in the band the calibration unlocked.
        assert 0.15 < confidence < 0.4, (
            f"confidence={confidence} — expected in the (0.15, 0.4) band that "
            f"the old 0.4 threshold used to reject"
        )

    def test_s1_fires_on_punct_heavy_fact_with_paraphrase(self):
        """T-1 pipeline smoke test: realistic fact+paraphrase produces a
        direct_quote signal ≥ 0.15 with punctuation stripping in effect.

        The strict punctuation-stripping regression guarantees are covered by
        `test_utils.py::TestExtractKeyPhrases` (the utility-level tests).
        This is the scanner-level integration: once `_extract_key_phrases`
        has been made punct-tolerant, `_detect_direct_quote` consuming it
        must still produce a firing signal on a realistic agent paraphrase
        (live DIAG-M1 baseline on a production TimescaleDB fact was ratio
        ~0.067 pre-T-1).

        The fact text has a trailing period (`"data."`). Post-T-1 strips
        that period so phrases ending in `"data"` substring-match the
        response `"time-series data in this project"`.
        """
        lc = _make_lifecycle()
        item = make_working_set_item(
            text="I use PostgreSQL with the TimescaleDB extension for time-series data.",
        )
        msgs = [
            AgentMessage(
                role="assistant",
                content=(
                    "You use PostgreSQL with the TimescaleDB extension "
                    "for time-series data in this project."
                ),
            ),
        ]
        is_quote, confidence = lc._detect_direct_quote(item, msgs, 0)
        assert is_quote is True, (
            "S1 should fire post-T-1: trailing-punct phrases now match "
            "paraphrased responses without the punctuation"
        )
        assert confidence > 0.15, (
            f"confidence={confidence} — expected > 0.15 post-T-1 "
            f"(pre-fix baseline was ~0.067)"
        )

    async def test_vector_source_type_triggers_successful_use_update(self):
        """Retrieval-sourced fact fires successful_use_count increment after a
        matching response — validates the T-3 source_type split.

        Pre-J-1: ``item.source_type == "fact"`` guard skipped retrieval items
        whose ``source_type`` carried the retrieval path ("vector", etc.).
        J-1 widened via ``FACT_SOURCE_TYPES`` frozenset as a tactical fix.
        T-3 split the two concerns — fact-class items now carry
        ``source_type="fact"`` with ``retrieval_source`` on a separate
        field — so the clean ``== "fact"`` check correctly covers all
        fact-class items regardless of retrieval path.
        """
        item = make_working_set_item(
            text="postgres timescaledb compression hypertables",
            source_type="fact",           # T-3: DataPoint-type semantic
            retrieval_source="vector",    # T-3: retrieval-path provenance
            successful_use_count=0,
            use_count=0,
        )
        snapshot = make_working_set_snapshot(items=[item])
        ctx = make_session_context(
            last_snapshot_id=str(snapshot.snapshot_id),
            fact_last_injection_turn={str(item.id): 0},
            turn_count=1,
        )
        session_store = AsyncMock()
        session_store.get = AsyncMock(return_value=ctx)
        session_store.save = AsyncMock()
        session_store._effective_ttl = lambda p: 86400
        redis = AsyncMock()
        redis.get = AsyncMock(return_value=snapshot.model_dump_json())
        memory_store = AsyncMock()
        config = ElephantBrokerConfig()
        config.successful_use.enabled = True
        lc = _make_lifecycle(
            session_context_store=session_store,
            redis=redis,
            memory_store=memory_store,
            config=config,
        )

        params = AfterTurnParams(
            session_id=SID, session_key=SK,
            messages=[
                AgentMessage(
                    role="assistant",
                    content=(
                        "postgres timescaledb is the right answer — "
                        "it handles hypertables natively."
                    ),
                ),
            ],
            pre_prompt_message_count=0,
        )
        await lc.after_turn(params)

        # Extract any memory_store.update call carrying successful_use_count.
        # T-3: fact-class items (source_type="fact") with retrieval_source
        # stamped produce successful_use_count increments when the response
        # quotes their text. Pre-T-3, the `FACT_SOURCE_TYPES` frozenset
        # widened this check to cover retrieval-path-typed items; post-T-3
        # the check is a clean `== "fact"` against the DataPoint-type
        # semantic and retrieval_source lives on a separate field.
        success_updates = []
        for c in memory_store.update.call_args_list:
            if len(c.args) >= 2 and isinstance(c.args[1], dict):
                if "successful_use_count" in c.args[1]:
                    success_updates.append(c.args[1])
        assert len(success_updates) >= 1, (
            "Expected successful_use_count increment for fact-class item "
            "(source_type='fact', retrieval_source='vector'). "
            "If this fails, the T-3 source_type split or the scanner's "
            "fact-update gate regressed."
        )
        assert success_updates[0]["successful_use_count"] == 1

    async def test_unrelated_fact_still_ignored(self):
        """Calibration must not cause false positives: a fact with no token overlap
        with the response still yields method='ignored' and does NOT increment
        successful_use_count (just last_used_at / use_count).
        """
        item = make_working_set_item(
            text="redis sentinel failover cluster configuration",
            source_type="fact",           # T-3: DataPoint-type semantic
            retrieval_source="vector",    # T-3: retrieval-path provenance
            successful_use_count=0,
            use_count=0,
        )
        snapshot = make_working_set_snapshot(items=[item])
        ctx = make_session_context(
            last_snapshot_id=str(snapshot.snapshot_id),
            fact_last_injection_turn={str(item.id): 0},
            turn_count=1,
        )
        session_store = AsyncMock()
        session_store.get = AsyncMock(return_value=ctx)
        session_store.save = AsyncMock()
        session_store._effective_ttl = lambda p: 86400
        redis = AsyncMock()
        redis.get = AsyncMock(return_value=snapshot.model_dump_json())
        memory_store = AsyncMock()
        config = ElephantBrokerConfig()
        config.successful_use.enabled = True
        lc = _make_lifecycle(
            session_context_store=session_store,
            redis=redis,
            memory_store=memory_store,
            config=config,
        )

        params = AfterTurnParams(
            session_id=SID, session_key=SK,
            messages=[
                AgentMessage(
                    role="assistant",
                    content="postgres schemas and database connection pooling.",
                ),
            ],
            pre_prompt_message_count=0,
        )
        await lc.after_turn(params)

        for c in memory_store.update.call_args_list:
            if len(c.args) >= 2 and isinstance(c.args[1], dict):
                assert "successful_use_count" not in c.args[1], (
                    f"Unrelated fact should not trigger successful_use_count "
                    f"increment, got update payload: {c.args[1]}"
                )

    async def test_uses_profile_thresholds(self):
        """T-2: scanner respects per-profile `successful_use_thresholds`.

        Uses the exact fact+response pair from
        ``test_s1_direct_quote_fires_at_lowered_threshold`` (ratio ~0.2,
        which fires at the default S1 threshold 0.15). With tightened
        profile thresholds (S1=0.5, S3=0.5) the same input produces
        ``method="ignored"`` and no ``successful_use_count`` increment.

        Both S1 and S3 are tightened because the fact+response pair also
        produces a Jaccard score (~0.29, from the 2-token intersection
        ``{postgres, timescaledb}``) that would fire under the default
        S3 gate of 0.15 — the test isolates scanner-level threshold flow,
        not signal-specific semantics.

        Covers end-to-end resolution: profile → registry resolver →
        ``_track_successful_use`` → ``_detect_direct_quote`` +
        ``_compute_running_jaccard``. Confirms the thresholds object flows
        from the registry through the scanner helpers and is actually
        consulted at each gate.
        """
        from elephantbroker.schemas.profile import SuccessfulUseThresholds

        item = make_working_set_item(
            text="postgres timescaledb compression hypertables",
            source_type="fact",           # T-3: DataPoint-type semantic
            retrieval_source="vector",    # T-3: retrieval-path provenance
            successful_use_count=0,
            use_count=0,
        )
        snapshot = make_working_set_snapshot(items=[item])
        ctx = make_session_context(
            last_snapshot_id=str(snapshot.snapshot_id),
            fact_last_injection_turn={str(item.id): 0},
            turn_count=1,
        )
        session_store = AsyncMock()
        session_store.get = AsyncMock(return_value=ctx)
        session_store.save = AsyncMock()
        session_store._effective_ttl = lambda p: 86400
        redis = AsyncMock()
        redis.get = AsyncMock(return_value=snapshot.model_dump_json())
        memory_store = AsyncMock()
        config = ElephantBrokerConfig()
        config.successful_use.enabled = True
        lc = _make_lifecycle(
            session_context_store=session_store,
            redis=redis,
            memory_store=memory_store,
            config=config,
        )
        # Override resolver to return tighter S1 + S3 thresholds (0.5 each)
        # — the same input that fires at the default (0.15/0.15) must now
        # get ignored on both signals, yielding method="ignored".
        lc._profile_registry.effective_successful_use_thresholds = MagicMock(
            return_value=SuccessfulUseThresholds(
                s1_direct_quote_ratio=0.5,
                s3_jaccard_score=0.5,
            ),
        )

        params = AfterTurnParams(
            session_id=SID, session_key=SK,
            messages=[
                AgentMessage(
                    role="assistant",
                    content="postgres timescaledb is the right answer here",
                ),
            ],
            pre_prompt_message_count=0,
        )
        await lc.after_turn(params)

        # At tightened thresholds, neither the 0.2-ratio quote nor the
        # 0.29-score jaccard fires. The memory_store should only see the
        # plain use_count bump (last_used_at path), never successful_use_count.
        for c in memory_store.update.call_args_list:
            if len(c.args) >= 2 and isinstance(c.args[1], dict):
                assert "successful_use_count" not in c.args[1], (
                    f"Profile thresholds 0.5/0.5 should have ignored both "
                    f"S1 and S3 signals — got update payload: {c.args[1]}"
                )


# ======================================================================
# Message transformation (AD-4)
# ======================================================================


class TestReplaceOldToolOutputs:
    async def test_replaces_old_keeps_recent(self):
        """Old tool outputs replaced with placeholders, most recent kept."""
        artifact_store = AsyncMock()
        artifact = SessionArtifact(
            tool_name="bash", content="x" * 500, summary="output summary",
        )
        artifact_store.get_by_hash = AsyncMock(return_value=artifact)
        lc = _make_lifecycle(session_artifact_store=artifact_store)
        policy = make_profile_policy().assembly_placement

        msgs = [
            AgentMessage(role="tool", content="x" * 500, name="bash"),  # old — should be replaced
            AgentMessage(role="user", content="fix it"),
            AgentMessage(role="tool", content="y" * 500, name="bash"),  # recent — should stay
        ]
        result = await lc._replace_old_tool_outputs(msgs, "sk", "sid", policy)
        # First tool output replaced, second kept
        assert "artifact_search" in result[0].content
        assert result[2].content == "y" * 500

    async def test_skip_when_no_artifact(self):
        """Tool outputs without matching artifacts are kept."""
        artifact_store = AsyncMock()
        artifact_store.get_by_hash = AsyncMock(return_value=None)
        lc = _make_lifecycle(session_artifact_store=artifact_store)
        policy = make_profile_policy().assembly_placement
        msgs = [AgentMessage(role="tool", content="x" * 500, name="bash")]
        result = await lc._replace_old_tool_outputs(msgs, "sk", "sid", policy)
        assert result[0].content == "x" * 500

    async def test_skip_small_outputs(self):
        """Tool outputs below min_tokens threshold are kept."""
        artifact_store = AsyncMock()
        lc = _make_lifecycle(session_artifact_store=artifact_store)
        policy = make_profile_policy().assembly_placement
        msgs = [AgentMessage(role="tool", content="OK", name="echo")]
        result = await lc._replace_old_tool_outputs(msgs, "sk", "sid", policy)
        assert result[0].content == "OK"
        artifact_store.get_by_hash.assert_not_called()


class TestDeduplicateConversation:
    def test_removes_covered_tool_messages(self):
        """Tool messages covered by Block 3 items are removed."""
        lc = _make_lifecycle()
        policy = make_profile_policy().assembly_placement
        items = [make_working_set_item(text="postgresql database migration script running")]
        msgs = [
            AgentMessage(role="user", content="run the migration"),
            AgentMessage(role="tool", content="postgresql database migration script running successfully"),
            AgentMessage(role="assistant", content="done"),
        ]
        result, removed = lc._deduplicate_conversation(msgs, items, policy)
        assert removed >= 1
        # User and assistant messages should remain
        assert any(m.role == "user" for m in result)
        assert any(m.role == "assistant" for m in result)

    def test_no_dedup_when_disabled(self):
        """Dedup disabled returns all messages."""
        lc = _make_lifecycle()
        from elephantbroker.schemas.profile import AssemblyPlacementPolicy
        policy = AssemblyPlacementPolicy(conversation_dedup_enabled=False)
        items = [make_working_set_item(text="same content")]
        msgs = [AgentMessage(role="tool", content="same content")]
        result, removed = lc._deduplicate_conversation(msgs, items, policy)
        assert removed == 0
        assert len(result) == 1

    def test_skips_already_replaced_messages(self):
        """Messages with eb_replaced metadata should NOT be dedup'd."""
        lc = _make_lifecycle()
        policy = make_profile_policy().assembly_placement
        items = [make_working_set_item(text="postgresql database query results")]
        msgs = [
            AgentMessage(
                role="tool",
                content="postgresql database query results summary",
                metadata={"eb_replaced": "true"},  # Already a placeholder
            ),
        ]
        result, removed = lc._deduplicate_conversation(msgs, items, policy)
        assert removed == 0  # Should NOT remove already-replaced messages
        assert len(result) == 1


class TestInjectionTurnFiltering:
    def test_jaccard_filters_pre_injection_messages(self):
        """Running Jaccard only considers messages at or after injection turn."""
        lc = _make_lifecycle()
        item = make_working_set_item(text="kubernetes cluster deployment")
        msgs = [
            AgentMessage(role="assistant", content="kubernetes cluster deployment replicas", metadata={"eb_turn": "1"}),
            AgentMessage(role="assistant", content="unrelated response about cats", metadata={"eb_turn": "5"}),
        ]
        # injection_turn=3 should skip turn 1 message
        score = lc._compute_running_jaccard(item, msgs, 3)
        # Only the turn 5 message is considered, which doesn't match
        assert score < 0.3

    def test_s1_filters_pre_injection_messages(self):
        """S1 direct quote only considers post-injection assistant messages."""
        lc = _make_lifecycle()
        item = make_working_set_item(text="deploy kubernetes helm charts production")
        msgs = [
            AgentMessage(role="assistant", content="deploy kubernetes helm charts production cluster", metadata={"eb_turn": "1"}),
            AgentMessage(role="assistant", content="unrelated", metadata={"eb_turn": "5"}),
        ]
        # injection_turn=3 should skip turn 1
        is_quote, _ = lc._detect_direct_quote(item, msgs, 3)
        assert is_quote is False  # Turn 1 is pre-injection, turn 5 doesn't match


# ======================================================================
# Integration flow-through tests (Phase 6 → Phase 3-5 code paths)
# ======================================================================


class TestIntegrationFlowThrough:
    async def test_assemble_converts_session_id_to_uuid_for_wsm(self):
        """WorkingSetManager.build_working_set expects uuid.UUID, lifecycle passes str."""
        wsm = AsyncMock()
        wsm.build_working_set = AsyncMock(return_value=make_working_set_snapshot(items=[]))
        assembler = AsyncMock()
        assembler.assemble_from_snapshot = AsyncMock(return_value=AssembleResult())
        redis = _make_redis_mock()
        lc = _make_lifecycle(working_set_manager=wsm, context_assembler=assembler, redis=redis)

        sid = str(uuid.uuid4())
        await lc.assemble(AssembleParams(session_id=sid, session_key=SK))

        # Verify session_id was passed as UUID, not string
        call_kwargs = wsm.build_working_set.call_args.kwargs
        assert isinstance(call_kwargs["session_id"], uuid.UUID)

    async def test_assemble_converts_session_id_for_guard(self):
        """Guard engine expects uuid.UUID for session_id."""
        guard = AsyncMock()
        from elephantbroker.schemas.guards import GuardResult, GuardOutcome
        guard.preflight_check = AsyncMock(return_value=GuardResult(outcome=GuardOutcome.PASS))
        guard.reinject_constraints = AsyncMock(return_value=[])
        wsm = AsyncMock()
        wsm.build_working_set = AsyncMock(return_value=make_working_set_snapshot(items=[]))
        assembler = AsyncMock()
        assembler.assemble_from_snapshot = AsyncMock(return_value=AssembleResult())
        redis = _make_redis_mock()
        lc = _make_lifecycle(guard_engine=guard, working_set_manager=wsm,
                             context_assembler=assembler, redis=redis)

        sid = str(uuid.uuid4())
        await lc.assemble(AssembleParams(session_id=sid, session_key=SK))

        # Verify preflight_check received UUID
        call_args = guard.preflight_check.call_args[0]
        assert isinstance(call_args[0], uuid.UUID)

    async def test_assemble_merges_transformed_messages_with_assembler_output(self):
        """Lifecycle owns message transformation, assembler owns system_prompt_addition."""
        snapshot = make_working_set_snapshot(items=[])
        wsm = AsyncMock()
        wsm.build_working_set = AsyncMock(return_value=snapshot)
        assembler = AsyncMock()
        assembler.assemble_from_snapshot = AsyncMock(return_value=AssembleResult(
            estimated_tokens=100,
            system_prompt_addition="CONSTRAINT: do not expose secrets",
        ))
        redis = _make_redis_mock()
        lc = _make_lifecycle(working_set_manager=wsm, context_assembler=assembler, redis=redis)

        msgs = [AgentMessage(role="user", content="hello")]
        result = await lc.assemble(AssembleParams(
            session_id=str(uuid.uuid4()), session_key=SK, messages=msgs,
        ))

        # Messages should be the transformed conversation (from lifecycle)
        assert len(result.messages) >= 1
        assert result.messages[0].role == "user"
        # system_prompt_addition should come from assembler
        assert result.system_prompt_addition == "CONSTRAINT: do not expose secrets"

    async def test_after_turn_updates_use_count_via_memory_store(self):
        """after_turn calls memory_store.update with source_id (uuid.UUID)."""
        item = make_working_set_item(source_type="fact", text="test fact")
        snapshot = make_working_set_snapshot(items=[item])
        ctx = make_session_context()
        ctx.last_snapshot_id = "snap1"
        ctx.fact_last_injection_turn = {str(item.id): 0}

        redis = _make_redis_mock()
        redis.get = AsyncMock(return_value=snapshot.model_dump_json())
        session_store = AsyncMock()
        session_store.get = AsyncMock(return_value=ctx)
        session_store.save = AsyncMock()
        memory_store = AsyncMock()
        config = ElephantBrokerConfig()
        config.successful_use.enabled = True
        lc = _make_lifecycle(redis=redis, session_context_store=session_store, memory_store=memory_store, config=config)

        await lc.after_turn(AfterTurnParams(
            session_id=str(uuid.uuid4()), session_key=SK,
            messages=[AgentMessage(role="assistant", content="irrelevant response")],
        ))

        # memory_store.update should be called with uuid.UUID source_id
        assert memory_store.update.called
        call_args = memory_store.update.call_args[0]
        assert isinstance(call_args[0], uuid.UUID)  # source_id is UUID

    async def test_ingest_batch_passes_agent_key_to_pipeline(self):
        """Turn ingest pipeline receives agent_key for GAP-6 attribution."""
        turn_ingest = AsyncMock()
        lc = _make_lifecycle(turn_ingest=turn_ingest)

        await lc.ingest_batch(IngestBatchParams(
            session_id=str(uuid.uuid4()), session_key=SK,
            messages=[AgentMessage(role="user", content="hello")],
        ))

        # Verify agent_key was passed
        assert turn_ingest.run.called
        call_kwargs = turn_ingest.run.call_args.kwargs
        assert "agent_key" in call_kwargs
        # TD-28: pipeline accepts list[AgentMessage | dict]; lifecycle now forwards
        # AgentMessage objects directly (pipeline normalizes internally).
        assert all(isinstance(m, (dict, AgentMessage)) for m in call_kwargs["messages"])

    async def test_ingest_batch_result_has_facts_stored_on_success(self):
        """Pipeline facts_stored propagates to IngestBatchResult."""
        turn_ingest = AsyncMock()
        turn_ingest.run.return_value = AsyncMock(facts_stored=3)
        lc = _make_lifecycle(turn_ingest=turn_ingest)

        result = await lc.ingest_batch(IngestBatchParams(
            session_id=str(uuid.uuid4()), session_key=SK,
            messages=[AgentMessage(role="user", content="hello")],
        ))

        assert result.facts_stored == 3
        assert result.ingested_count == 1

    async def test_ingest_batch_result_facts_stored_zero_on_failure(self):
        """Pipeline failure yields facts_stored=0."""
        turn_ingest = AsyncMock()
        turn_ingest.run.side_effect = Exception("pipeline boom")
        lc = _make_lifecycle(turn_ingest=turn_ingest)

        result = await lc.ingest_batch(IngestBatchParams(
            session_id=str(uuid.uuid4()), session_key=SK,
            messages=[AgentMessage(role="user", content="hello")],
        ))

        assert result.facts_stored == 0
        assert result.ingested_count == 1


class TestRefreshGuardRules:
    """Phase 7: lifecycle.refresh_guard_rules() reloads guard rules after procedure changes."""

    async def test_refresh_calls_load_session_rules(self):
        guard = AsyncMock()
        guard.load_session_rules = AsyncMock()
        procedure_engine = AsyncMock()
        procedure_engine.get_active_execution_ids = AsyncMock(return_value=[uuid.uuid4()])
        lc = _make_lifecycle(guard_engine=guard, procedure_engine=procedure_engine)
        await lc.refresh_guard_rules(SK, SID, "coding")
        guard.load_session_rules.assert_called_once()
        call_kwargs = guard.load_session_rules.call_args.kwargs
        assert call_kwargs["profile_name"] == "coding"
        assert call_kwargs["session_key"] == SK

    async def test_refresh_handles_no_guard(self):
        lc = _make_lifecycle(guard_engine=None)
        await lc.refresh_guard_rules(SK, SID, "coding")
        # No error

    async def test_refresh_handles_guard_error(self):
        guard = AsyncMock()
        guard.load_session_rules = AsyncMock(side_effect=Exception("boom"))
        lc = _make_lifecycle(guard_engine=guard)
        await lc.refresh_guard_rules(SK, SID, "coding")
        # No error — just logs warning


class TestDisposeGuardUnload:
    """Phase 7 / GF-15: dispose() is lightweight (no guard unload), session_end() does full cleanup."""

    async def test_dispose_does_not_call_guard_unload(self):
        """GF-15: dispose() is engine teardown only — no guard unload."""
        guard = AsyncMock()
        guard.unload_session = AsyncMock()
        redis = _make_redis_mock()
        lc = _make_lifecycle(guard_engine=guard, redis=redis)
        await lc.dispose(SK, SID)
        guard.unload_session.assert_not_called()

    async def test_session_end_calls_guard_unload(self):
        """GF-15: session_end() handles actual cleanup including guard unload."""
        guard = AsyncMock()
        guard.unload_session = AsyncMock()
        redis = _make_redis_mock()
        lc = _make_lifecycle(guard_engine=guard, redis=redis)
        await lc.session_end(SK, SID)
        guard.unload_session.assert_called_once()
        call_args = guard.unload_session.call_args[0]
        assert isinstance(call_args[0], uuid.UUID)

    async def test_session_end_handles_guard_error(self):
        """GF-15: session_end() gracefully handles guard errors."""
        guard = AsyncMock()
        guard.unload_session = AsyncMock(side_effect=Exception("boom"))
        redis = _make_redis_mock()
        lc = _make_lifecycle(guard_engine=guard, redis=redis)
        await lc.session_end(SK, SID)
        # No error raised — graceful degradation


# ======================================================================
# Auto-compaction in after_turn (Fix #33)
# ======================================================================


class TestAutoCompaction:
    """Fix #33: auto-compaction triggers in after_turn() when tokens exceed threshold."""

    def _make_messages_json(self, count: int, chars_per_msg: int = 200) -> list[bytes]:
        """Create serialized AgentMessage JSON blobs for Redis mock."""
        msgs = []
        for i in range(count):
            m = AgentMessage(role="assistant", content="x" * chars_per_msg)
            msgs.append(m.model_dump_json().encode())
        return msgs

    async def test_auto_compaction_triggers_above_threshold(self):
        """Auto-compaction fires when total tokens exceed target * cadence_multiplier."""
        compaction = AsyncMock()
        compaction.compact_with_context = AsyncMock(
            return_value=CompactResult(ok=True, compacted=True, reason="compacted"),
        )
        redis = _make_redis_mock()
        # 20 messages × 2000 chars ÷ 4 = 500 tokens/msg × 20 = 10000 total
        # Default threshold: 4000 * 2.0 = 8000 → 10000 > 8000 → should trigger
        redis.lrange = AsyncMock(return_value=self._make_messages_json(20, 2000))

        lc = _make_lifecycle(compaction_engine=compaction, redis=redis)
        params = AfterTurnParams(session_id=SID, session_key=SK)
        await lc.after_turn(params)

        # compact() should have been called (which delegates to compact_with_context)
        assert compaction.compact_with_context.called

    async def test_auto_compaction_does_not_trigger_below_threshold(self):
        """Auto-compaction does NOT fire when tokens are below threshold."""
        compaction = AsyncMock()
        compaction.compact_with_context = AsyncMock(
            return_value=CompactResult(ok=True, compacted=False, reason="below threshold"),
        )
        redis = _make_redis_mock()
        # 2 messages × 20 chars ÷ 4 = 5 tokens per msg = 10 total
        # Default threshold: 4000 * 2.0 = 8000 → 10 < 8000 → should NOT trigger
        redis.lrange = AsyncMock(return_value=self._make_messages_json(2, 20))

        lc = _make_lifecycle(compaction_engine=compaction, redis=redis)
        params = AfterTurnParams(session_id=SID, session_key=SK)
        await lc.after_turn(params)

        compaction.compact_with_context.assert_not_called()

    async def test_auto_compaction_skipped_when_no_redis(self):
        """Auto-compaction is skipped when redis is None."""
        compaction = AsyncMock()
        compaction.compact_with_context = AsyncMock()
        lc = _make_lifecycle(compaction_engine=compaction, redis=None)

        params = AfterTurnParams(session_id=SID, session_key=SK)
        await lc.after_turn(params)

        compaction.compact_with_context.assert_not_called()

    async def test_auto_compaction_handles_compact_exception(self):
        """Auto-compaction gracefully handles exception from compact()."""
        compaction = AsyncMock()
        compaction.compact_with_context = AsyncMock(side_effect=Exception("compact boom"))
        redis = _make_redis_mock()
        # Enough tokens to trigger (20 × 2000/4 = 10000 > 8000 threshold)
        redis.lrange = AsyncMock(return_value=self._make_messages_json(20, 2000))

        lc = _make_lifecycle(compaction_engine=compaction, redis=redis)
        params = AfterTurnParams(session_id=SID, session_key=SK)
        # Should not raise
        await lc.after_turn(params)

    async def test_auto_compaction_cadence_fallback_to_balanced(self):
        """Unknown cadence values fall back to 'balanced'."""
        from elephantbroker.runtime.compaction.engine import CADENCE_MULTIPLIERS

        compaction = AsyncMock()
        compaction.compact_with_context = AsyncMock(
            return_value=CompactResult(ok=True, compacted=True, reason="ok"),
        )
        redis = _make_redis_mock()
        # 20 × 2000/4 = 10000 > 8000 threshold (balanced fallback: 4000*2.0)
        redis.lrange = AsyncMock(return_value=self._make_messages_json(20, 2000))

        # Override profile with unknown cadence
        from elephantbroker.schemas.profile import CompactionPolicy
        profile = make_profile_policy(
            compaction=CompactionPolicy(cadence="unknown_cadence", target_tokens=4000),
        )
        ctx = make_session_context(profile=profile)
        session_store = AsyncMock()
        session_store.get = AsyncMock(return_value=ctx)
        session_store.save = AsyncMock()
        session_store._effective_ttl = lambda p: 86400

        lc = _make_lifecycle(
            compaction_engine=compaction,
            redis=redis,
            session_context_store=session_store,
        )
        params = AfterTurnParams(session_id=SID, session_key=SK)
        await lc.after_turn(params)

        # With fallback to balanced (2.0), threshold = 4000*2.0 = 8000
        # 20 msgs × 2000 chars ÷ 4 = 500 tokens/msg × 20 = 10000 > 8000 → triggers
        assert compaction.compact_with_context.called

    async def test_auto_compaction_emits_error_metric_on_failure(self):
        """Failed auto-compaction emits inc_lifecycle_error metric."""
        compaction = AsyncMock()
        compaction.compact_with_context = AsyncMock(
            return_value=CompactResult(ok=False, compacted=False, reason="engine error"),
        )
        redis = _make_redis_mock()
        # 20 × 2000/4 = 10000 > 8000 threshold
        redis.lrange = AsyncMock(return_value=self._make_messages_json(20, 2000))
        metrics = MagicMock(spec=MetricsContext)

        lc = _make_lifecycle(compaction_engine=compaction, redis=redis, metrics=metrics)
        params = AfterTurnParams(session_id=SID, session_key=SK)
        await lc.after_turn(params)

        metrics.inc_lifecycle_error.assert_called_with("compact", "auto_compaction")
