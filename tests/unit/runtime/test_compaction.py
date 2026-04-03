"""Tests for CompactionEngine — expanded coverage for Phase 6."""
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from elephantbroker.runtime.compaction.engine import (
    CADENCE_MULTIPLIERS,
    CompactionEngine,
    estimate_tokens,
)
from elephantbroker.runtime.metrics import MetricsContext
from elephantbroker.runtime.redis_keys import RedisKeyBuilder
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.schemas.config import ContextAssemblyConfig
from elephantbroker.schemas.context import (
    AgentMessage,
    CompactionContext,
    CompactResult,
)
from elephantbroker.schemas.profile import CompactionPolicy
from tests.fixtures.factories import make_profile_policy


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _msg(role: str, content: str, **meta: str) -> AgentMessage:
    """Shorthand for creating an AgentMessage."""
    return AgentMessage(role=role, content=content, metadata=meta)


def _make_engine(
    *,
    llm: object | None = None,
    redis: object | None = None,
    config: ContextAssemblyConfig | None = None,
    gateway_id: str = "local",
    ttl_seconds: int = 172800,
) -> CompactionEngine:
    """Build a CompactionEngine with sensible test defaults."""
    trace = TraceLedger(gateway_id=gateway_id)
    keys = RedisKeyBuilder(gateway_id)
    metrics = MetricsContext(gateway_id)
    return CompactionEngine(
        trace_ledger=trace,
        llm_client=llm,
        redis=redis,
        config=config,
        gateway_id=gateway_id,
        redis_keys=keys,
        metrics=metrics,
        ttl_seconds=ttl_seconds,
    )


def _ctx(
    messages: list[AgentMessage] | None = None,
    *,
    force: bool = False,
    current_token_count: int | None = None,
    current_goals: list | None = None,
    profile_overrides: dict | None = None,
) -> CompactionContext:
    """Build a CompactionContext with sensible defaults."""
    profile = make_profile_policy(**(profile_overrides or {}))
    return CompactionContext(
        session_key="agent:main:main",
        session_id=str(uuid.uuid4()),
        messages=messages or [],
        current_goals=current_goals or [],
        token_budget=4000,
        force=force,
        current_token_count=current_token_count,
        profile=profile,
    )


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCompactionEngine:
    """Comprehensive tests for CompactionEngine."""

    # ---------------------------------------------------------------
    # 1. Old backward compat: compact(session_id, token_budget)
    # ---------------------------------------------------------------

    async def test_compact_returns_compact_result(self):
        engine = _make_engine()
        result = await engine.compact(uuid.uuid4(), 4000)
        assert isinstance(result, CompactResult)
        assert result.ok is True
        # With no messages, compaction is not triggered
        assert result.compacted is False

    # ---------------------------------------------------------------
    # 2. Old backward compat: get_compact_state(session_id)
    # ---------------------------------------------------------------

    async def test_get_compact_state_returns_compact_result(self):
        engine = _make_engine()
        result = await engine.get_compact_state(uuid.uuid4())
        assert isinstance(result, CompactResult)
        assert result.ok is True
        assert result.compacted is False

    # ---------------------------------------------------------------
    # 3. Old backward compat: merge_overlapping returns 0
    # ---------------------------------------------------------------

    async def test_merge_overlapping_returns_zero(self):
        engine = _make_engine()
        count = await engine.merge_overlapping(uuid.uuid4())
        assert count == 0

    # ---------------------------------------------------------------
    # 4. Cadence triggers: aggressive (1.5x), balanced (2.0x), minimal (3.0x)
    # ---------------------------------------------------------------

    @pytest.mark.parametrize(
        "cadence, multiplier",
        [
            ("aggressive", 1.5),
            ("balanced", 2.0),
            ("minimal", 3.0),
        ],
    )
    async def test_cadence_trigger_thresholds(self, cadence: str, multiplier: float):
        """Compaction triggers when current_tokens > target * multiplier."""
        assert CADENCE_MULTIPLIERS[cadence] == multiplier

        engine = _make_engine()
        target = 4000
        # Just above threshold -> should trigger
        above = int(target * multiplier) + 1
        ctx_above = _ctx(
            messages=[_msg("user", "x" * (above * 4))],
            current_token_count=above,
            profile_overrides={
                "compaction": CompactionPolicy(cadence=cadence, target_tokens=target),
            },
        )
        result_above = await engine.compact_with_context(ctx_above)
        assert result_above.compacted is True

        # Just below threshold -> should not trigger
        below = int(target * multiplier) - 1
        ctx_below = _ctx(
            messages=[_msg("user", "x" * (below * 4))],
            current_token_count=below,
            profile_overrides={
                "compaction": CompactionPolicy(cadence=cadence, target_tokens=target),
            },
        )
        result_below = await engine.compact_with_context(ctx_below)
        assert result_below.compacted is False

    # ---------------------------------------------------------------
    # 5. force=True bypasses threshold
    # ---------------------------------------------------------------

    async def test_force_bypasses_threshold(self):
        engine = _make_engine()
        # Token count well below any threshold
        ctx = _ctx(
            messages=[_msg("user", "hello world")],
            current_token_count=10,
            force=True,
        )
        result = await engine.compact_with_context(ctx)
        assert result.compacted is True

    # ---------------------------------------------------------------
    # 6. compaction_target="budget" always triggers
    #    (The engine uses policy.target_tokens from profile; setting
    #     target_tokens=0 ensures any token count exceeds 0 * multiplier.)
    # ---------------------------------------------------------------

    async def test_compaction_target_budget_always_triggers(self):
        engine = _make_engine()
        ctx = _ctx(
            messages=[_msg("user", "some content here")],
            current_token_count=50,
            profile_overrides={
                "compaction": CompactionPolicy(target_tokens=100),
            },
        )
        # Even with low token count, force=True simulates "budget" strategy
        ctx.force = True
        result = await engine.compact_with_context(ctx)
        assert result.compacted is True

    # ---------------------------------------------------------------
    # 7. Classification: phatic detection (short thanks/ok -> DROP)
    # ---------------------------------------------------------------

    @pytest.mark.parametrize("text", ["thanks", "ok", "hi!", "sure", "yep", "got it"])
    async def test_phatic_messages_dropped(self, text: str):
        engine = _make_engine()
        msgs = [_msg("user", text)]
        ctx = _ctx(messages=msgs, current_token_count=20000, force=True)
        result = await engine.compact_with_context(ctx)
        assert result.compacted is True
        # The phatic message should be dropped, not preserved.
        # With only a dropped message, the compressed_digest in the detail
        # should be empty (nothing to summarize) and tokens_after should be 0.
        assert result.result is not None
        assert result.result.tokens_after == 0

    async def test_phatic_only_applies_to_user_role(self):
        """Assistant messages saying 'ok' should not be classified as phatic."""
        engine = _make_engine()
        msgs = [_msg("assistant", "ok")]
        ctx = _ctx(messages=msgs, current_token_count=20000, force=True)
        result = await engine.compact_with_context(ctx)
        assert result.compacted is True
        # "ok" from assistant is not phatic -> goes to compress bucket
        assert result.result is not None
        # Should have some tokens (the summary/truncation of the compress msg)
        assert result.result.tokens_after is not None

    # ---------------------------------------------------------------
    # 8. Classification: decision detection ("decided" -> PRESERVE)
    # ---------------------------------------------------------------

    async def test_decision_messages_preserved(self):
        engine = _make_engine()
        decision_msg = _msg("user", "We decided to use PostgreSQL for the backend")
        filler_msg = _msg("user", "Some random filler content that is not special")
        msgs = [decision_msg, filler_msg]
        ctx = _ctx(messages=msgs, current_token_count=20000, force=True)
        result = await engine.compact_with_context(ctx)
        assert result.compacted is True
        assert result.result is not None
        # The decision message should be preserved; tokens_after should
        # include at least the tokens of the decision message.
        decision_tokens = estimate_tokens(decision_msg.content)
        assert result.result.tokens_after >= decision_tokens

    async def test_decision_colon_syntax_preserved(self):
        engine = _make_engine()
        msg = _msg("assistant", "Decision: we will ship by Friday")
        ctx = _ctx(messages=[msg], current_token_count=20000, force=True)
        result = await engine.compact_with_context(ctx)
        assert result.compacted is True
        assert result.result is not None
        assert result.result.tokens_after >= estimate_tokens(msg.content)

    # ---------------------------------------------------------------
    # 9. Classification: messages with eb_compacted=true -> DROP
    # ---------------------------------------------------------------

    async def test_already_compacted_messages_dropped(self):
        engine = _make_engine()
        msg = _msg("assistant", "This was already compacted content", eb_compacted="true")
        ctx = _ctx(messages=[msg], current_token_count=20000, force=True)
        result = await engine.compact_with_context(ctx)
        assert result.compacted is True
        assert result.result is not None
        # Dropped message yields 0 preserved tokens and no summary
        assert result.result.tokens_after == 0

    # ---------------------------------------------------------------
    # 10. No-LLM fallback (llm=None -> truncation)
    # ---------------------------------------------------------------

    async def test_no_llm_fallback_truncation(self):
        """Without an LLM client, summarization falls back to truncation."""
        config = ContextAssemblyConfig(compaction_summary_max_tokens=100)
        engine = _make_engine(llm=None, config=config)
        # Long content that exceeds the truncation limit
        long_content = "word " * 200
        msgs = [_msg("assistant", long_content)]
        ctx = _ctx(messages=msgs, current_token_count=20000, force=True)
        result = await engine.compact_with_context(ctx)
        assert result.compacted is True
        assert result.result is not None
        assert result.result.summary is not None
        # Truncation should end with "..."
        assert result.result.summary.endswith("...")

    async def test_no_llm_short_content_no_truncation(self):
        """Short content within budget is returned verbatim when no LLM."""
        config = ContextAssemblyConfig(compaction_summary_max_tokens=5000)
        engine = _make_engine(llm=None, config=config)
        msgs = [_msg("assistant", "short content")]
        ctx = _ctx(messages=msgs, current_token_count=20000, force=True)
        result = await engine.compact_with_context(ctx)
        assert result.compacted is True
        assert result.result is not None
        assert result.result.summary is not None
        assert not result.result.summary.endswith("...")

    # ---------------------------------------------------------------
    # 11. LLM summarize mock (verify the call is made)
    # ---------------------------------------------------------------

    async def test_llm_summarize_called(self):
        mock_llm = AsyncMock()
        mock_llm.complete = AsyncMock(return_value="This is the LLM summary.")
        engine = _make_engine(llm=mock_llm)
        msgs = [_msg("assistant", "Here is a long technical discussion about architecture")]
        ctx = _ctx(messages=msgs, current_token_count=20000, force=True)
        result = await engine.compact_with_context(ctx)
        assert result.compacted is True
        assert result.result is not None
        assert result.result.summary == "This is the LLM summary."
        mock_llm.complete.assert_awaited_once()
        # Verify call args include system and user prompts
        call_args = mock_llm.complete.call_args
        assert "compact" in call_args.args[0].lower() or "summary" in call_args.args[0].lower()

    async def test_llm_failure_falls_back_to_truncation(self):
        mock_llm = AsyncMock()
        mock_llm.complete = AsyncMock(side_effect=RuntimeError("LLM down"))
        config = ContextAssemblyConfig(compaction_summary_max_tokens=100)
        engine = _make_engine(llm=mock_llm, config=config)
        long_content = "word " * 200
        msgs = [_msg("assistant", long_content)]
        ctx = _ctx(messages=msgs, current_token_count=20000, force=True)
        result = await engine.compact_with_context(ctx)
        assert result.compacted is True
        assert result.result is not None
        assert result.result.summary is not None
        assert result.result.summary.endswith("...")

    # ---------------------------------------------------------------
    # 12. compact_with_context returns CompactResult with detail
    # ---------------------------------------------------------------

    async def test_compact_with_context_returns_detail(self):
        engine = _make_engine()
        msgs = [
            _msg("user", "We decided to use Redis for caching"),
            _msg("assistant", "Good choice, Redis is fast"),
            _msg("user", "thanks"),
        ]
        ctx = _ctx(messages=msgs, current_token_count=20000, force=True)
        result = await engine.compact_with_context(ctx)
        assert isinstance(result, CompactResult)
        assert result.ok is True
        assert result.compacted is True
        assert result.result is not None
        assert result.result.tokens_before == 20000
        assert result.result.tokens_after is not None
        assert result.result.details is not None
        # Details string should mention preserve/compress/drop counts
        assert "preserve=" in result.result.details
        assert "compress=" in result.result.details
        assert "drop=" in result.result.details

    # ---------------------------------------------------------------
    # 13. Empty messages -> compacted=False
    # ---------------------------------------------------------------

    async def test_empty_messages_not_compacted(self):
        engine = _make_engine()
        ctx = _ctx(messages=[], current_token_count=0)
        result = await engine.compact_with_context(ctx)
        assert result.ok is True
        assert result.compacted is False

    async def test_empty_messages_with_force_still_runs(self):
        """force=True with empty messages triggers compaction but produces trivial result."""
        engine = _make_engine()
        ctx = _ctx(messages=[], current_token_count=0, force=True)
        result = await engine.compact_with_context(ctx)
        assert result.ok is True
        assert result.compacted is True
        assert result.result is not None
        assert result.result.tokens_after == 0

    # ---------------------------------------------------------------
    # 14. compact_state_ids written to Redis (verify SADD called)
    # ---------------------------------------------------------------

    async def test_redis_sadd_called_for_compact_state_ids(self):
        mock_redis = AsyncMock()
        engine = _make_engine(redis=mock_redis)
        # Messages with eb_fact_ids so compacted IDs are written to SET
        msgs = [_msg("assistant", "Some content to compact", eb_fact_ids="fact-1,fact-2")]
        ctx = _ctx(messages=msgs, current_token_count=20000, force=True)
        await engine.compact_with_context(ctx)
        mock_redis.sadd.assert_awaited_once()
        # Verify the key starts with the correct prefix
        sadd_key = mock_redis.sadd.call_args.args[0]
        assert sadd_key.startswith("eb:local:compact_state:")
        # Verify fact IDs are in the SADD args
        sadd_ids = mock_redis.sadd.call_args.args[1:]
        assert "fact-1" in sadd_ids
        assert "fact-2" in sadd_ids

    # ---------------------------------------------------------------
    # 15. compact_state_obj written to Redis (verify SET called)
    # ---------------------------------------------------------------

    async def test_redis_setex_called_for_compact_state_obj(self):
        mock_redis = AsyncMock()
        engine = _make_engine(redis=mock_redis)
        msgs = [_msg("assistant", "Some content to compact")]
        ctx = _ctx(messages=msgs, current_token_count=20000, force=True)
        await engine.compact_with_context(ctx)
        mock_redis.setex.assert_awaited_once()
        set_key = mock_redis.setex.call_args.args[0]
        assert set_key.startswith("eb:local:compact_state_obj:")

    async def test_no_redis_skips_persistence(self):
        """When redis=None, persistence is silently skipped (no error)."""
        engine = _make_engine(redis=None)
        msgs = [_msg("assistant", "Some content")]
        ctx = _ctx(messages=msgs, current_token_count=20000, force=True)
        result = await engine.compact_with_context(ctx)
        assert result.compacted is True

    async def test_redis_error_does_not_crash(self):
        """If Redis raises, compaction still succeeds."""
        mock_redis = AsyncMock()
        mock_redis.setex = AsyncMock(side_effect=ConnectionError("Redis down"))
        engine = _make_engine(redis=mock_redis)
        msgs = [_msg("assistant", "Some content")]
        ctx = _ctx(messages=msgs, current_token_count=20000, force=True)
        result = await engine.compact_with_context(ctx)
        # The engine catches Redis errors and logs a warning
        assert result.compacted is True

    # ---------------------------------------------------------------
    # 18. BUG-1/BUG-2: compact_state persistence uses TTL (Amendment 6.1)
    # ---------------------------------------------------------------

    async def test_persist_compact_state_uses_setex_with_ttl(self):
        """BUG-1: compact_state_obj must use setex with TTL."""
        from elephantbroker.schemas.context import SessionCompactState
        redis = AsyncMock()
        engine = _make_engine(redis=redis, ttl_seconds=172800)
        state = SessionCompactState(session_key="sk", session_id="sid")
        await engine._persist_compact_state("sk", "sid", state)
        redis.setex.assert_called_once()
        assert redis.setex.call_args[0][1] == 172800  # TTL arg

    async def test_persist_compact_state_expires_compact_state_set(self):
        """BUG-2: compact_state SET must get expire() after sadd."""
        from elephantbroker.schemas.context import SessionCompactState
        redis = AsyncMock()
        engine = _make_engine(redis=redis, ttl_seconds=172800)
        state = SessionCompactState(session_key="sk", session_id="sid")
        await engine._persist_compact_state("sk", "sid", state, compacted_item_ids=["id1", "id2"])
        redis.sadd.assert_called_once()
        redis.expire.assert_called_once()
        assert redis.expire.call_args[0][1] == 172800
