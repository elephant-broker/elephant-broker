"""Tests for runtime/redis_keys.py — gateway-scoped Redis key builder."""
import logging
from unittest.mock import AsyncMock, MagicMock

import pytest

from elephantbroker.runtime.redis_keys import RedisKeyBuilder, touch_session_keys


def test_ingest_buffer_key_includes_gateway():
    keys = RedisKeyBuilder("gw-prod")
    assert keys.ingest_buffer("agent:main:main") == "eb:gw-prod:ingest_buffer:agent:main:main"


def test_recent_facts_key_includes_gateway():
    keys = RedisKeyBuilder("gw-prod")
    assert keys.recent_facts("agent:main:main") == "eb:gw-prod:recent_facts:agent:main:main"


def test_session_goals_key_includes_gateway_and_session():
    keys = RedisKeyBuilder("gw-prod")
    result = keys.session_goals("agent:main:main")
    assert result == "eb:gw-prod:session_goals:agent:main:main"


def test_ws_snapshot_key_includes_gateway():
    keys = RedisKeyBuilder("gw-prod")
    result = keys.ws_snapshot("agent:main:main", "sid1")
    assert result == "eb:gw-prod:ws_snapshot:agent:main:main:sid1"


def test_ws_snapshot_scan_pattern_includes_gateway_and_glob():
    keys = RedisKeyBuilder("gw-prod")
    result = keys.ws_snapshot_scan_pattern("sid1")
    assert result == "eb:gw-prod:ws_snapshot:*:sid1"


def test_ws_snapshot_scan_pattern_different_gateways_different_patterns():
    keys_a = RedisKeyBuilder("gw-a")
    keys_b = RedisKeyBuilder("gw-b")
    assert keys_a.ws_snapshot_scan_pattern("sid") != keys_b.ws_snapshot_scan_pattern("sid")


def test_guard_history_scan_pattern_includes_gateway_and_glob():
    keys = RedisKeyBuilder("gw-prod")
    assert keys.guard_history_scan_pattern() == "eb:gw-prod:guard_history:*"


def test_guard_history_scan_pattern_different_gateways_different_patterns():
    keys_a = RedisKeyBuilder("gw-a")
    keys_b = RedisKeyBuilder("gw-b")
    assert keys_a.guard_history_scan_pattern() != keys_b.guard_history_scan_pattern()


def test_compact_state_key_includes_gateway():
    keys = RedisKeyBuilder("gw-prod")
    result = keys.compact_state("sk", "sid")
    assert result == "eb:gw-prod:compact_state:sk:sid"


def test_session_parent_key_includes_gateway():
    keys = RedisKeyBuilder("gw-prod")
    assert keys.session_parent("sk") == "eb:gw-prod:session_parent:sk"


def test_embedding_cache_not_gateway_scoped():
    result = RedisKeyBuilder.embedding_cache("abc123")
    assert result == "eb:emb_cache:abc123"
    # Same regardless of gateway
    keys_a = RedisKeyBuilder("gw-a")
    keys_b = RedisKeyBuilder("gw-b")
    assert keys_a.embedding_cache("abc") == keys_b.embedding_cache("abc")


def test_different_gateways_different_keys():
    keys_a = RedisKeyBuilder("gw-a")
    keys_b = RedisKeyBuilder("gw-b")
    assert keys_a.session_goals("sk") != keys_b.session_goals("sk")


def test_same_gateway_same_keys():
    keys1 = RedisKeyBuilder("gw-prod")
    keys2 = RedisKeyBuilder("gw-prod")
    assert keys1.ingest_buffer("sk") == keys2.ingest_buffer("sk")


def test_prefix_property():
    keys = RedisKeyBuilder("gw-test")
    assert keys.prefix == "eb:gw-test"


# ---------------------------------------------------------------------------
# 5-214: empty gateway_id warning — surfaces bootstrap bugs without breaking
# legitimate empty-id paths (test conftests, buffer default-branch fallback).
# ---------------------------------------------------------------------------


def test_empty_gateway_id_emits_warning(caplog):
    """An empty gateway_id must emit a WARNING log so a missed bootstrap
    wiring surfaces in logs. The warning is non-fatal — the builder still
    constructs — because some test/default paths legitimately pass "". """
    with caplog.at_level(logging.WARNING, logger="elephantbroker.runtime.redis_keys"):
        keys = RedisKeyBuilder("")
    # Key still builds (non-fatal warning).
    assert keys.prefix == "eb:"
    warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
    assert len(warnings) == 1
    msg = warnings[0].getMessage()
    assert "empty gateway_id" in msg
    assert "eb::" in msg  # Warning must name the surface symptom


def test_non_empty_gateway_id_no_warning(caplog):
    """Happy path: a non-empty gateway_id does NOT emit any warning."""
    with caplog.at_level(logging.WARNING, logger="elephantbroker.runtime.redis_keys"):
        RedisKeyBuilder("gw-prod")
    assert [r for r in caplog.records if r.levelno == logging.WARNING] == []


# ---------------------------------------------------------------------------
# touch_session_keys tests (Amendment 6.1)
# ---------------------------------------------------------------------------


def _make_pipeline_mock(results=None):
    pipe = MagicMock()
    pipe.expire = MagicMock()
    pipe.execute = AsyncMock(return_value=results or [1] * 10)
    return pipe


@pytest.mark.asyncio
async def test_touch_session_keys_expires_all_base_keys():
    keys = RedisKeyBuilder("gw-test")
    pipe = _make_pipeline_mock()
    redis = AsyncMock()
    redis.pipeline = MagicMock(return_value=pipe)

    await touch_session_keys(keys, redis, "sk", "sid", 172800)

    assert pipe.expire.call_count == 10
    expected = [
        "eb:gw-test:session_context:sk:sid",
        "eb:gw-test:session_messages:sk:sid",
        "eb:gw-test:session_goals:sk",
        "eb:gw-test:session_artifacts:sk:sid",
        "eb:gw-test:ws_snapshot:sk:sid",
        "eb:gw-test:compact_state:sk:sid",
        "eb:gw-test:compact_state_obj:sk:sid",
        "eb:gw-test:procedure_exec:sk:sid",
        "eb:gw-test:guard_history:sk:sid",
        "eb:gw-test:fact_domains:sk:sid",
    ]
    actual = [call.args[0] for call in pipe.expire.call_args_list]
    assert actual == expected
    for call in pipe.expire.call_args_list:
        assert call.args[1] == 172800
    pipe.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_touch_returns_count_of_existing_keys():
    keys = RedisKeyBuilder("gw-test")
    pipe = _make_pipeline_mock(results=[1, 1, 0, 0, 1, 0, 0, 0, 0, 0])
    redis = AsyncMock()
    redis.pipeline = MagicMock(return_value=pipe)
    count = await touch_session_keys(keys, redis, "sk", "sid", 172800)
    assert count == 3


@pytest.mark.asyncio
async def test_touch_include_parent_touches_parent_and_children():
    keys = RedisKeyBuilder("gw-test")
    pipe = _make_pipeline_mock(results=[1] * 11)
    redis = AsyncMock()
    redis.pipeline = MagicMock(return_value=pipe)
    redis.get = AsyncMock(return_value="parent-sk")
    redis.expire = AsyncMock()

    await touch_session_keys(keys, redis, "sk", "sid", 172800, include_parent=True)

    assert pipe.expire.call_count == 11  # 10 base (8+2 Phase 7) + session_parent
    redis.get.assert_awaited_once()  # looked up parent
    redis.expire.assert_awaited_once_with("eb:gw-test:session_children:parent-sk", 172800)


@pytest.mark.asyncio
async def test_touch_include_parent_no_parent_found():
    keys = RedisKeyBuilder("gw-test")
    pipe = _make_pipeline_mock(results=[1] * 11)
    redis = AsyncMock()
    redis.pipeline = MagicMock(return_value=pipe)
    redis.get = AsyncMock(return_value=None)
    redis.expire = AsyncMock()

    await touch_session_keys(keys, redis, "sk", "sid", 172800, include_parent=True)
    redis.expire.assert_not_awaited()  # no children to touch


@pytest.mark.asyncio
async def test_touch_no_keys_exist():
    keys = RedisKeyBuilder("gw-test")
    pipe = _make_pipeline_mock(results=[0] * 10)
    redis = AsyncMock()
    redis.pipeline = MagicMock(return_value=pipe)
    count = await touch_session_keys(keys, redis, "sk", "sid", 172800)
    assert count == 0


# ---------------------------------------------------------------------------
# TF-FN-017: gap-filling pins for key builders not directly covered above,
# plus two PROD-risk pins on #1516 (gateway_id values that corrupt the key
# namespace because the builder does zero validation).
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "make_key,expected",
    [
        # Format: (builder_method_call, expected_key)
        # Source-grounded against redis_keys.py:79-130.
        (lambda k: k.session_context("sk", "sid"), "eb:gw-prod:session_context:sk:sid"),
        (lambda k: k.procedure_exec("sk", "sid"), "eb:gw-prod:procedure_exec:sk:sid"),
        (lambda k: k.session_messages("sk", "sid"), "eb:gw-prod:session_messages:sk:sid"),
        (lambda k: k.guard_history("sk", "sid"), "eb:gw-prod:guard_history:sk:sid"),
        # approval() interleaves agent_id BEFORE the literal "approval" segment —
        # pinning the ordering is the main point of this case.
        (lambda k: k.approval("main", "req-1"), "eb:gw-prod:main:approval:req-1"),
        # Consolidation lock + status keys are per-gateway (distributed lock for
        # consolidation runs, AD-10). No session_key / session_id component.
        (lambda k: k.consolidation_lock(), "eb:gw-prod:consolidation_lock"),
        (lambda k: k.consolidation_status(), "eb:gw-prod:consolidation_status"),
    ],
    ids=[
        "session_context",
        "procedure_exec",
        "session_messages",
        "guard_history",
        "approval",
        "consolidation_lock",
        "consolidation_status",
    ],
)
def test_gateway_scoped_key_formats(make_key, expected):
    """G1 (TF-FN-017): gap-fill exact-format pins for 7 key builders not
    previously covered by direct-format tests.

    Existing coverage already pins ingest_buffer, recent_facts, session_goals,
    ws_snapshot (+ scan_pattern), compact_state, session_parent,
    guard_history_scan_pattern, embedding_cache, plus the
    different-gateways-different-keys cross-builder invariant. This adds the
    last seven first-class methods so every non-deprecated public key helper
    on RedisKeyBuilder has a format lock-down test.

    If a key's segment order ever changes (as PR #11 ISSUE-15/18 changed
    session_goals from `{sk}:{sid}` to `{sk}`, see D18), one of these will
    fail and force an explicit migration decision.
    """
    keys = RedisKeyBuilder("gw-prod")
    assert make_key(keys) == expected


def test_approvals_by_session_key_format():
    """G2 (TF-FN-017): pin the `approvals_by_session` key format — same
    agent-id-before-literal interleaving as `approval`, source
    redis_keys.py:113-114.

    Complements G1's `approval` row so both approval-family helpers are
    format-locked. Any refactor that swaps the agent-id position (e.g.,
    moves it after the literal "approvals_by_session") will fail this test
    and force an explicit migration decision.
    """
    keys = RedisKeyBuilder("gw-prod")
    assert keys.approvals_by_session("main", "sid-42") == "eb:gw-prod:main:approvals_by_session:sid-42"


def test_redis_key_builder_accepts_any_gateway_id_string_documented_permissive():
    """G3 (TF-FN-017): pins `RedisKeyBuilder`'s deliberate permissive contract.

    `RedisKeyBuilder.__init__` (redis_keys.py:16-34) performs **no validation**
    on `gateway_id` beyond the empty-string WARNING. Values containing Redis
    key-separator (`:`) or glob metacharacters (`*`, `?`, `[`, `]`) are
    accepted verbatim and interpolated into prefixes / SCAN patterns.

    This is defense-in-depth positioning: validation lives at
    `_validate_startup_safety()` / A6 (container.py) where we can refuse to
    boot with a forbidden gateway_id, and at the middleware layer in
    principle. The builder itself trusts its caller — so adapters that
    instantiate a `RedisKeyBuilder` mid-runtime (tests, dev-branch fallback,
    future plugins) don't duplicate validation logic.

    If validation ever moves INTO the builder (e.g., raise on `:` or `*`),
    this test will break and force explicit removal of the A6 startup-safety
    duplication — which is fine, just do it in one coherent change.

    See:
    - container.py A6: refuses boot when `gateway_id` contains `: * ? [ ]`
    - test_container_startup_safety.py G4/G5: pin A6 rejection semantics
    - #1516 RESOLVED in-PR via A6 (running draft notes)
    """
    # No raise: builder takes anything a string can be, including the
    # exact forbidden characters A6 rejects.
    for gw in ("gw:prod", "gw*", "gw?prod", "gw[abc]", "gw]"):
        keys = RedisKeyBuilder(gw)
        # Prefix retains the character verbatim — the builder does not sanitize.
        assert keys.prefix == f"eb:{gw}"


# ---------------------------------------------------------------------------
# TF-FN-018 G11, G12 — gap-fill pins complementing the TF-FN-018 bundle.
# G11 documents the embedding-cache key's CURRENT shape (excludes model
# name — a mixed-model deployment would produce a collision surface).
# G12 pins that touch_session_keys only refreshes gateway-scoped keys.
# ---------------------------------------------------------------------------


def test_embedding_cache_key_excludes_model_name():
    """G11 (TF-FN-018): pins the CURRENT ``embedding_cache`` key shape
    ``eb:emb_cache:{hash}`` — intentionally excludes the embedding model
    name.

    Implication: same text hashed identically across embedding models
    produces the same key. In a single-model deployment this is fine and
    deliberate (embedding cache is global, per CLAUDE.md "Global (NOT
    gateway-scoped)" section at redis_keys.py:132-137). But a mixed-model
    deployment (e.g., rolling an upgrade from model A to model B) would
    cache hits from A and serve them to B — silently corrupting vector
    search quality.

    This is NOT a current bug (single-model in prod today), but the shape
    decision should surface in CI if we ever move to multi-model. No TD
    filed today — promote to TD only when mixed-model deployment lands on
    the roadmap; TF-FN-019 is the natural place to revisit.
    """
    key = RedisKeyBuilder.embedding_cache("sha256-abc123")
    # Exact format: no model-name segment, no gateway prefix.
    assert key == "eb:emb_cache:sha256-abc123"
    # Static method: two different gateways produce byte-identical keys.
    assert RedisKeyBuilder("gw-a").embedding_cache("h") == \
           RedisKeyBuilder("gw-b").embedding_cache("h")


@pytest.mark.asyncio
async def test_touch_session_keys_returns_gateway_scoped_paths_only():
    """G12 (TF-FN-018): every key that ``touch_session_keys`` refreshes via
    EXPIRE must be prefixed with the gateway's ``eb:{gateway_id}:``
    namespace.

    Guards against a regression where a new session-level key is added to
    ``key_list`` in ``touch_session_keys`` (redis_keys.py:140-182) but
    forgets the gateway prefix — which would silently break per-tenant
    TTL refresh isolation.

    Uses the same mock pattern as the existing
    ``test_touch_session_keys_expires_all_base_keys`` test (MagicMock
    pipeline recording EXPIRE calls) but asserts the cross-cutting
    invariant — every key starts with the gateway prefix — instead of the
    exact 10-key enumeration. Complements, not replaces, that test.
    """
    keys = RedisKeyBuilder("gw-tenant-42")
    pipe = _make_pipeline_mock()
    redis = AsyncMock()
    redis.pipeline = MagicMock(return_value=pipe)

    await touch_session_keys(keys, redis, "sk", "sid", 172800)

    # Walk every EXPIRE call and verify the key starts with the gateway prefix.
    touched_keys = [call.args[0] for call in pipe.expire.call_args_list]
    assert touched_keys, "touch_session_keys did not invoke EXPIRE on any key"
    for key in touched_keys:
        assert key.startswith("eb:gw-tenant-42:"), (
            f"touch_session_keys refreshed {key!r} which is NOT scoped to "
            f"gateway 'gw-tenant-42'. All session-level TTL refreshes must "
            f"stay within the gateway prefix."
        )


