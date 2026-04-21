"""Tests for the refactored ProfileRegistry — inheritance, caching, org overrides."""
import os
import tempfile
import time
from unittest.mock import AsyncMock

import pytest

from elephantbroker.runtime.profiles.org_override_store import OrgOverrideStore
from elephantbroker.runtime.profiles.registry import ProfileRegistry
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.schemas.profile import GraphMode


@pytest.fixture
def trace():
    return TraceLedger()


@pytest.fixture
async def org_store():
    with tempfile.TemporaryDirectory() as tmp:
        store = OrgOverrideStore(db_path=os.path.join(tmp, "overrides.db"))
        await store.init_db()
        yield store
        await store.close()


class TestProfileRegistryBasic:
    async def test_resolve_coding_profile(self, trace):
        reg = ProfileRegistry(trace)
        p = await reg.resolve_profile("coding")
        assert p.id == "coding"
        assert p.graph_mode == GraphMode.LOCAL

    async def test_resolve_all_5_named_profiles(self, trace):
        reg = ProfileRegistry(trace)
        for name in ("coding", "research", "managerial", "worker", "personal_assistant"):
            p = await reg.resolve_profile(name)
            assert p.id == name

    async def test_unknown_profile_raises_key_error(self, trace):
        reg = ProfileRegistry(trace)
        with pytest.raises(KeyError, match="Unknown profile"):
            await reg.resolve_profile("nonexistent")

    async def test_list_profiles_returns_5_excluding_base(self, trace):
        reg = ProfileRegistry(trace)
        profiles = await reg.list_profiles()
        assert len(profiles) == 5
        assert "base" not in profiles
        assert "coding" in profiles


class TestProfileRegistryInheritance:
    async def test_inheritance_base_then_profile(self, trace):
        reg = ProfileRegistry(trace)
        p = await reg.resolve_profile("coding")
        # Coding extends base but has all fields (complete object)
        assert p.extends == "base"
        assert p.scoring_weights.turn_relevance == 1.5

    async def test_resolve_without_org_store_skips_overrides(self, trace):
        reg = ProfileRegistry(trace)  # no org_store
        p = await reg.resolve_profile("coding", org_id="acme")
        assert p.id == "coding"
        assert p.scoring_weights.turn_relevance == 1.5  # no override applied


class TestProfileRegistryOrgOverrides:
    async def test_org_override_applied_on_top(self, trace, org_store):
        reg = ProfileRegistry(trace, org_store=org_store)
        await org_store.set_override("acme", "coding", {"scoring_weights": {"evidence_strength": 0.99}})
        p = await reg.resolve_profile("coding", org_id="acme")
        assert p.scoring_weights.evidence_strength == 0.99
        assert p.scoring_weights.turn_relevance == 1.5  # other weights preserved

    async def test_org_override_from_sqlite(self, trace, org_store):
        reg = ProfileRegistry(trace, org_store=org_store)
        await org_store.set_override("acme", "research", {"budgets": {"max_prompt_tokens": 20000}})
        p = await reg.resolve_profile("research", org_id="acme")
        assert p.budgets.max_prompt_tokens == 20000

    async def test_register_org_override_persists(self, trace, org_store):
        reg = ProfileRegistry(trace, org_store=org_store)
        await reg.register_org_override("acme", "coding", {"session_data_ttl_seconds": 7200})
        p = await reg.resolve_profile("coding", org_id="acme")
        assert p.session_data_ttl_seconds == 7200

    async def test_register_org_override_unknown_profile_raises(self, trace, org_store):
        reg = ProfileRegistry(trace, org_store=org_store)
        with pytest.raises(KeyError, match="Unknown profile"):
            await reg.register_org_override("acme", "nonexistent", {"x": 1})

    async def test_delete_org_override(self, trace, org_store):
        reg = ProfileRegistry(trace, org_store=org_store)
        await org_store.set_override("acme", "coding", {"session_data_ttl_seconds": 7200})
        await reg.delete_org_override("acme", "coding")
        p = await reg.resolve_profile("coding", org_id="acme")
        assert p.session_data_ttl_seconds == 86400  # back to preset default

    async def test_resolve_with_org_id_none_skips_org_store(self, trace, org_store):
        reg = ProfileRegistry(trace, org_store=org_store)
        await org_store.set_override("acme", "coding", {"session_data_ttl_seconds": 7200})
        p = await reg.resolve_profile("coding", org_id=None)
        assert p.session_data_ttl_seconds == 86400  # not overridden


class TestProfileRegistryCaching:
    async def test_resolved_profile_is_deep_copy(self, trace):
        reg = ProfileRegistry(trace)
        p1 = await reg.resolve_profile("coding")
        p2 = await reg.resolve_profile("coding")
        p1.scoring_weights.turn_relevance = 999.0
        assert p2.scoring_weights.turn_relevance == 1.5

    async def test_cache_hit_returns_same_result(self, trace):
        reg = ProfileRegistry(trace)
        p1 = await reg.resolve_profile("coding")
        p2 = await reg.resolve_profile("coding")
        assert p1.id == p2.id
        assert p1.scoring_weights.turn_relevance == p2.scoring_weights.turn_relevance

    async def test_cache_expires_after_ttl(self, trace, org_store):
        reg = ProfileRegistry(trace, org_store=org_store, cache_ttl_seconds=0)  # instant expiry
        p1 = await reg.resolve_profile("coding", org_id="acme")
        await org_store.set_override("acme", "coding", {"session_data_ttl_seconds": 10800})
        # Don't invalidate cache — let TTL expire
        p2 = await reg.resolve_profile("coding", org_id="acme")
        assert p2.session_data_ttl_seconds == 10800

    async def test_register_override_invalidates_cache(self, trace, org_store):
        reg = ProfileRegistry(trace, org_store=org_store, cache_ttl_seconds=300)
        await reg.resolve_profile("coding", org_id="acme")  # populate cache
        await reg.register_org_override("acme", "coding", {"session_data_ttl_seconds": 5400})
        p = await reg.resolve_profile("coding", org_id="acme")
        assert p.session_data_ttl_seconds == 5400

    async def test_delete_override_invalidates_cache(self, trace, org_store):
        reg = ProfileRegistry(trace, org_store=org_store, cache_ttl_seconds=300)
        await org_store.set_override("acme", "coding", {"session_data_ttl_seconds": 5400})
        await reg.resolve_profile("coding", org_id="acme")  # populate cache
        await reg.delete_org_override("acme", "coding")
        p = await reg.resolve_profile("coding", org_id="acme")
        assert p.session_data_ttl_seconds == 86400  # back to preset

    async def test_configurable_cache_ttl(self, trace):
        reg = ProfileRegistry(trace, cache_ttl_seconds=60)
        assert reg._cache_ttl == 60


class TestEffectiveIngestBatchSize:
    """P6: ingest_batch_size resolver prefers profile override, else LLMConfig."""

    def test_effective_ingest_batch_size_returns_profile_value_or_global(self, trace):
        from elephantbroker.schemas.config import LLMConfig
        from elephantbroker.schemas.profile import ProfilePolicy

        reg = ProfileRegistry(trace)
        llm = LLMConfig(ingest_batch_size=6)

        # None override → fall back to global LLMConfig.
        policy_default = ProfilePolicy(id="x", name="X")
        assert policy_default.ingest_batch_size is None
        assert reg.effective_ingest_batch_size(policy_default, llm) == 6

        # Explicit override → profile value wins.
        policy_override = ProfilePolicy(id="x", name="X", ingest_batch_size=4)
        assert reg.effective_ingest_batch_size(policy_override, llm) == 4


class TestEffectiveSuccessfulUseThresholds:
    """T-2: successful_use_thresholds resolver returns the policy's override
    when set, otherwise a fresh SuccessfulUseThresholds() with module defaults.
    Mirrors the effective_ingest_batch_size precedent exactly.
    """

    def test_returns_defaults_when_unset(self, trace):
        from elephantbroker.schemas.profile import ProfilePolicy, SuccessfulUseThresholds

        reg = ProfileRegistry(trace)
        policy = ProfilePolicy(id="x", name="X")
        assert policy.successful_use_thresholds is None

        result = reg.effective_successful_use_thresholds(policy)
        assert isinstance(result, SuccessfulUseThresholds)
        # Defaults (J-1 baseline) — caller must receive a usable instance,
        # not None, even when the profile left the field unset.
        assert result.s1_direct_quote_ratio == 0.15
        assert result.s2_tool_correlation_overlap == 0.3
        assert result.s3_jaccard_score == 0.15
        assert result.use_confidence_gate == 0.15
        assert result.s6_ignored_turns_floor == 3

    def test_returns_override_when_set(self, trace):
        from elephantbroker.schemas.profile import ProfilePolicy, SuccessfulUseThresholds

        reg = ProfileRegistry(trace)
        override = SuccessfulUseThresholds(
            s1_direct_quote_ratio=0.05,
            s2_tool_correlation_overlap=0.5,
            s3_jaccard_score=0.22,
            use_confidence_gate=0.42,
            s6_ignored_turns_floor=7,
        )
        policy = ProfilePolicy(id="x", name="X", successful_use_thresholds=override)

        result = reg.effective_successful_use_thresholds(policy)
        # The resolver must return the exact policy override — not a copy
        # with modified fields, not the defaults.
        assert result is override
        assert result.s1_direct_quote_ratio == 0.05
        assert result.s2_tool_correlation_overlap == 0.5
        assert result.use_confidence_gate == 0.42


class TestPresetSuccessfulUseThresholds:
    """T-2: the 5 named profile presets carry the expected per-profile scanner
    threshold overrides (or None to signal module defaults). Locks in the
    M-1 design table against accidental preset drift.
    """

    async def test_preset_thresholds_match_design(self, trace):
        from elephantbroker.schemas.profile import SuccessfulUseThresholds

        reg = ProfileRegistry(trace)

        # base/coding/worker: leave successful_use_thresholds unset — module
        # defaults (0.15/0.3/0.15/0.15/3) apply implicitly via the resolver.
        for name in ("coding", "worker"):
            p = await reg.resolve_profile(name)
            assert p.successful_use_thresholds is None, (
                f"{name} profile should leave thresholds None (implicit defaults); "
                f"got {p.successful_use_thresholds}"
            )

        # research: loose detection, default update-gate
        research = await reg.resolve_profile("research")
        assert research.successful_use_thresholds == SuccessfulUseThresholds(
            s1_direct_quote_ratio=0.10,
            s2_tool_correlation_overlap=0.25,
            s3_jaccard_score=0.10,
        )

        # managerial: default detection, tighter update-gate (0.25)
        managerial = await reg.resolve_profile("managerial")
        assert managerial.successful_use_thresholds == SuccessfulUseThresholds(
            use_confidence_gate=0.25,
        )

        # personal_assistant: mild loosening (S1/S3) + tight gate (0.20)
        pa = await reg.resolve_profile("personal_assistant")
        assert pa.successful_use_thresholds == SuccessfulUseThresholds(
            s1_direct_quote_ratio=0.12,
            s3_jaccard_score=0.12,
            use_confidence_gate=0.20,
        )
