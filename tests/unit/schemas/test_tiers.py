"""Tests for tier capability matrix."""
import pytest

from elephantbroker.schemas.tiers import TIER_CAPABILITIES, BusinessTier, TierCapabilityMatrix


class TestBusinessTier:
    def test_all_tiers(self):
        assert len(BusinessTier) == 3


class TestTierCapabilities:
    def test_memory_only_has_memory_but_no_compaction(self):
        matrix = TierCapabilityMatrix.for_tier(BusinessTier.MEMORY_ONLY)
        assert matrix.is_enabled("IMemoryStoreFacade")
        assert not matrix.is_enabled("ICompactionEngine")

    def test_full_tier_enables_all_modules(self):
        matrix = TierCapabilityMatrix.for_tier(BusinessTier.FULL)
        # TODO-8-R1-017: assert known modules are present, not the count.
        # The previous `len == 18` brittle check has been removed — adding
        # a new module (e.g. C2.2 added IContextLifecycle, taking 17 → 18)
        # produced a confusing arithmetic failure instead of pointing at
        # the actual semantic regression. The set-equality test
        # `test_full_tier_is_union_plus_consolidation` below pins the
        # exact membership, which is the contract that matters.
        assert matrix.is_enabled("IMemoryStoreFacade")
        assert matrix.is_enabled("IContextLifecycle")
        assert matrix.is_enabled("IConsolidationEngine")

    def test_context_only_has_working_set(self):
        matrix = TierCapabilityMatrix.for_tier(BusinessTier.CONTEXT_ONLY)
        assert matrix.is_enabled("IWorkingSetManager")
        assert not matrix.is_enabled("IMemoryStoreFacade")

    def test_is_enabled_returns_false_for_unknown(self):
        matrix = TierCapabilityMatrix.for_tier(BusinessTier.MEMORY_ONLY)
        assert not matrix.is_enabled("IDoesNotExist")

    def test_json_round_trip(self):
        matrix = TierCapabilityMatrix.for_tier(BusinessTier.FULL)
        data = matrix.model_dump(mode="json")
        restored = TierCapabilityMatrix.model_validate(data)
        assert restored.tier == BusinessTier.FULL

    def test_memory_only_has_exactly_10_modules(self):
        """MEMORY_ONLY tier has the enumerated set of modules.

        TODO-8-R1-017: redundant ``len == 10`` removed — set-equality below
        is the actual contract; the count assertion produced a confusing
        arithmetic failure on module addition without pointing at which
        module went missing.
        """
        m = TierCapabilityMatrix.for_tier(BusinessTier.MEMORY_ONLY)
        assert m.enabled_modules == {
            "IActorRegistry",
            "IGoalManager",
            "IMemoryStoreFacade",
            "IRetrievalOrchestrator",
            "IRerankOrchestrator",
            "IToolArtifactStore",
            "ITraceLedger",
            "IProfileRegistry",
            "IEvidenceAndVerificationEngine",
            "IProcedureEngine",
        }

    def test_context_only_has_exactly_11_modules(self):
        """CONTEXT_ONLY tier has the enumerated set of modules.

        TODO-8-R1-017: redundant ``len == 11`` removed — set-equality below
        is the actual contract. C2.2 added IContextLifecycle; the count
        assertion would fail on module addition without pointing at the
        affected interface.
        """
        m = TierCapabilityMatrix.for_tier(BusinessTier.CONTEXT_ONLY)
        assert m.enabled_modules == {
            "IActorRegistry",
            "IGoalManager",
            "IWorkingSetManager",
            "IContextAssembler",
            "ICompactionEngine",
            "IContextLifecycle",
            "IRedLineGuardEngine",
            "IStatsAndTelemetryEngine",
            "ITraceLedger",
            "IProfileRegistry",
            "IScoringTuner",
        }

    def test_for_tier_unknown_raises_keyerror(self):
        """for_tier() with a non-BusinessTier key raises KeyError at TIER_CAPABILITIES lookup."""
        with pytest.raises(KeyError):
            TierCapabilityMatrix.for_tier("UNKNOWN_TIER")

    def test_full_tier_is_union_plus_consolidation(self):
        """Invariant: FULL == MEMORY_ONLY ∪ CONTEXT_ONLY ∪ {IConsolidationEngine}."""
        assert TIER_CAPABILITIES[BusinessTier.FULL] == (
            TIER_CAPABILITIES[BusinessTier.MEMORY_ONLY]
            | TIER_CAPABILITIES[BusinessTier.CONTEXT_ONLY]
            | {"IConsolidationEngine"}
        )

    def test_consolidation_engine_exclusive_to_full(self):
        """IConsolidationEngine is only enabled in FULL tier."""
        assert "IConsolidationEngine" not in TIER_CAPABILITIES[BusinessTier.MEMORY_ONLY]
        assert "IConsolidationEngine" not in TIER_CAPABILITIES[BusinessTier.CONTEXT_ONLY]
        assert "IConsolidationEngine" in TIER_CAPABILITIES[BusinessTier.FULL]

    def test_shared_modules_between_memory_and_context_only(self):
        """The 4 modules shared between MEMORY_ONLY and CONTEXT_ONLY: actor, goal, trace, profile."""
        shared = (
            TIER_CAPABILITIES[BusinessTier.MEMORY_ONLY]
            & TIER_CAPABILITIES[BusinessTier.CONTEXT_ONLY]
        )
        assert shared == {"IActorRegistry", "IGoalManager", "ITraceLedger", "IProfileRegistry"}

    def test_memory_only_unique_modules(self):
        """The 6 modules unique to MEMORY_ONLY: memory/retrieval/rerank/artifact/evidence/procedure."""
        unique = (
            TIER_CAPABILITIES[BusinessTier.MEMORY_ONLY]
            - TIER_CAPABILITIES[BusinessTier.CONTEXT_ONLY]
        )
        assert unique == {
            "IMemoryStoreFacade",
            "IRetrievalOrchestrator",
            "IRerankOrchestrator",
            "IToolArtifactStore",
            "IEvidenceAndVerificationEngine",
            "IProcedureEngine",
        }

    def test_context_only_unique_modules(self):
        """The 7 modules unique to CONTEXT_ONLY: working-set/assembler/compaction/lifecycle/guard/stats/tuner.

        C2.2: IContextLifecycle was added to CONTEXT_ONLY (and FULL) — set count 6 → 7.
        """
        unique = (
            TIER_CAPABILITIES[BusinessTier.CONTEXT_ONLY]
            - TIER_CAPABILITIES[BusinessTier.MEMORY_ONLY]
        )
        assert unique == {
            "IWorkingSetManager",
            "IContextAssembler",
            "ICompactionEngine",
            "IContextLifecycle",
            "IRedLineGuardEngine",
            "IStatsAndTelemetryEngine",
            "IScoringTuner",
        }
