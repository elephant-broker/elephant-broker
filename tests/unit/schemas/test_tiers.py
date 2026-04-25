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
        assert len(matrix.enabled_modules) == 17

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
        """MEMORY_ONLY tier has exactly 10 modules, enumerated by interface name."""
        m = TierCapabilityMatrix.for_tier(BusinessTier.MEMORY_ONLY)
        assert len(m.enabled_modules) == 10
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

    def test_context_only_has_exactly_10_modules(self):
        """CONTEXT_ONLY tier has exactly 10 modules, enumerated by interface name."""
        m = TierCapabilityMatrix.for_tier(BusinessTier.CONTEXT_ONLY)
        assert len(m.enabled_modules) == 10
        assert m.enabled_modules == {
            "IActorRegistry",
            "IGoalManager",
            "IWorkingSetManager",
            "IContextAssembler",
            "ICompactionEngine",
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
        """The 6 modules unique to CONTEXT_ONLY: working-set/assembler/compaction/guard/stats/tuner."""
        unique = (
            TIER_CAPABILITIES[BusinessTier.CONTEXT_ONLY]
            - TIER_CAPABILITIES[BusinessTier.MEMORY_ONLY]
        )
        assert unique == {
            "IWorkingSetManager",
            "IContextAssembler",
            "ICompactionEngine",
            "IRedLineGuardEngine",
            "IStatsAndTelemetryEngine",
            "IScoringTuner",
        }
