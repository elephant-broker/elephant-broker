"""Tests for tier capability matrix."""

from elephantbroker.schemas.tiers import BusinessTier, TierCapabilityMatrix


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
