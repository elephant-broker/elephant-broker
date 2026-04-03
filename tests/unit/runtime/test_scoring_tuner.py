"""Tests for ScoringTuner — updated for Phase 9 interface."""
from elephantbroker.runtime.profiles.registry import ProfileRegistry
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.runtime.working_set.scoring_tuner import ScoringTuner
from elephantbroker.schemas.scoring import ScoringDimension, TuningDelta


class TestScoringTuner:
    def _make(self):
        ledger = TraceLedger()
        reg = ProfileRegistry(ledger)
        return ScoringTuner(ledger, reg)

    async def test_get_weights(self):
        tuner = self._make()
        w = await tuner.get_weights("coding")
        assert w.turn_relevance == 1.5

    async def test_get_weights_with_org(self):
        tuner = self._make()
        w = await tuner.get_weights("coding", org_id="test_org")
        assert w.turn_relevance == 1.5

    async def test_apply_feedback(self):
        tuner = self._make()
        delta = TuningDelta(dimension=ScoringDimension.RECENCY, delta=0.1)
        result = await tuner.apply_feedback("coding", [delta])
        # Stub: returns None (new interface)
        assert result is None

    async def test_run_tuning_cycle(self):
        tuner = self._make()
        result = await tuner.run_tuning_cycle("test_org", "test_gw")
        # Returns dict of profile_name -> list[TuningDelta] (empty lists without scoring ledger)
        assert isinstance(result, dict)
        for v in result.values():
            assert isinstance(v, list)

    async def test_backward_compat_profile_only(self):
        tuner = self._make()
        w = await tuner.get_weights("research")
        assert w.evidence_strength == 0.9
