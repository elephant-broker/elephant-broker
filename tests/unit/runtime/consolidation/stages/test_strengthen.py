"""Tests for Stage 3: Strengthen Useful Facts."""
from __future__ import annotations

from elephantbroker.runtime.consolidation.stages.strengthen import StrengthenStage
from elephantbroker.schemas.consolidation import ConsolidationConfig
from tests.fixtures.factories import make_fact_assertion


def _make_stage(boost: float = 0.1, min_use: int = 3, threshold: float = 0.5):
    config = ConsolidationConfig(
        strengthen_boost_factor=boost,
        strengthen_min_use_count=min_use,
        strengthen_success_ratio_threshold=threshold,
    )
    return StrengthenStage(config)


class TestStrengthen:
    async def test_high_success_ratio_gets_boosted(self):
        stage = _make_stage()
        facts = [make_fact_assertion(use_count=10, successful_use_count=8, confidence=0.5)]
        results = await stage.run(facts, "gw-1")
        assert len(results) == 1
        assert results[0].boosted is True
        assert results[0].new_confidence > 0.5

    async def test_low_success_ratio_not_boosted(self):
        stage = _make_stage(threshold=0.5)
        facts = [make_fact_assertion(use_count=10, successful_use_count=2, confidence=0.5)]
        results = await stage.run(facts, "gw-1")
        assert len(results) == 1
        assert results[0].boosted is False

    async def test_confidence_capped_at_1(self):
        stage = _make_stage(boost=0.5)
        facts = [make_fact_assertion(use_count=5, successful_use_count=5, confidence=0.9)]
        results = await stage.run(facts, "gw-1")
        assert results[0].new_confidence <= 1.0

    async def test_min_use_count_gate(self):
        stage = _make_stage(min_use=5)
        facts = [make_fact_assertion(use_count=3, successful_use_count=3, confidence=0.5)]
        results = await stage.run(facts, "gw-1")
        assert len(results) == 0  # Below min_use threshold

    async def test_configurable_boost_factor(self):
        stage = _make_stage(boost=0.2)
        facts = [make_fact_assertion(use_count=5, successful_use_count=5, confidence=0.5)]
        results = await stage.run(facts, "gw-1")
        # ratio=1.0, boost=0.2 → new = 0.5 + 0.2*1.0 = 0.7
        assert results[0].boosted is True
        assert results[0].new_confidence >= 0.7

    async def test_decay_timer_reset_on_boost(self):
        stage = _make_stage()
        fact = make_fact_assertion(use_count=5, successful_use_count=4, confidence=0.5)
        old_last_used = fact.last_used_at
        await stage.run([fact], "gw-1")
        assert fact.last_used_at is not None
        assert fact.last_used_at != old_last_used

    async def test_no_facts_returns_empty(self):
        stage = _make_stage()
        results = await stage.run([], "gw-1")
        assert results == []
