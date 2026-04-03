"""Tests for Stage 5: Prune Bad Autorecall."""
from __future__ import annotations

from elephantbroker.runtime.consolidation.stages.prune_autorecall import PruneAutorecallStage
from elephantbroker.schemas.consolidation import ConsolidationConfig
from tests.fixtures.factories import make_fact_assertion


def _make_stage(min_recalls: int = 5, max_ratio: float = 0.0):
    config = ConsolidationConfig(
        autorecall_blacklist_min_recalls=min_recalls,
        autorecall_blacklist_max_success_ratio=max_ratio,
    )
    return PruneAutorecallStage(config)


class TestPruneAutorecall:
    async def test_blacklists_high_recall_zero_use(self):
        stage = _make_stage(min_recalls=5)
        fact = make_fact_assertion(use_count=10, successful_use_count=0)
        result = await stage.run([fact], "gw-1")
        assert len(result) == 1
        assert str(fact.id) in result
        assert fact.autorecall_blacklisted is True

    async def test_keeps_items_with_some_use(self):
        stage = _make_stage(min_recalls=5, max_ratio=0.0)
        fact = make_fact_assertion(use_count=10, successful_use_count=1)
        result = await stage.run([fact], "gw-1")
        assert len(result) == 0
        assert fact.autorecall_blacklisted is False

    async def test_keeps_items_below_recall_threshold(self):
        stage = _make_stage(min_recalls=5)
        fact = make_fact_assertion(use_count=3, successful_use_count=0)
        result = await stage.run([fact], "gw-1")
        assert len(result) == 0

    async def test_already_blacklisted_skipped(self):
        stage = _make_stage()
        fact = make_fact_assertion(use_count=10, successful_use_count=0)
        fact.autorecall_blacklisted = True
        result = await stage.run([fact], "gw-1")
        assert len(result) == 0  # Already blacklisted, not re-added

    async def test_configurable_max_ratio(self):
        stage = _make_stage(min_recalls=3, max_ratio=0.2)
        # ratio = 1/5 = 0.2 → <= 0.2 → blacklisted
        fact = make_fact_assertion(use_count=5, successful_use_count=1)
        result = await stage.run([fact], "gw-1")
        assert len(result) == 1
