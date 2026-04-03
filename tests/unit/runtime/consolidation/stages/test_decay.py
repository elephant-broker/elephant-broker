"""Tests for Stage 4: Decay Unused Facts."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

from elephantbroker.runtime.consolidation.stages.decay import DecayStage
from elephantbroker.schemas.consolidation import ConsolidationConfig
from elephantbroker.schemas.working_set import ScoringWeights
from tests.fixtures.factories import make_fact_assertion


def _make_stage(**overrides):
    defaults = {
        "decay_recalled_unused_factor": 0.85,
        "decay_never_recalled_factor": 0.95,
        "decay_archival_threshold": 0.05,
    }
    defaults.update(overrides)
    config = ConsolidationConfig(**defaults)
    return DecayStage(config)


def _make_profile(half_life: float = 69.0):
    profile = MagicMock()
    profile.scoring_weights = ScoringWeights(recency_half_life_hours=half_life)
    return profile


class TestDecay:
    async def test_recalled_but_unused_facts_decay(self):
        stage = _make_stage()
        fact = make_fact_assertion(use_count=5, successful_use_count=0, confidence=0.8, scope="session")
        results = await stage.run([fact], _make_profile(), "gw-1")
        assert len(results) == 1
        assert results[0].decay_reason == "recalled_unused"
        assert results[0].new_confidence < 0.8

    async def test_never_recalled_gentle_time_decay(self):
        stage = _make_stage()
        fact = make_fact_assertion(
            use_count=0, successful_use_count=0, confidence=0.8,
            last_used_at=datetime.now(UTC) - timedelta(hours=100), scope="actor",
        )
        results = await stage.run([fact], _make_profile(), "gw-1")
        assert len(results) == 1
        assert results[0].decay_reason == "never_recalled"
        assert results[0].new_confidence < 0.8

    async def test_session_scoped_decays_faster(self):
        stage = _make_stage()
        f_session = make_fact_assertion(use_count=3, successful_use_count=0, confidence=0.8, scope="session")
        f_actor = make_fact_assertion(use_count=3, successful_use_count=0, confidence=0.8, scope="actor")
        r_session = (await stage.run([f_session], _make_profile(), "gw-1"))[0]
        r_actor = (await stage.run([f_actor], _make_profile(), "gw-1"))[0]
        # session scope multiplier=1.5 vs actor=1.0 → session decays more
        assert r_session.new_confidence < r_actor.new_confidence

    async def test_org_scoped_decays_slower(self):
        stage = _make_stage()
        f_org = make_fact_assertion(use_count=3, successful_use_count=0, confidence=0.8, scope="organization")
        f_actor = make_fact_assertion(use_count=3, successful_use_count=0, confidence=0.8, scope="actor")
        r_org = (await stage.run([f_org], _make_profile(), "gw-1"))[0]
        r_actor = (await stage.run([f_actor], _make_profile(), "gw-1"))[0]
        assert r_org.new_confidence > r_actor.new_confidence

    async def test_global_scoped_decays_slowest(self):
        stage = _make_stage()
        f_global = make_fact_assertion(use_count=3, successful_use_count=0, confidence=0.8, scope="global")
        f_session = make_fact_assertion(use_count=3, successful_use_count=0, confidence=0.8, scope="session")
        r_global = (await stage.run([f_global], _make_profile(), "gw-1"))[0]
        r_session = (await stage.run([f_session], _make_profile(), "gw-1"))[0]
        assert r_global.new_confidence > r_session.new_confidence

    async def test_active_facts_not_decayed(self):
        stage = _make_stage()
        fact = make_fact_assertion(use_count=5, successful_use_count=3, confidence=0.8)
        results = await stage.run([fact], _make_profile(), "gw-1")
        assert len(results) == 0  # Active facts skipped

    async def test_below_threshold_marked_for_archival(self):
        stage = _make_stage(decay_archival_threshold=0.5)
        fact = make_fact_assertion(use_count=3, successful_use_count=0, confidence=0.1, scope="session")
        results = await stage.run([fact], _make_profile(), "gw-1")
        assert results[0].archived is True

    async def test_archival_threshold_configurable(self):
        stage = _make_stage(decay_archival_threshold=0.01)
        fact = make_fact_assertion(use_count=3, successful_use_count=0, confidence=0.1, scope="actor")
        results = await stage.run([fact], _make_profile(), "gw-1")
        # 0.1 * 0.85 * 1.0 = 0.085 > 0.01 threshold → NOT archived
        assert results[0].archived is False

    async def test_uses_last_used_at_not_created_at(self):
        stage = _make_stage()
        # last_used_at is recent → less time decay
        fact_recent = make_fact_assertion(
            use_count=0, successful_use_count=0, confidence=0.8,
            last_used_at=datetime.now(UTC) - timedelta(hours=1), scope="actor",
        )
        fact_old = make_fact_assertion(
            use_count=0, successful_use_count=0, confidence=0.8,
            last_used_at=datetime.now(UTC) - timedelta(hours=500), scope="actor",
        )
        r_recent = (await stage.run([fact_recent], _make_profile(), "gw-1"))[0]
        r_old = (await stage.run([fact_old], _make_profile(), "gw-1"))[0]
        assert r_recent.new_confidence > r_old.new_confidence

    async def test_empty_facts_returns_empty(self):
        stage = _make_stage()
        results = await stage.run([], _make_profile(), "gw-1")
        assert results == []
