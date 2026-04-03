"""Tests for Stage 9: Recompute Salience Priors."""
from __future__ import annotations

import pytest

from elephantbroker.runtime.consolidation.stages.recompute_salience import (
    RecomputeSalienceStage,
    _spearman_correlation,
)
from elephantbroker.schemas.consolidation import ConsolidationConfig
from elephantbroker.schemas.scoring import ScoringDimension
from elephantbroker.schemas.working_set import ScoringWeights


def _make_stage(ema: float = 0.3, max_pct: float = 0.05, min_samples: int = 5):
    config = ConsolidationConfig(
        ema_alpha=ema,
        max_weight_adjustment_pct=max_pct,
        min_correlation_samples=min_samples,
    )
    return RecomputeSalienceStage(config)


def _make_ledger(n: int = 20, success_rate: float = 0.5):
    """Create scoring ledger entries with controllable success pattern."""
    entries = []
    for i in range(n):
        was_successful = i < int(n * success_rate)
        entries.append({
            "fact_id": f"fact-{i}",
            "dim_scores": {
                "turn_relevance": 0.9 if was_successful else 0.3,
                "recency": 0.5,
                "confidence": 0.7,
            },
            "successful_use_count_at_scoring": 0,
        })
    return entries


class TestRecomputeSalience:
    async def test_positive_correlation_increases_weight(self):
        stage = _make_stage(min_samples=5)
        ledger = _make_ledger(20, success_rate=0.5)
        # turn_relevance is high for successful facts → positive correlation
        use_counts = {f"fact-{i}": 1 if i < 10 else 0 for i in range(20)}
        weights = ScoringWeights(turn_relevance=1.5)
        deltas = await stage.run(ledger, use_counts, weights)
        tr_deltas = [d for d in deltas if d.dimension == ScoringDimension.TURN_RELEVANCE]
        assert len(tr_deltas) == 1
        assert tr_deltas[0].delta > 0  # Positive correlation → positive delta

    async def test_negative_correlation_decreases_weight(self):
        stage = _make_stage(min_samples=5)
        entries = []
        for i in range(20):
            was_successful = i < 10
            entries.append({
                "fact_id": f"fact-{i}",
                "dim_scores": {
                    "turn_relevance": 0.2 if was_successful else 0.9,  # Inverted!
                },
                "successful_use_count_at_scoring": 0,
            })
        use_counts = {f"fact-{i}": 1 if i < 10 else 0 for i in range(20)}
        weights = ScoringWeights(turn_relevance=1.5)
        deltas = await stage.run(entries, use_counts, weights)
        tr_deltas = [d for d in deltas if d.dimension == ScoringDimension.TURN_RELEVANCE]
        if tr_deltas:
            assert tr_deltas[0].delta < 0  # Negative correlation

    async def test_adjustment_capped_at_max_pct(self):
        stage = _make_stage(max_pct=0.05, ema=1.0, min_samples=5)
        ledger = _make_ledger(30, success_rate=0.5)
        use_counts = {f"fact-{i}": 1 if i < 15 else 0 for i in range(30)}
        weights = ScoringWeights(turn_relevance=1.5)
        deltas = await stage.run(ledger, use_counts, weights)
        for d in deltas:
            base = getattr(weights, d.dimension.value, 1.0)
            assert abs(d.delta) <= 0.05 * abs(base) + 0.001

    async def test_insufficient_samples_returns_empty(self):
        stage = _make_stage(min_samples=50)
        ledger = _make_ledger(10)
        deltas = await stage.run(ledger, {}, ScoringWeights())
        assert deltas == []

    async def test_ema_smoothing_applied(self):
        stage = _make_stage(ema=0.3, min_samples=5)
        ledger = _make_ledger(20, success_rate=0.5)
        use_counts = {f"fact-{i}": 1 if i < 10 else 0 for i in range(20)}
        weights = ScoringWeights(turn_relevance=1.5)
        # With previous delta of 0.02
        deltas = await stage.run(ledger, use_counts, weights, previous_deltas={"turn_relevance": 0.02})
        tr_deltas = [d for d in deltas if d.dimension == ScoringDimension.TURN_RELEVANCE]
        if tr_deltas:
            # EMA: 0.3 * raw + 0.7 * 0.02 — result should include the smoothing
            assert tr_deltas[0].delta != 0.0

    async def test_output_is_list_of_tuning_deltas(self):
        stage = _make_stage(min_samples=5)
        ledger = _make_ledger(20, success_rate=0.5)
        use_counts = {f"fact-{i}": 1 if i < 10 else 0 for i in range(20)}
        deltas = await stage.run(ledger, use_counts, ScoringWeights())
        for d in deltas:
            assert hasattr(d, "dimension")
            assert hasattr(d, "delta")
            assert hasattr(d, "reason")


class TestSpearmanCorrelation:
    def test_perfect_positive(self):
        corr = _spearman_correlation([1, 2, 3, 4, 5], [1, 2, 3, 4, 5])
        assert corr == pytest.approx(1.0)

    def test_perfect_negative(self):
        corr = _spearman_correlation([1, 2, 3, 4, 5], [5, 4, 3, 2, 1])
        assert corr == pytest.approx(-1.0)

    def test_no_correlation(self):
        # Roughly uncorrelated
        corr = _spearman_correlation([1, 2, 3, 4, 5], [3, 5, 1, 4, 2])
        assert abs(corr) < 0.6  # Weak or no correlation

    def test_too_short(self):
        assert _spearman_correlation([1, 2], [3, 4]) == 0.0
