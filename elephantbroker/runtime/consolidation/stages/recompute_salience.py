"""Stage 9: Recompute Salience Priors — correlate scoring dimensions with successful use.

No LLM calls. Pure statistics. Outputs TuningDelta list for ScoringTuner.
"""
from __future__ import annotations

import logging
import math
from typing import TYPE_CHECKING

from elephantbroker.runtime.observability import traced
from elephantbroker.schemas.scoring import ScoringDimension, TuningDelta

if TYPE_CHECKING:
    from elephantbroker.schemas.consolidation import ConsolidationConfig
    from elephantbroker.schemas.working_set import ScoringWeights

logger = logging.getLogger("elephantbroker.runtime.consolidation.stages.recompute_salience")

# All 11 scoring dimensions
_DIMENSIONS = [d.value for d in ScoringDimension]


class RecomputeSalienceStage:
    """Correlate scoring dimensions with successful-use outcome.

    Uses Spearman rank correlation per dimension. EMA-smoothed deltas capped
    at ±max_weight_adjustment_pct of BASE weight (prevents convergence trap).

    Input: scoring_ledger entries with dim_scores + was_successful flag.
    Output: list[TuningDelta] for dimensions with significant correlation.
    """

    def __init__(self, config: ConsolidationConfig) -> None:
        self._ema_alpha = config.ema_alpha
        self._max_pct = config.max_weight_adjustment_pct
        self._min_samples = config.min_correlation_samples

    @traced
    async def run(
        self,
        scoring_ledger: list[dict],
        current_use_counts: dict[str, int],
        profile_weights: ScoringWeights,
        previous_deltas: dict[str, float] | None = None,
    ) -> list[TuningDelta]:
        if len(scoring_ledger) < self._min_samples:
            logger.info(
                "Stage 9: insufficient samples (%d < %d) — skipping",
                len(scoring_ledger), self._min_samples,
            )
            return []

        prev = previous_deltas or {}

        # Compute was_successful per entry
        for entry in scoring_ledger:
            fid = entry.get("fact_id", "")
            at_scoring = entry.get("successful_use_count_at_scoring", 0)
            current = current_use_counts.get(fid, 0)
            entry["was_successful"] = current > at_scoring

        deltas: list[TuningDelta] = []

        for dim in _DIMENSIONS:
            scores = []
            outcomes = []
            for entry in scoring_ledger:
                dim_scores = entry.get("dim_scores", {})
                score = dim_scores.get(dim, 0.0)
                scores.append(score)
                outcomes.append(1.0 if entry["was_successful"] else 0.0)

            correlation = _spearman_correlation(scores, outcomes)
            if abs(correlation) <= 0.1:
                continue  # Below significance threshold

            base_weight = getattr(profile_weights, dim, 1.0)
            raw_delta = correlation * self._max_pct * base_weight
            prev_accumulated = prev.get(dim, 0.0)
            smoothed = self._ema_alpha * raw_delta + (1 - self._ema_alpha) * prev_accumulated

            # Cap at ±max_pct of BASE weight (AD-9: caps reference base, not current)
            max_abs = self._max_pct * abs(base_weight)
            capped = max(-max_abs, min(max_abs, smoothed))

            deltas.append(TuningDelta(
                dimension=ScoringDimension(dim),
                delta=capped,
                reason=f"spearman_corr={correlation:.3f}",
            ))

        logger.info(
            "Stage 9: %d entries, %d dimension adjustments",
            len(scoring_ledger), len(deltas),
        )
        return deltas


def _spearman_correlation(x: list[float], y: list[float]) -> float:
    """Spearman rank correlation. Falls back to manual computation if scipy unavailable."""
    if len(x) != len(y) or len(x) < 3:
        return 0.0

    try:
        from scipy.stats import spearmanr
        corr, _ = spearmanr(x, y)
        return corr if not math.isnan(corr) else 0.0
    except ImportError:
        pass

    # Manual rank computation
    def _rank(values: list[float]) -> list[float]:
        sorted_indices = sorted(range(len(values)), key=lambda i: values[i])
        ranks = [0.0] * len(values)
        i = 0
        while i < len(sorted_indices):
            j = i
            while j < len(sorted_indices) and values[sorted_indices[j]] == values[sorted_indices[i]]:
                j += 1
            avg_rank = (i + j - 1) / 2.0 + 1
            for k in range(i, j):
                ranks[sorted_indices[k]] = avg_rank
            i = j
        return ranks

    rx = _rank(x)
    ry = _rank(y)
    n = len(x)

    d_sq_sum = sum((rx[i] - ry[i]) ** 2 for i in range(n))
    denom = n * (n * n - 1)
    if denom == 0:
        return 0.0
    return 1 - (6 * d_sq_sum) / denom
