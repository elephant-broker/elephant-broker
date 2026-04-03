"""Stage 3: Strengthen Useful Facts — boost confidence for high success ratio.

No LLM calls. Pure arithmetic. Returns decisions — caller handles upserts.
"""
from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from elephantbroker.runtime.observability import traced
from elephantbroker.schemas.consolidation import StrengthenResult

if TYPE_CHECKING:
    from elephantbroker.schemas.consolidation import ConsolidationConfig
    from elephantbroker.schemas.fact import FactAssertion

logger = logging.getLogger("elephantbroker.runtime.consolidation.stages.strengthen")


class StrengthenStage:
    """Boost confidence for facts with high successful_use_count / use_count ratio.

    Formula:
        success_ratio = successful_use_count / use_count
        if use_count >= min (3) AND ratio >= threshold (0.5):
            new_confidence = min(1.0, old + boost_factor * success_ratio)
            last_used_at = now  (reset decay timer)
    """

    def __init__(self, config: ConsolidationConfig) -> None:
        self._min_use = config.strengthen_min_use_count
        self._threshold = config.strengthen_success_ratio_threshold
        self._boost = config.strengthen_boost_factor

    @traced
    async def run(
        self, facts: list[FactAssertion], gateway_id: str,
    ) -> list[StrengthenResult]:
        results: list[StrengthenResult] = []
        now = datetime.now(UTC)

        for fact in facts:
            if fact.use_count < self._min_use:
                continue
            ratio = fact.successful_use_count / fact.use_count
            if ratio < self._threshold:
                results.append(StrengthenResult(
                    fact_id=str(fact.id),
                    old_confidence=fact.confidence,
                    new_confidence=fact.confidence,
                    success_ratio=ratio,
                    boosted=False,
                ))
                continue

            new_conf = min(1.0, fact.confidence + self._boost * ratio)
            fact.confidence = new_conf
            fact.last_used_at = now  # Reset decay timer

            results.append(StrengthenResult(
                fact_id=str(fact.id),
                old_confidence=fact.confidence - (self._boost * ratio if new_conf < 1.0 else 0),
                new_confidence=new_conf,
                success_ratio=ratio,
                boosted=True,
            ))

        boosted = sum(1 for r in results if r.boosted)
        logger.info("Stage 3: %d eligible, %d boosted (gateway=%s)", len(results), boosted, gateway_id)
        return results
