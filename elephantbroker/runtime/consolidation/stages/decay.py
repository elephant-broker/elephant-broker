"""Stage 4: Decay Unused Facts — apply confidence decay based on usage patterns.

No LLM calls. Returns decisions — caller handles upserts and archival.
"""
from __future__ import annotations

import logging
import math
from datetime import UTC, datetime
from typing import TYPE_CHECKING

from elephantbroker.runtime.observability import traced
from elephantbroker.schemas.consolidation import DecayResult

if TYPE_CHECKING:
    from elephantbroker.schemas.consolidation import ConsolidationConfig
    from elephantbroker.schemas.fact import FactAssertion
    from elephantbroker.schemas.profile import ProfilePolicy

logger = logging.getLogger("elephantbroker.runtime.consolidation.stages.decay")


class DecayStage:
    """Apply confidence decay based on usage patterns.

    Three decay categories:
    1. Recalled but unused (use_count > 0, successful_use_count == 0):
       confidence *= decay_recalled_unused_factor × scope_multiplier
    2. Never recalled (use_count == 0):
       confidence *= decay_never_recalled_factor × time_factor × scope_multiplier
    3. Actively used (successful_use_count > 0): NO DECAY (Stage 3 handles these)

    Archival: if new_confidence < threshold (0.05) → mark archived=True
    """

    def __init__(self, config: ConsolidationConfig) -> None:
        self._recalled_factor = config.decay_recalled_unused_factor
        self._never_factor = config.decay_never_recalled_factor
        self._archival_threshold = config.decay_archival_threshold
        self._scope_multipliers = config.decay_scope_multipliers

    @traced
    async def run(
        self,
        facts: list[FactAssertion],
        profile: ProfilePolicy,
        gateway_id: str,
    ) -> list[DecayResult]:
        results: list[DecayResult] = []
        now = datetime.now(UTC)
        half_life = getattr(profile.scoring_weights, "recency_half_life_hours", 69.0)

        for fact in facts:
            # Skip actively used facts — Stage 3 handles these
            if fact.successful_use_count > 0:
                continue

            old_conf = fact.confidence
            scope_key = fact.scope.value if hasattr(fact.scope, "value") else str(fact.scope)
            scope_mult = self._scope_multipliers.get(scope_key, 1.0)

            if fact.use_count > 0:
                # Recalled but never successfully used
                # scope_mult > 1 means faster decay: factor^mult (e.g. 0.85^1.5 ≈ 0.77)
                effective_factor = self._recalled_factor ** scope_mult
                new_conf = old_conf * effective_factor
                reason = "recalled_unused"
            else:
                # Never recalled — gentle time-based decay
                ref_time = fact.last_used_at or fact.updated_at or fact.created_at
                hours_since = max(0.0, (now - ref_time).total_seconds() / 3600)
                time_factor = math.exp(-math.log(2) / max(half_life, 0.01) * hours_since)
                effective_factor = self._never_factor ** scope_mult
                new_conf = old_conf * effective_factor * time_factor
                reason = "never_recalled"

            new_conf = max(0.0, min(1.0, new_conf))
            archived = new_conf < self._archival_threshold

            if archived:
                fact.archived = True
                fact.confidence = 0.0
            else:
                fact.confidence = new_conf

            results.append(DecayResult(
                fact_id=str(fact.id),
                old_confidence=old_conf,
                new_confidence=0.0 if archived else new_conf,
                decay_reason=reason,
                archived=archived,
            ))

        decayed = sum(1 for r in results if r.old_confidence != r.new_confidence)
        archived_count = sum(1 for r in results if r.archived)
        logger.info(
            "Stage 4: %d processed, %d decayed, %d archived (gateway=%s)",
            len(results), decayed, archived_count, gateway_id,
        )
        return results
