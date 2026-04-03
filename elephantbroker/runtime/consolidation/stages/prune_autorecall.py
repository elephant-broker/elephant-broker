"""Stage 5: Prune Bad Autorecall — blacklist facts recalled many times but never used.

No LLM calls. Sets autorecall_blacklisted=True on flagged facts.
Facts remain searchable via explicit memory_search — only auto-injection is blocked.
"""
from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from elephantbroker.runtime.observability import traced

if TYPE_CHECKING:
    from elephantbroker.schemas.consolidation import ConsolidationConfig
    from elephantbroker.schemas.fact import FactAssertion

logger = logging.getLogger("elephantbroker.runtime.consolidation.stages.prune_autorecall")


class PruneAutorecallStage:
    """Blacklist items recalled many times but never successfully used.

    Criteria:
        use_count >= min_recalls (5)
        AND successful_use_count / use_count <= max_success_ratio (0.0)
    """

    def __init__(self, config: ConsolidationConfig) -> None:
        self._min_recalls = config.autorecall_blacklist_min_recalls
        self._max_ratio = config.autorecall_blacklist_max_success_ratio

    @traced
    async def run(
        self, facts: list[FactAssertion], gateway_id: str,
    ) -> list[str]:
        """Returns list of blacklisted fact IDs."""
        blacklisted: list[str] = []

        for fact in facts:
            if fact.autorecall_blacklisted:
                continue  # Already blacklisted
            if fact.use_count < self._min_recalls:
                continue
            ratio = fact.successful_use_count / fact.use_count if fact.use_count > 0 else 0.0
            if ratio <= self._max_ratio:
                fact.autorecall_blacklisted = True
                blacklisted.append(str(fact.id))

        logger.info(
            "Stage 5: %d facts checked, %d blacklisted (gateway=%s)",
            len(facts), len(blacklisted), gateway_id,
        )
        return blacklisted
