"""BudgetSelector — greedy selection with dynamic penalties (Pass 2)."""
from __future__ import annotations

import logging

from elephantbroker.runtime.working_set.scoring import ScoringEngine
from elephantbroker.schemas.working_set import (
    ScoringContext,
    ScoringWeights,
    WorkingSetItem,
    WorkingSetSnapshot,
    WorkingSetScores,
)

logger = logging.getLogger("elephantbroker.runtime.working_set.selector")


class BudgetSelector:
    """Stateless budget selector. Greedy knapsack with dynamic penalties."""

    def select(
        self, scored_items: list[WorkingSetItem], ctx: ScoringContext,
        token_budget: int, session_id, scoring_engine: ScoringEngine,
    ) -> WorkingSetSnapshot:
        """Select items within budget using greedy selection with dynamic penalties."""
        import uuid as _uuid

        # Separate must_inject items
        must_inject = [it for it in scored_items if it.must_inject]
        candidates = [it for it in scored_items if not it.must_inject]

        selected: list[WorkingSetItem] = []
        remaining_budget = token_budget

        # 1. Pre-allocate must_inject
        for item in must_inject:
            if item.token_size > remaining_budget:
                logger.warning(
                    "Must-inject item %s (%d tokens) exceeds remaining budget %d — including anyway",
                    item.id, item.token_size, remaining_budget,
                )
            selected.append(item)
            remaining_budget -= item.token_size

        # 2. Sort candidates by partial_final desc
        candidates.sort(key=lambda it: it.scores.final, reverse=True)

        # 3. Greedy selection with dynamic penalties
        for item in candidates:
            if remaining_budget <= 0:
                break
            if item.token_size > remaining_budget:
                continue

            # Recompute dynamic penalties
            redundancy = scoring_engine.compute_redundancy_penalty(item, selected, ctx)
            contradiction = scoring_engine.compute_contradiction_penalty(item, selected, ctx)
            cost = scoring_engine.compute_cost_penalty(item, remaining_budget)

            # Update scores
            updated = item.scores.model_copy()
            updated.redundancy_penalty = redundancy
            updated.contradiction_penalty = contradiction
            updated.cost_penalty = cost
            updated.final = ctx.weights.weighted_sum(updated)

            if updated.final > 0:
                item.scores = updated
                selected.append(item)
                remaining_budget -= item.token_size

        # Diversity warning
        if len(selected) > 1:
            source_types = {it.source_type for it in selected}
            if len(source_types) == 1:
                logger.warning(
                    "All %d selected items are from source_type=%s — low diversity",
                    len(selected), next(iter(source_types)),
                )

        tokens_used = sum(it.token_size for it in selected)

        return WorkingSetSnapshot(
            session_id=session_id if isinstance(session_id, _uuid.UUID) else _uuid.UUID(str(session_id)),
            items=selected,
            token_budget=token_budget,
            tokens_used=tokens_used,
            weights_used=ctx.weights,
        )
