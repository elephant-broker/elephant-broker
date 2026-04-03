"""ScoringEngine — 11-dimension scoring system for working set competition."""
from __future__ import annotations

import logging
import math
from datetime import UTC, datetime

from elephantbroker.schemas.working_set import ScoringContext, WorkingSetItem, WorkingSetScores

logger = logging.getLogger("elephantbroker.runtime.working_set.scoring")


class ScoringEngine:
    """Stateless scoring engine. All state comes via ScoringContext."""

    def score_independent(self, item: WorkingSetItem, ctx: ScoringContext) -> WorkingSetScores:
        """Compute 9 independent dimensions. Redundancy and contradiction left at 0."""
        scores = WorkingSetScores(
            turn_relevance=self.compute_turn_relevance(item, ctx),
            session_goal_relevance=self.compute_session_goal_relevance(item, ctx),
            global_goal_relevance=self.compute_global_goal_relevance(item, ctx),
            recency=self.compute_recency(item, ctx),
            successful_use_prior=self.compute_successful_use_prior(item, ctx),
            confidence=self.compute_confidence(item, ctx),
            evidence_strength=self.compute_evidence_strength(item, ctx),
            novelty=self.compute_novelty(item, ctx),
            cost_penalty=self.compute_cost_penalty(item, ctx.token_budget),
        )
        scores.final = ctx.weights.weighted_sum(scores)
        return scores

    def compute_turn_relevance(self, item: WorkingSetItem, ctx: ScoringContext) -> float:
        item_emb = ctx.item_embeddings.get(item.id)
        if not item_emb or not ctx.turn_embedding:
            return 0.0
        return max(0.0, min(1.0, self._cosine_similarity(item_emb, ctx.turn_embedding)))

    def compute_session_goal_relevance(self, item: WorkingSetItem, ctx: ScoringContext) -> float:
        # Check goal_relevance_tags first
        if item.goal_relevance_tags:
            best = 0.0
            for _, strength in item.goal_relevance_tags.items():
                if strength == "direct":
                    best = max(best, 1.0)
                elif strength == "indirect":
                    best = max(best, 0.7)
            if best > 0:
                return best

        if not ctx.session_goals or not ctx.goal_embeddings:
            return 0.0
        item_emb = ctx.item_embeddings.get(item.id)
        if not item_emb:
            return 0.0
        best = 0.0
        for goal in ctx.session_goals:
            gid = str(getattr(goal, "id", ""))
            goal_emb = ctx.goal_embeddings.get(gid)
            if goal_emb:
                sim = self._cosine_similarity(item_emb, goal_emb)
                best = max(best, sim)
        # Parent chain walk-up: if item matches a sub-goal, parent gets 0.7x credit
        for goal in ctx.session_goals:
            if hasattr(goal, 'parent_goal_id') and goal.parent_goal_id:
                gid = str(getattr(goal, "id", ""))
                goal_emb = ctx.goal_embeddings.get(gid)
                if goal_emb and item_emb:
                    child_sim = self._cosine_similarity(item_emb, goal_emb)
                    # Find parent and give it 0.7x child relevance
                    parent_id = str(goal.parent_goal_id)
                    parent_bonus = child_sim * 0.7
                    best = max(best, parent_bonus)
        return max(0.0, min(1.0, best))

    def compute_global_goal_relevance(self, item: WorkingSetItem, ctx: ScoringContext) -> float:
        if not ctx.global_goals or not ctx.goal_embeddings:
            return 0.0
        item_emb = ctx.item_embeddings.get(item.id)
        if not item_emb:
            return 0.0
        best = 0.0
        for goal in ctx.global_goals:
            gid = str(getattr(goal, "id", ""))
            goal_emb = ctx.goal_embeddings.get(gid)
            if goal_emb:
                sim = self._cosine_similarity(item_emb, goal_emb)
                best = max(best, sim)
        return max(0.0, min(1.0, best))

    def compute_recency(self, item: WorkingSetItem, ctx: ScoringContext) -> float:
        ref_time = item.last_used_at or item.updated_at or item.created_at
        if ref_time is None:
            return 0.5  # neutral when no timestamp
        now = ctx.now
        if ref_time.tzinfo is None:
            ref_time = ref_time.replace(tzinfo=UTC)
        if now.tzinfo is None:
            now = now.replace(tzinfo=UTC)
        hours_since = max(0.0, (now - ref_time).total_seconds() / 3600.0)
        half_life = ctx.weights.recency_half_life_hours
        return math.exp(-math.log(2) / half_life * hours_since)

    def compute_successful_use_prior(self, item: WorkingSetItem, ctx: ScoringContext) -> float:
        if item.use_count > 0:
            return item.successful_use_count / item.use_count
        return ctx.scoring_config.neutral_use_prior

    def compute_confidence(self, item: WorkingSetItem, ctx: ScoringContext) -> float:
        status = ctx.verification_index.get(item.id)
        vm = ctx.verification_multipliers
        multiplier_map = {
            "supervisor_verified": vm.supervisor_verified,
            "tool_supported": vm.tool_supported,
            "self_supported": vm.self_supported,
            "unverified": vm.unverified,
        }
        multiplier = multiplier_map.get(status, vm.no_claim) if status else vm.no_claim
        return min(1.0, item.confidence * multiplier)

    def compute_evidence_strength(self, item: WorkingSetItem, ctx: ScoringContext) -> float:
        count = ctx.evidence_index.get(item.id, 0)
        max_refs = ctx.weights.evidence_refs_for_max_score
        return min(count / max_refs, 1.0)

    def compute_novelty(self, item: WorkingSetItem, ctx: ScoringContext) -> float:
        return 0.0 if item.id in ctx.compact_state_ids else 1.0

    def compute_cost_penalty(self, item: WorkingSetItem, budget_remaining: int) -> float:
        return item.token_size / max(budget_remaining, 1)

    def compute_redundancy_penalty(
        self, item: WorkingSetItem, selected: list[WorkingSetItem], ctx: ScoringContext,
    ) -> float:
        if not selected:
            return 0.0
        item_emb = ctx.item_embeddings.get(item.id)
        if not item_emb:
            return 0.0
        threshold = ctx.weights.redundancy_similarity_threshold
        max_sim = 0.0
        for sel in selected:
            sel_emb = ctx.item_embeddings.get(sel.id)
            if sel_emb:
                sim = self._cosine_similarity(item_emb, sel_emb)
                max_sim = max(max_sim, sim)
        return max_sim if max_sim > threshold else 0.0

    def compute_contradiction_penalty(
        self, item: WorkingSetItem, selected: list[WorkingSetItem], ctx: ScoringContext,
    ) -> float:
        max_penalty = 0.0
        cc = ctx.conflict_config
        for sel in selected:
            # Layer 1: Graph edges
            pair = (item.id, sel.id)
            pair_rev = (sel.id, item.id)
            for p in (pair, pair_rev):
                if p in ctx.conflict_pairs:
                    edge_type = ctx.conflict_edge_types.get(p, "SUPERSEDES")
                    if edge_type == "CONTRADICTS":
                        max_penalty = max(max_penalty, cc.contradiction_edge_penalty)
                    else:
                        max_penalty = max(max_penalty, cc.supersession_penalty)

            # Layer 2: High similarity + confidence divergence
            item_emb = ctx.item_embeddings.get(item.id)
            sel_emb = ctx.item_embeddings.get(sel.id)
            if item_emb and sel_emb:
                sim = self._cosine_similarity(item_emb, sel_emb)
                if sim > ctx.weights.contradiction_similarity_threshold:
                    gap = abs(item.confidence - sel.confidence)
                    if gap > ctx.weights.contradiction_confidence_gap:
                        max_penalty = max(max_penalty, cc.layer2_penalty)

        return max_penalty

    @staticmethod
    def _cosine_similarity(a: list[float], b: list[float]) -> float:
        if not a or not b or len(a) != len(b):
            return 0.0
        dot = sum(x * y for x, y in zip(a, b))
        norm_a = math.sqrt(sum(x * x for x in a))
        norm_b = math.sqrt(sum(x * x for x in b))
        if norm_a < 1e-10 or norm_b < 1e-10:
            return 0.0
        return dot / (norm_a * norm_b)
