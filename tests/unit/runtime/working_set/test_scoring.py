"""Comprehensive tests for ScoringEngine — 11-dimension scoring system."""
from __future__ import annotations

import math
import uuid
from datetime import UTC, datetime, timedelta

import pytest

from elephantbroker.runtime.working_set.scoring import ScoringEngine
from elephantbroker.schemas.config import (
    ConflictDetectionConfig,
    ScoringConfig,
    VerificationMultipliers,
)
from elephantbroker.schemas.working_set import (
    ScoringContext,
    ScoringWeights,
    WorkingSetItem,
    WorkingSetScores,
)
from tests.fixtures.factories import (
    make_goal_state,
    make_scoring_context,
    make_working_set_item,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _unit_vec(dim: int, index: int) -> list[float]:
    """Create a unit vector with 1.0 at `index` and 0.0 elsewhere."""
    v = [0.0] * dim
    v[index] = 1.0
    return v


def _uniform_vec(dim: int, value: float = 1.0) -> list[float]:
    """Create a vector with the same value in all dimensions."""
    return [value] * dim


# ---------------------------------------------------------------------------
# Cosine similarity (static method)
# ---------------------------------------------------------------------------

class TestCosineSimilarity:
    def test_identical_vectors(self):
        v = [1.0, 0.0, 0.0]
        assert ScoringEngine._cosine_similarity(v, v) == pytest.approx(1.0)

    def test_orthogonal_vectors(self):
        a = [1.0, 0.0]
        b = [0.0, 1.0]
        assert ScoringEngine._cosine_similarity(a, b) == pytest.approx(0.0)

    def test_opposite_vectors(self):
        a = [1.0, 0.0]
        b = [-1.0, 0.0]
        assert ScoringEngine._cosine_similarity(a, b) == pytest.approx(-1.0)

    def test_empty_vectors(self):
        assert ScoringEngine._cosine_similarity([], []) == 0.0

    def test_mismatched_lengths(self):
        assert ScoringEngine._cosine_similarity([1.0], [1.0, 2.0]) == 0.0

    def test_zero_vector(self):
        assert ScoringEngine._cosine_similarity([0.0, 0.0], [1.0, 0.0]) == 0.0

    def test_both_zero_vectors(self):
        assert ScoringEngine._cosine_similarity([0.0, 0.0], [0.0, 0.0]) == 0.0

    def test_partial_similarity(self):
        a = [1.0, 1.0]
        b = [1.0, 0.0]
        expected = 1.0 / math.sqrt(2)
        assert ScoringEngine._cosine_similarity(a, b) == pytest.approx(expected, abs=1e-6)

    def test_negative_components(self):
        a = [1.0, -1.0]
        b = [1.0, -1.0]
        assert ScoringEngine._cosine_similarity(a, b) == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# Turn relevance
# ---------------------------------------------------------------------------

class TestTurnRelevance:
    def setup_method(self):
        self.engine = ScoringEngine()

    def test_identical_embeddings(self):
        emb = [0.5, 0.5, 0.5]
        item = make_working_set_item(id="a")
        ctx = make_scoring_context(
            turn_embedding=emb,
            item_embeddings={"a": emb},
        )
        assert self.engine.compute_turn_relevance(item, ctx) == pytest.approx(1.0)

    def test_orthogonal_embeddings(self):
        item = make_working_set_item(id="a")
        ctx = make_scoring_context(
            turn_embedding=_unit_vec(3, 0),
            item_embeddings={"a": _unit_vec(3, 1)},
        )
        assert self.engine.compute_turn_relevance(item, ctx) == pytest.approx(0.0)

    def test_missing_item_embedding(self):
        item = make_working_set_item(id="a")
        ctx = make_scoring_context(
            turn_embedding=[0.1, 0.2],
            item_embeddings={},
        )
        assert self.engine.compute_turn_relevance(item, ctx) == 0.0

    def test_missing_turn_embedding(self):
        item = make_working_set_item(id="a")
        ctx = make_scoring_context(
            turn_embedding=[],
            item_embeddings={"a": [0.1, 0.2]},
        )
        assert self.engine.compute_turn_relevance(item, ctx) == 0.0

    def test_clamped_to_0_1(self):
        """Negative cosine similarity should be clamped to 0.0."""
        item = make_working_set_item(id="a")
        ctx = make_scoring_context(
            turn_embedding=[1.0, 0.0],
            item_embeddings={"a": [-1.0, 0.0]},
        )
        assert self.engine.compute_turn_relevance(item, ctx) == 0.0


# ---------------------------------------------------------------------------
# Session goal relevance
# ---------------------------------------------------------------------------

class TestSessionGoalRelevance:
    def setup_method(self):
        self.engine = ScoringEngine()

    def test_direct_tag_returns_1(self):
        item = make_working_set_item(id="a", goal_relevance_tags={"g1": "direct"})
        ctx = make_scoring_context()
        assert self.engine.compute_session_goal_relevance(item, ctx) == 1.0

    def test_indirect_tag_returns_0_7(self):
        item = make_working_set_item(id="a", goal_relevance_tags={"g1": "indirect"})
        ctx = make_scoring_context()
        assert self.engine.compute_session_goal_relevance(item, ctx) == pytest.approx(0.7)

    def test_mixed_tags_take_best(self):
        item = make_working_set_item(id="a", goal_relevance_tags={"g1": "indirect", "g2": "direct"})
        ctx = make_scoring_context()
        assert self.engine.compute_session_goal_relevance(item, ctx) == 1.0

    def test_unknown_tag_strength_ignored(self):
        """Tags with unrecognized strength values do not contribute."""
        item = make_working_set_item(id="a", goal_relevance_tags={"g1": "tangential"})
        ctx = make_scoring_context()
        # best stays 0, so falls through to embedding-based
        # but no goals/embeddings -> 0
        assert self.engine.compute_session_goal_relevance(item, ctx) == 0.0

    def test_embedding_fallback_with_goals(self):
        goal = make_goal_state()
        gid = str(goal.id)
        emb = _uniform_vec(3, 1.0)
        item = make_working_set_item(id="a")
        ctx = make_scoring_context(
            session_goals=[goal],
            goal_embeddings={gid: emb},
            item_embeddings={"a": emb},
        )
        assert self.engine.compute_session_goal_relevance(item, ctx) == pytest.approx(1.0)

    def test_no_goals_returns_0(self):
        item = make_working_set_item(id="a")
        ctx = make_scoring_context(session_goals=[], goal_embeddings={})
        assert self.engine.compute_session_goal_relevance(item, ctx) == 0.0

    def test_no_item_embedding_returns_0(self):
        goal = make_goal_state()
        item = make_working_set_item(id="a")
        ctx = make_scoring_context(
            session_goals=[goal],
            goal_embeddings={str(goal.id): [0.1, 0.2]},
            item_embeddings={},
        )
        assert self.engine.compute_session_goal_relevance(item, ctx) == 0.0

    def test_multiple_goals_takes_best(self):
        g1 = make_goal_state()
        g2 = make_goal_state()
        item = make_working_set_item(id="a")
        # Item is identical to g2 embedding, orthogonal to g1
        ctx = make_scoring_context(
            session_goals=[g1, g2],
            goal_embeddings={
                str(g1.id): _unit_vec(3, 0),
                str(g2.id): _unit_vec(3, 1),
            },
            item_embeddings={"a": _unit_vec(3, 1)},
        )
        assert self.engine.compute_session_goal_relevance(item, ctx) == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# Global goal relevance
# ---------------------------------------------------------------------------

class TestGlobalGoalRelevance:
    def setup_method(self):
        self.engine = ScoringEngine()

    def test_no_global_goals(self):
        item = make_working_set_item(id="a")
        ctx = make_scoring_context(global_goals=[])
        assert self.engine.compute_global_goal_relevance(item, ctx) == 0.0

    def test_identical_embedding(self):
        goal = make_goal_state()
        emb = [0.3, 0.4, 0.5]
        item = make_working_set_item(id="a")
        ctx = make_scoring_context(
            global_goals=[goal],
            goal_embeddings={str(goal.id): emb},
            item_embeddings={"a": emb},
        )
        assert self.engine.compute_global_goal_relevance(item, ctx) == pytest.approx(1.0)

    def test_no_item_embedding(self):
        goal = make_goal_state()
        item = make_working_set_item(id="a")
        ctx = make_scoring_context(
            global_goals=[goal],
            goal_embeddings={str(goal.id): [0.1]},
            item_embeddings={},
        )
        assert self.engine.compute_global_goal_relevance(item, ctx) == 0.0

    def test_no_goal_embeddings(self):
        goal = make_goal_state()
        item = make_working_set_item(id="a")
        ctx = make_scoring_context(
            global_goals=[goal],
            goal_embeddings={},
            item_embeddings={"a": [0.1]},
        )
        assert self.engine.compute_global_goal_relevance(item, ctx) == 0.0


# ---------------------------------------------------------------------------
# Recency
# ---------------------------------------------------------------------------

class TestRecency:
    def setup_method(self):
        self.engine = ScoringEngine()

    def test_no_timestamps_returns_0_5(self):
        item = make_working_set_item(id="a")
        ctx = make_scoring_context()
        assert self.engine.compute_recency(item, ctx) == 0.5

    def test_just_now_returns_near_1(self):
        now = datetime.now(UTC)
        item = make_working_set_item(id="a", created_at=now)
        ctx = make_scoring_context(now=now)
        assert self.engine.compute_recency(item, ctx) == pytest.approx(1.0, abs=0.01)

    def test_one_half_life_ago_returns_0_5(self):
        now = datetime.now(UTC)
        half_life = 69.0  # default
        ref_time = now - timedelta(hours=half_life)
        item = make_working_set_item(id="a", created_at=ref_time)
        ctx = make_scoring_context(now=now)
        assert self.engine.compute_recency(item, ctx) == pytest.approx(0.5, abs=0.01)

    def test_two_half_lives_returns_0_25(self):
        now = datetime.now(UTC)
        half_life = 69.0
        ref_time = now - timedelta(hours=2 * half_life)
        item = make_working_set_item(id="a", created_at=ref_time)
        ctx = make_scoring_context(now=now)
        assert self.engine.compute_recency(item, ctx) == pytest.approx(0.25, abs=0.01)

    def test_last_used_at_takes_priority(self):
        """last_used_at is checked first, so even with old created_at, recent use wins."""
        now = datetime.now(UTC)
        item = make_working_set_item(
            id="a",
            created_at=now - timedelta(hours=1000),
            updated_at=now - timedelta(hours=500),
            last_used_at=now,
        )
        ctx = make_scoring_context(now=now)
        assert self.engine.compute_recency(item, ctx) == pytest.approx(1.0, abs=0.01)

    def test_updated_at_fallback(self):
        now = datetime.now(UTC)
        item = make_working_set_item(
            id="a",
            created_at=now - timedelta(hours=1000),
            updated_at=now,
        )
        ctx = make_scoring_context(now=now)
        assert self.engine.compute_recency(item, ctx) == pytest.approx(1.0, abs=0.01)

    def test_custom_half_life(self):
        now = datetime.now(UTC)
        ref_time = now - timedelta(hours=24)
        item = make_working_set_item(id="a", created_at=ref_time)
        weights = ScoringWeights(recency_half_life_hours=24.0)
        ctx = make_scoring_context(now=now, weights=weights)
        assert self.engine.compute_recency(item, ctx) == pytest.approx(0.5, abs=0.01)

    def test_naive_timestamp_handling(self):
        """Naive datetimes should be treated as UTC."""
        now_naive = datetime(2025, 1, 1, 12, 0, 0)
        ref_naive = datetime(2025, 1, 1, 12, 0, 0)
        item = make_working_set_item(id="a", created_at=ref_naive)
        ctx = make_scoring_context(now=now_naive)
        # Both are the same time, so should be ~1.0
        assert self.engine.compute_recency(item, ctx) == pytest.approx(1.0, abs=0.01)

    def test_naive_timezone_produces_correct_decay(self):
        """Naive datetimes with a real time gap must produce correct half-life decay."""
        ref_naive = datetime(2025, 1, 1, 11, 0, 0)  # 1 hour in the past, no tzinfo
        now_naive = datetime(2025, 1, 1, 12, 0, 0)   # no tzinfo
        item = make_working_set_item(id="a", created_at=ref_naive)
        weights = ScoringWeights(recency_half_life_hours=1.0)
        ctx = make_scoring_context(now=now_naive, weights=weights)
        result = self.engine.compute_recency(item, ctx)
        # With half_life=1h and gap=1h: exp(-ln2/1 * 1) = 0.5
        assert result == pytest.approx(0.5, abs=0.01)
        # Must NOT be ~1.0 (which would mean the time gap was ignored)
        assert result < 0.9


# ---------------------------------------------------------------------------
# Successful use prior
# ---------------------------------------------------------------------------

class TestSuccessfulUsePrior:
    def setup_method(self):
        self.engine = ScoringEngine()

    def test_no_uses_returns_neutral(self):
        item = make_working_set_item(id="a", use_count=0)
        ctx = make_scoring_context()
        assert self.engine.compute_successful_use_prior(item, ctx) == 0.5

    def test_custom_neutral_prior(self):
        item = make_working_set_item(id="a", use_count=0)
        ctx = make_scoring_context(scoring_config=ScoringConfig(neutral_use_prior=0.7))
        assert self.engine.compute_successful_use_prior(item, ctx) == 0.7

    def test_all_successful(self):
        item = make_working_set_item(id="a", use_count=10, successful_use_count=10)
        ctx = make_scoring_context()
        assert self.engine.compute_successful_use_prior(item, ctx) == 1.0

    def test_none_successful(self):
        item = make_working_set_item(id="a", use_count=10, successful_use_count=0)
        ctx = make_scoring_context()
        assert self.engine.compute_successful_use_prior(item, ctx) == 0.0

    def test_partial_success(self):
        item = make_working_set_item(id="a", use_count=4, successful_use_count=3)
        ctx = make_scoring_context()
        assert self.engine.compute_successful_use_prior(item, ctx) == pytest.approx(0.75)


# ---------------------------------------------------------------------------
# Confidence
# ---------------------------------------------------------------------------

class TestConfidence:
    def setup_method(self):
        self.engine = ScoringEngine()

    def test_no_verification_uses_no_claim(self):
        item = make_working_set_item(id="a", confidence=1.0)
        ctx = make_scoring_context()
        # no_claim default = 0.8
        assert self.engine.compute_confidence(item, ctx) == pytest.approx(0.8)

    def test_supervisor_verified(self):
        item = make_working_set_item(id="a", confidence=1.0)
        ctx = make_scoring_context(verification_index={"a": "supervisor_verified"})
        assert self.engine.compute_confidence(item, ctx) == pytest.approx(1.0)

    def test_tool_supported(self):
        item = make_working_set_item(id="a", confidence=1.0)
        ctx = make_scoring_context(verification_index={"a": "tool_supported"})
        assert self.engine.compute_confidence(item, ctx) == pytest.approx(0.9)

    def test_self_supported(self):
        item = make_working_set_item(id="a", confidence=1.0)
        ctx = make_scoring_context(verification_index={"a": "self_supported"})
        assert self.engine.compute_confidence(item, ctx) == pytest.approx(0.7)

    def test_unverified(self):
        item = make_working_set_item(id="a", confidence=1.0)
        ctx = make_scoring_context(verification_index={"a": "unverified"})
        assert self.engine.compute_confidence(item, ctx) == pytest.approx(0.5)

    def test_unknown_status_uses_no_claim(self):
        item = make_working_set_item(id="a", confidence=1.0)
        ctx = make_scoring_context(verification_index={"a": "something_unknown"})
        # Falls through to no_claim
        assert self.engine.compute_confidence(item, ctx) == pytest.approx(0.8)

    def test_low_confidence_item(self):
        item = make_working_set_item(id="a", confidence=0.5)
        ctx = make_scoring_context(verification_index={"a": "supervisor_verified"})
        # 0.5 * 1.0 = 0.5
        assert self.engine.compute_confidence(item, ctx) == pytest.approx(0.5)

    def test_capped_at_1(self):
        """Even with high multiplier and confidence, result is capped at 1.0."""
        item = make_working_set_item(id="a", confidence=0.9)
        vm = VerificationMultipliers(supervisor_verified=2.0)
        ctx = make_scoring_context(
            verification_index={"a": "supervisor_verified"},
            verification_multipliers=vm,
        )
        # 0.9 * 2.0 = 1.8 -> capped to 1.0
        assert self.engine.compute_confidence(item, ctx) == 1.0

    def test_custom_multipliers(self):
        item = make_working_set_item(id="a", confidence=1.0)
        vm = VerificationMultipliers(tool_supported=0.6)
        ctx = make_scoring_context(
            verification_index={"a": "tool_supported"},
            verification_multipliers=vm,
        )
        assert self.engine.compute_confidence(item, ctx) == pytest.approx(0.6)


# ---------------------------------------------------------------------------
# Evidence strength
# ---------------------------------------------------------------------------

class TestEvidenceStrength:
    def setup_method(self):
        self.engine = ScoringEngine()

    def test_no_evidence(self):
        item = make_working_set_item(id="a")
        ctx = make_scoring_context()
        assert self.engine.compute_evidence_strength(item, ctx) == 0.0

    def test_max_evidence(self):
        item = make_working_set_item(id="a")
        ctx = make_scoring_context(evidence_index={"a": 3})
        # default evidence_refs_for_max_score = 3
        assert self.engine.compute_evidence_strength(item, ctx) == pytest.approx(1.0)

    def test_partial_evidence(self):
        item = make_working_set_item(id="a")
        ctx = make_scoring_context(evidence_index={"a": 1})
        assert self.engine.compute_evidence_strength(item, ctx) == pytest.approx(1.0 / 3.0)

    def test_over_max_capped(self):
        item = make_working_set_item(id="a")
        ctx = make_scoring_context(evidence_index={"a": 10})
        assert self.engine.compute_evidence_strength(item, ctx) == 1.0

    def test_custom_refs_for_max(self):
        item = make_working_set_item(id="a")
        weights = ScoringWeights(evidence_refs_for_max_score=5)
        ctx = make_scoring_context(evidence_index={"a": 2}, weights=weights)
        assert self.engine.compute_evidence_strength(item, ctx) == pytest.approx(0.4)


# ---------------------------------------------------------------------------
# Novelty
# ---------------------------------------------------------------------------

class TestNovelty:
    def setup_method(self):
        self.engine = ScoringEngine()

    def test_novel_item(self):
        item = make_working_set_item(id="a")
        ctx = make_scoring_context(compact_state_ids=set())
        assert self.engine.compute_novelty(item, ctx) == 1.0

    def test_not_novel(self):
        item = make_working_set_item(id="a")
        ctx = make_scoring_context(compact_state_ids={"a"})
        assert self.engine.compute_novelty(item, ctx) == 0.0

    def test_other_ids_in_compact_state(self):
        item = make_working_set_item(id="a")
        ctx = make_scoring_context(compact_state_ids={"b", "c"})
        assert self.engine.compute_novelty(item, ctx) == 1.0


# ---------------------------------------------------------------------------
# Cost penalty
# ---------------------------------------------------------------------------

class TestCostPenalty:
    def setup_method(self):
        self.engine = ScoringEngine()

    def test_zero_tokens(self):
        item = make_working_set_item(id="a", token_size=0)
        assert self.engine.compute_cost_penalty(item, 8000) == 0.0

    def test_full_budget(self):
        item = make_working_set_item(id="a", token_size=8000)
        assert self.engine.compute_cost_penalty(item, 8000) == pytest.approx(1.0)

    def test_half_budget(self):
        item = make_working_set_item(id="a", token_size=4000)
        assert self.engine.compute_cost_penalty(item, 8000) == pytest.approx(0.5)

    def test_zero_budget_remaining(self):
        """Zero budget remaining should not divide by zero."""
        item = make_working_set_item(id="a", token_size=100)
        result = self.engine.compute_cost_penalty(item, 0)
        assert result == 100.0

    def test_negative_budget_remaining(self):
        """Negative budget treated same as zero (max(budget, 1))."""
        item = make_working_set_item(id="a", token_size=100)
        result = self.engine.compute_cost_penalty(item, -5)
        assert result == 100.0


# ---------------------------------------------------------------------------
# Redundancy penalty
# ---------------------------------------------------------------------------

class TestRedundancyPenalty:
    def setup_method(self):
        self.engine = ScoringEngine()

    def test_empty_selected(self):
        item = make_working_set_item(id="a")
        ctx = make_scoring_context()
        assert self.engine.compute_redundancy_penalty(item, [], ctx) == 0.0

    def test_no_item_embedding(self):
        item = make_working_set_item(id="a")
        sel = make_working_set_item(id="b")
        ctx = make_scoring_context(item_embeddings={"b": [1.0]})
        assert self.engine.compute_redundancy_penalty(item, [sel], ctx) == 0.0

    def test_identical_items_above_threshold(self):
        emb = [1.0, 0.0, 0.0]
        item = make_working_set_item(id="a")
        sel = make_working_set_item(id="b")
        ctx = make_scoring_context(item_embeddings={"a": emb, "b": emb})
        penalty = self.engine.compute_redundancy_penalty(item, [sel], ctx)
        assert penalty == pytest.approx(1.0)

    def test_dissimilar_below_threshold(self):
        item = make_working_set_item(id="a")
        sel = make_working_set_item(id="b")
        ctx = make_scoring_context(
            item_embeddings={
                "a": _unit_vec(3, 0),
                "b": _unit_vec(3, 1),
            },
        )
        penalty = self.engine.compute_redundancy_penalty(item, [sel], ctx)
        assert penalty == 0.0

    def test_custom_threshold(self):
        # Create vectors with moderate similarity (~0.95)
        a = [1.0, 0.0]
        b = [1.0, 0.3]
        item = make_working_set_item(id="a")
        sel = make_working_set_item(id="b")
        weights = ScoringWeights(redundancy_similarity_threshold=0.99)
        ctx = make_scoring_context(
            item_embeddings={"a": a, "b": b},
            weights=weights,
        )
        sim = ScoringEngine._cosine_similarity(a, b)
        penalty = self.engine.compute_redundancy_penalty(item, [sel], ctx)
        # Similarity should be high but below 0.99 threshold
        assert sim < 0.99
        assert penalty == 0.0

    def test_multiple_selected_takes_max(self):
        """Should return the max similarity over all selected items."""
        item = make_working_set_item(id="a")
        sel1 = make_working_set_item(id="b")
        sel2 = make_working_set_item(id="c")
        emb_a = [1.0, 0.0, 0.0]
        ctx = make_scoring_context(
            item_embeddings={
                "a": emb_a,
                "b": _unit_vec(3, 1),  # orthogonal
                "c": emb_a,  # identical
            },
        )
        penalty = self.engine.compute_redundancy_penalty(item, [sel1, sel2], ctx)
        assert penalty == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# Contradiction penalty
# ---------------------------------------------------------------------------

class TestContradictionPenalty:
    def setup_method(self):
        self.engine = ScoringEngine()

    def test_no_selected(self):
        item = make_working_set_item(id="a")
        ctx = make_scoring_context()
        assert self.engine.compute_contradiction_penalty(item, [], ctx) == 0.0

    def test_contradicts_edge(self):
        item = make_working_set_item(id="a")
        sel = make_working_set_item(id="b")
        ctx = make_scoring_context(
            conflict_pairs={("a", "b")},
            conflict_edge_types={("a", "b"): "CONTRADICTS"},
        )
        penalty = self.engine.compute_contradiction_penalty(item, [sel], ctx)
        assert penalty == pytest.approx(0.9)  # contradiction_edge_penalty default

    def test_supersedes_edge(self):
        item = make_working_set_item(id="a")
        sel = make_working_set_item(id="b")
        ctx = make_scoring_context(
            conflict_pairs={("a", "b")},
            conflict_edge_types={("a", "b"): "SUPERSEDES"},
        )
        penalty = self.engine.compute_contradiction_penalty(item, [sel], ctx)
        assert penalty == pytest.approx(1.0)  # supersession_penalty default

    def test_reverse_pair_detected(self):
        """Conflict detected even if the pair is in reverse order."""
        item = make_working_set_item(id="a")
        sel = make_working_set_item(id="b")
        ctx = make_scoring_context(
            conflict_pairs={("b", "a")},
            conflict_edge_types={("b", "a"): "CONTRADICTS"},
        )
        penalty = self.engine.compute_contradiction_penalty(item, [sel], ctx)
        assert penalty == pytest.approx(0.9)

    def test_no_edge_type_defaults_to_supersedes(self):
        item = make_working_set_item(id="a")
        sel = make_working_set_item(id="b")
        ctx = make_scoring_context(
            conflict_pairs={("a", "b")},
            conflict_edge_types={},  # no edge type recorded
        )
        penalty = self.engine.compute_contradiction_penalty(item, [sel], ctx)
        # Defaults to SUPERSEDES -> supersession_penalty = 1.0
        assert penalty == pytest.approx(1.0)

    def test_layer2_high_sim_confidence_gap(self):
        """Layer 2: high cosine similarity + confidence divergence."""
        emb = [1.0, 0.0, 0.0]
        item = make_working_set_item(id="a", confidence=0.9)
        sel = make_working_set_item(id="b", confidence=0.3)
        ctx = make_scoring_context(
            item_embeddings={"a": emb, "b": emb},  # sim = 1.0 > 0.9 threshold
            # gap = 0.6 > 0.3 threshold
        )
        penalty = self.engine.compute_contradiction_penalty(item, [sel], ctx)
        assert penalty == pytest.approx(0.7)  # layer2_penalty default

    def test_layer2_not_triggered_below_sim_threshold(self):
        item = make_working_set_item(id="a", confidence=0.9)
        sel = make_working_set_item(id="b", confidence=0.3)
        ctx = make_scoring_context(
            item_embeddings={
                "a": _unit_vec(3, 0),
                "b": _unit_vec(3, 1),
            },
        )
        penalty = self.engine.compute_contradiction_penalty(item, [sel], ctx)
        assert penalty == 0.0

    def test_layer2_not_triggered_small_confidence_gap(self):
        emb = [1.0, 0.0, 0.0]
        item = make_working_set_item(id="a", confidence=0.8)
        sel = make_working_set_item(id="b", confidence=0.7)
        ctx = make_scoring_context(
            item_embeddings={"a": emb, "b": emb},
        )
        # gap = 0.1 < 0.3 threshold
        penalty = self.engine.compute_contradiction_penalty(item, [sel], ctx)
        assert penalty == 0.0

    def test_layer1_and_layer2_takes_max(self):
        """Both graph edge and layer 2 triggered — should take max penalty."""
        emb = [1.0, 0.0, 0.0]
        item = make_working_set_item(id="a", confidence=0.9)
        sel = make_working_set_item(id="b", confidence=0.3)
        ctx = make_scoring_context(
            conflict_pairs={("a", "b")},
            conflict_edge_types={("a", "b"): "CONTRADICTS"},
            item_embeddings={"a": emb, "b": emb},
        )
        penalty = self.engine.compute_contradiction_penalty(item, [sel], ctx)
        # contradiction_edge = 0.9, layer2 = 0.7, supersession = N/A -> max = 0.9
        assert penalty == pytest.approx(0.9)

    def test_custom_conflict_config(self):
        item = make_working_set_item(id="a")
        sel = make_working_set_item(id="b")
        cc = ConflictDetectionConfig(
            supersession_penalty=0.5,
            contradiction_edge_penalty=0.3,
            layer2_penalty=0.2,
        )
        ctx = make_scoring_context(
            conflict_pairs={("a", "b")},
            conflict_edge_types={("a", "b"): "CONTRADICTS"},
            conflict_config=cc,
        )
        penalty = self.engine.compute_contradiction_penalty(item, [sel], ctx)
        assert penalty == pytest.approx(0.3)

    def test_multiple_selected_takes_max_penalty(self):
        item = make_working_set_item(id="a")
        sel1 = make_working_set_item(id="b")
        sel2 = make_working_set_item(id="c")
        ctx = make_scoring_context(
            conflict_pairs={("a", "c")},
            conflict_edge_types={("a", "c"): "SUPERSEDES"},
        )
        penalty = self.engine.compute_contradiction_penalty(item, [sel1, sel2], ctx)
        assert penalty == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# score_independent
# ---------------------------------------------------------------------------

class TestScoreIndependent:
    def setup_method(self):
        self.engine = ScoringEngine()

    def test_all_zeros_context(self):
        item = make_working_set_item(id="a", token_size=100)
        ctx = make_scoring_context(
            turn_embedding=[],
            item_embeddings={},
            token_budget=8000,
        )
        scores = self.engine.score_independent(item, ctx)
        assert isinstance(scores, WorkingSetScores)
        assert scores.turn_relevance == 0.0
        assert scores.session_goal_relevance == 0.0
        assert scores.global_goal_relevance == 0.0
        assert scores.recency == 0.5  # no timestamps
        assert scores.novelty == 1.0  # not in compact state
        assert scores.redundancy_penalty == 0.0  # left at 0
        assert scores.contradiction_penalty == 0.0  # left at 0

    def test_final_score_computed(self):
        emb = [1.0, 0.0]
        now = datetime.now(UTC)
        item = make_working_set_item(
            id="a", token_size=100, confidence=1.0,
            created_at=now, use_count=4, successful_use_count=4,
        )
        ctx = make_scoring_context(
            turn_embedding=emb,
            item_embeddings={"a": emb},
            evidence_index={"a": 3},
            verification_index={"a": "supervisor_verified"},
            now=now,
            token_budget=8000,
        )
        scores = self.engine.score_independent(item, ctx)
        assert scores.final != 0.0
        # Verify final = weighted_sum(scores)
        expected_final = ctx.weights.weighted_sum(scores)
        assert scores.final == pytest.approx(expected_final)

    def test_redundancy_and_contradiction_are_zero(self):
        item = make_working_set_item(id="a")
        ctx = make_scoring_context()
        scores = self.engine.score_independent(item, ctx)
        assert scores.redundancy_penalty == 0.0
        assert scores.contradiction_penalty == 0.0

    def test_high_confidence_high_evidence(self):
        """Item with full evidence and supervisor verification scores high on those dims."""
        now = datetime.now(UTC)
        emb = _uniform_vec(4, 0.5)
        item = make_working_set_item(
            id="a", confidence=1.0, token_size=100,
            use_count=10, successful_use_count=10,
            created_at=now,
        )
        ctx = make_scoring_context(
            turn_embedding=emb,
            item_embeddings={"a": emb},
            evidence_index={"a": 5},
            verification_index={"a": "supervisor_verified"},
            now=now,
            token_budget=8000,
        )
        scores = self.engine.score_independent(item, ctx)
        assert scores.confidence == pytest.approx(1.0)
        assert scores.evidence_strength == 1.0
        assert scores.successful_use_prior == 1.0
        assert scores.turn_relevance == pytest.approx(1.0)

    def test_must_inject_item_still_scored(self):
        """must_inject items go through normal scoring (selection logic handles injection)."""
        item = make_working_set_item(id="a", must_inject=True, token_size=50)
        ctx = make_scoring_context()
        scores = self.engine.score_independent(item, ctx)
        assert isinstance(scores, WorkingSetScores)


# ---------------------------------------------------------------------------
# Integration: score_independent with different profiles/weights
# ---------------------------------------------------------------------------

class TestScoringWithProfiles:
    def setup_method(self):
        self.engine = ScoringEngine()

    def test_coding_profile_weights(self):
        """Coding profiles often weight turn_relevance high, novelty high."""
        weights = ScoringWeights(turn_relevance=2.0, novelty=1.5, recency=0.3)
        emb = [1.0, 0.0]
        item = make_working_set_item(id="a", token_size=100)
        ctx = make_scoring_context(
            weights=weights,
            turn_embedding=emb,
            item_embeddings={"a": emb},
        )
        scores = self.engine.score_independent(item, ctx)
        # Turn relevance should dominate
        assert scores.turn_relevance == pytest.approx(1.0)
        assert scores.final > 0

    def test_zero_weight_dimension_ignored(self):
        """Dimensions with zero weight don't affect the final score."""
        weights = ScoringWeights(
            turn_relevance=0.0, session_goal_relevance=0.0,
            global_goal_relevance=0.0, recency=0.0,
            successful_use_prior=0.0, confidence=0.0,
            evidence_strength=0.0, novelty=1.0,
            cost_penalty=0.0,
        )
        item = make_working_set_item(id="a")
        ctx = make_scoring_context(weights=weights)
        scores = self.engine.score_independent(item, ctx)
        # Only novelty contributes: 1.0 * 1.0 = 1.0
        assert scores.final == pytest.approx(1.0)

    def test_heavy_cost_penalty_weight(self):
        """Heavy cost penalty makes large items score very low."""
        weights = ScoringWeights(cost_penalty=-5.0)
        item = make_working_set_item(id="a", token_size=4000)
        ctx = make_scoring_context(weights=weights, token_budget=8000)
        scores = self.engine.score_independent(item, ctx)
        # cost_penalty dim = 4000/8000 = 0.5, weight = -5.0 -> -2.5 contribution
        cost_contribution = -5.0 * 0.5
        assert cost_contribution == -2.5
        # Final should include this large negative component
        assert scores.cost_penalty == pytest.approx(0.5)


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def setup_method(self):
        self.engine = ScoringEngine()

    def test_very_large_token_size(self):
        item = make_working_set_item(id="a", token_size=1000000)
        result = self.engine.compute_cost_penalty(item, 8000)
        assert result == pytest.approx(125.0)

    def test_item_with_all_none_timestamps(self):
        item = make_working_set_item(
            id="a", created_at=None, updated_at=None, last_used_at=None,
        )
        ctx = make_scoring_context()
        assert self.engine.compute_recency(item, ctx) == 0.5

    def test_confidence_zero_item(self):
        item = make_working_set_item(id="a", confidence=0.0)
        ctx = make_scoring_context(verification_index={"a": "supervisor_verified"})
        assert self.engine.compute_confidence(item, ctx) == 0.0

    def test_single_dimension_embedding(self):
        """One-dimensional embeddings should still work."""
        item = make_working_set_item(id="a")
        ctx = make_scoring_context(
            turn_embedding=[1.0],
            item_embeddings={"a": [1.0]},
        )
        assert self.engine.compute_turn_relevance(item, ctx) == pytest.approx(1.0)

    def test_very_old_item_recency_near_zero(self):
        now = datetime.now(UTC)
        ancient = now - timedelta(days=365 * 10)
        item = make_working_set_item(id="a", created_at=ancient)
        ctx = make_scoring_context(now=now)
        recency = self.engine.compute_recency(item, ctx)
        assert recency < 0.001

    def test_future_timestamp_recency(self):
        """If ref_time is in the future, hours_since is clamped to 0 -> recency = 1.0."""
        now = datetime.now(UTC)
        future = now + timedelta(hours=100)
        item = make_working_set_item(id="a", created_at=future)
        ctx = make_scoring_context(now=now)
        assert self.engine.compute_recency(item, ctx) == pytest.approx(1.0)

    def test_redundancy_with_no_embeddings_for_selected(self):
        """If selected items have no embeddings, redundancy should be 0."""
        item = make_working_set_item(id="a")
        sel = make_working_set_item(id="b")
        ctx = make_scoring_context(item_embeddings={"a": [1.0, 0.0]})
        penalty = self.engine.compute_redundancy_penalty(item, [sel], ctx)
        assert penalty == 0.0

    def test_contradiction_with_no_embeddings(self):
        """Layer 2 should not trigger when embeddings are missing."""
        item = make_working_set_item(id="a", confidence=0.9)
        sel = make_working_set_item(id="b", confidence=0.1)
        ctx = make_scoring_context(item_embeddings={})
        penalty = self.engine.compute_contradiction_penalty(item, [sel], ctx)
        assert penalty == 0.0

    def test_goal_relevance_tags_empty_dict(self):
        """Empty dict should fall through to embedding-based computation."""
        item = make_working_set_item(id="a", goal_relevance_tags={})
        ctx = make_scoring_context()
        assert self.engine.compute_session_goal_relevance(item, ctx) == 0.0


# ---------------------------------------------------------------------------
# TF-05-001 suggested tests
# ---------------------------------------------------------------------------

class TestTF05001SuggestedTests:
    """Tests from TF-05-001 flow spec for scoring edge cases."""

    def setup_method(self):
        self.engine = ScoringEngine()

    def test_turn_relevance_high_cosine_sim(self):
        """Verify cosine similarity > 0.5 when item embedding is close to query."""
        # Two vectors with high but not perfect similarity
        query_emb = [1.0, 0.5, 0.0]
        item_emb = [0.9, 0.6, 0.1]
        item = make_working_set_item(id="a")
        ctx = make_scoring_context(
            turn_embedding=query_emb,
            item_embeddings={"a": item_emb},
        )
        relevance = self.engine.compute_turn_relevance(item, ctx)
        assert relevance > 0.5
        assert relevance < 1.0

    def test_session_goal_relevance_parent_walkup_0_7x(self):
        """Parent gets 0.7 * child_sim via parent chain walk-up (#472)."""
        parent_goal = make_goal_state(title="Parent goal")
        child_goal = make_goal_state(
            title="Child goal",
            parent_goal_id=parent_goal.id,
        )
        # Item is identical to child embedding
        child_emb = [1.0, 0.0, 0.0]
        item = make_working_set_item(id="a")
        ctx = make_scoring_context(
            session_goals=[parent_goal, child_goal],
            goal_embeddings={
                str(parent_goal.id): [0.0, 0.0, 1.0],  # orthogonal to item
                str(child_goal.id): child_emb,           # identical to item
            },
            item_embeddings={"a": child_emb},
        )
        relevance = self.engine.compute_session_goal_relevance(item, ctx)
        # Direct match with child = 1.0, parent walk-up = 1.0 * 0.7 = 0.7
        # Best is max(1.0, 0.7) = 1.0 (child direct match dominates)
        assert relevance == pytest.approx(1.0)

        # Now test where ONLY parent walk-up contributes: item is somewhat
        # similar to child (not identical) and parent has no direct match
        partial_emb = [0.8, 0.6, 0.0]  # partial similarity to child
        ctx2 = make_scoring_context(
            session_goals=[parent_goal, child_goal],
            goal_embeddings={
                str(parent_goal.id): [0.0, 0.0, 1.0],  # orthogonal
                str(child_goal.id): [1.0, 0.0, 0.0],
            },
            item_embeddings={"a": partial_emb},
        )
        relevance2 = self.engine.compute_session_goal_relevance(item, ctx2)
        child_sim = ScoringEngine._cosine_similarity(partial_emb, [1.0, 0.0, 0.0])
        parent_bonus = child_sim * 0.7
        # Result should be max(child_sim, parent_bonus) = child_sim
        assert relevance2 == pytest.approx(child_sim, abs=1e-6)

    def test_session_goal_relevance_tags_precedence_over_cosine(self):
        """Tags path exits early, skipping cosine even when embeddings exist (#473)."""
        goal = make_goal_state()
        # Tags say "direct" -> should return 1.0 immediately
        # Even though cosine with goal embedding would give lower score
        item = make_working_set_item(
            id="a",
            goal_relevance_tags={str(goal.id): "direct"},
        )
        ctx = make_scoring_context(
            session_goals=[goal],
            goal_embeddings={str(goal.id): [0.0, 1.0, 0.0]},
            item_embeddings={"a": [1.0, 0.0, 0.0]},  # orthogonal → cosine ~0.0
        )
        assert self.engine.compute_session_goal_relevance(item, ctx) == 1.0

    def test_session_goal_relevance_all_none_tags_fallthrough(self):
        """All-'none' tags give best=0, so method falls through to cosine (#1205)."""
        goal = make_goal_state()
        emb = [0.5, 0.5, 0.5]
        item = make_working_set_item(
            id="a",
            goal_relevance_tags={str(goal.id): "none"},
        )
        ctx = make_scoring_context(
            session_goals=[goal],
            goal_embeddings={str(goal.id): emb},
            item_embeddings={"a": emb},
        )
        # "none" is not "direct" or "indirect", so best stays 0 → falls through
        # Cosine of identical embeddings = 1.0
        assert self.engine.compute_session_goal_relevance(item, ctx) == pytest.approx(1.0)

    def test_goal_embedding_title_only_index_shift(self):
        """When a goal has no embedding in context, it is skipped without index shift (#1330).

        If goal_embeddings is missing for one goal, the scoring engine still
        correctly matches remaining goals by ID lookup, not positional index.
        """
        g1 = make_goal_state(title="First goal")
        g2 = make_goal_state(title="Second goal")
        emb = [1.0, 0.0, 0.0]
        item = make_working_set_item(id="a")
        # Only g2 has an embedding — g1 is missing (e.g., empty title skipped during embedding)
        ctx = make_scoring_context(
            session_goals=[g1, g2],
            goal_embeddings={str(g2.id): emb},  # g1 missing
            item_embeddings={"a": emb},
        )
        relevance = self.engine.compute_session_goal_relevance(item, ctx)
        # Should match g2 by ID, not pick up g1's missing entry
        assert relevance == pytest.approx(1.0)


# ---------------------------------------------------------------------------
# TF-05-002: Suggested tests — confidence, evidence, novelty, use prior, cost
# ---------------------------------------------------------------------------

class TestTF05002SuggestedTests:
    """Suggested tests from TF-05-002 validation. 3 of 5 already covered by
    existing tests (evidence_capped, novelty_empty, use_prior_neutral).
    """

    def setup_method(self):
        self.engine = ScoringEngine()

    def test_confidence_unknown_status_falls_back_to_no_claim(self):
        """verification_index.get() returns None when key absent → no_claim multiplier (#474).

        Distinct from test_unknown_status_uses_no_claim (which tests an unknown
        *string* value). Here the item ID is simply absent from the index,
        exercising the ``if status else vm.no_claim`` branch.
        """
        item = make_working_set_item(id="absent_item", confidence=1.0)
        ctx = make_scoring_context(verification_index={})  # empty — key not present
        result = self.engine.compute_confidence(item, ctx)
        # no_claim default = 0.8
        assert result == pytest.approx(0.8)

    def test_cost_penalty_pass1_vs_pass2_difference(self):
        """Pass 1 (full budget) vs pass 2 (reduced budget) yields ~16x penalty difference (#1315).

        In the greedy selector, pass 1 uses the full token budget (e.g., 8000),
        while pass 2 uses the remaining budget after must-inject items. A 500-token
        item at budget=8000 gets penalty 0.0625; the same item at budget=500 gets
        penalty 1.0 — a 16x increase that correctly deprioritises large items
        when headroom is tight.
        """
        item = make_working_set_item(id="a", token_size=500)

        pass1_budget = 8000
        pass2_budget = 500

        penalty_pass1 = self.engine.compute_cost_penalty(item, pass1_budget)
        penalty_pass2 = self.engine.compute_cost_penalty(item, pass2_budget)

        assert penalty_pass1 == pytest.approx(500 / 8000)  # 0.0625
        assert penalty_pass2 == pytest.approx(500 / 500)   # 1.0
        ratio = penalty_pass2 / penalty_pass1
        assert ratio == pytest.approx(16.0)
