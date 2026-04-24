"""Tests for working set schemas."""
import uuid

import pytest
from pydantic import ValidationError

from elephantbroker.schemas.working_set import (
    ScoringWeights,
    WorkingSetItem,
    WorkingSetScores,
    WorkingSetSnapshot,
)


class TestScoringWeights:
    def test_defaults(self):
        w = ScoringWeights()
        assert w.turn_relevance == 1.0
        assert w.session_goal_relevance == 1.0
        assert w.global_goal_relevance == 0.5
        assert w.recency == 0.8
        assert w.successful_use_prior == 0.6
        assert w.confidence == 0.4
        assert w.evidence_strength == 0.3
        assert w.novelty == 0.5
        assert w.redundancy_penalty == -0.7
        assert w.contradiction_penalty == -1.0
        assert w.cost_penalty == -0.3

    def test_at_least_11_weight_fields(self):
        fields = list(ScoringWeights.model_fields)
        # 11 weight dimensions + 5 Phase 5 detection/config fields = 16
        assert len(fields) == 16

    def test_penalties_can_be_negative(self):
        w = ScoringWeights(redundancy_penalty=-2.0, contradiction_penalty=-5.0, cost_penalty=-1.0)
        assert w.redundancy_penalty == -2.0

    def test_json_round_trip(self):
        w = ScoringWeights(turn_relevance=2.0, novelty=0.5)
        data = w.model_dump(mode="json")
        restored = ScoringWeights.model_validate(data)
        assert restored.turn_relevance == 2.0

    def test_weighted_sum(self):
        w = ScoringWeights(
            turn_relevance=1.0, session_goal_relevance=0.0,
            global_goal_relevance=0.0, recency=0.0,
            successful_use_prior=0.0, confidence=0.0,
            evidence_strength=0.0, novelty=0.0,
            redundancy_penalty=0.0, contradiction_penalty=0.0,
            cost_penalty=0.0,
        )
        s = WorkingSetScores(turn_relevance=0.8)
        assert w.weighted_sum(s) == pytest.approx(0.8)

    def test_weighted_sum_with_penalties(self):
        w = ScoringWeights(
            turn_relevance=1.0, session_goal_relevance=0.0,
            global_goal_relevance=0.0, recency=0.0,
            successful_use_prior=0.0, confidence=0.0,
            evidence_strength=0.0, novelty=0.0,
            redundancy_penalty=-1.0, contradiction_penalty=0.0,
            cost_penalty=0.0,
        )
        s = WorkingSetScores(turn_relevance=0.8, redundancy_penalty=0.5)
        assert w.weighted_sum(s) == pytest.approx(0.8 - 0.5)

    def test_weighted_sum_default_weights_redundancy_only(self):
        """G1: default weights * only redundancy_penalty=1.0 -> -0.7."""
        w = ScoringWeights()
        s = WorkingSetScores(redundancy_penalty=1.0)
        assert w.weighted_sum(s) == pytest.approx(-0.7, abs=1e-9)

    def test_weighted_sum_default_weights_contradiction_only(self):
        """G2: default weights * only contradiction_penalty=1.0 -> -1.0."""
        w = ScoringWeights()
        s = WorkingSetScores(contradiction_penalty=1.0)
        assert w.weighted_sum(s) == pytest.approx(-1.0, abs=1e-9)

    def test_weighted_sum_default_weights_all_scores_one(self):
        """G3: canonical full-firing sum == 3.1 (schema-derived).

        Positives (1.0+1.0+0.5+0.8+0.6+0.4+0.3+0.5) = 5.1
        Negatives (-0.7 + -1.0 + -0.3)              = -2.0
        Total                                        = 3.1
        """
        w = ScoringWeights()
        s = WorkingSetScores(
            turn_relevance=1.0,
            session_goal_relevance=1.0,
            global_goal_relevance=1.0,
            recency=1.0,
            successful_use_prior=1.0,
            confidence=1.0,
            evidence_strength=1.0,
            novelty=1.0,
            redundancy_penalty=1.0,
            contradiction_penalty=1.0,
            cost_penalty=1.0,
        )
        assert w.weighted_sum(s) == pytest.approx(3.1, abs=1e-9)

    @pytest.mark.parametrize(
        "field,expected",
        [
            ("turn_relevance", 1.0),
            ("session_goal_relevance", 1.0),
            ("global_goal_relevance", 0.5),
            ("recency", 0.8),
            ("successful_use_prior", 0.6),
            ("confidence", 0.4),
            ("evidence_strength", 0.3),
            ("novelty", 0.5),
            ("redundancy_penalty", -0.7),
            ("contradiction_penalty", -1.0),
            ("cost_penalty", -0.3),
        ],
    )
    def test_weighted_sum_uses_exactly_11_dimensions(self, field, expected):
        """G4: firing a single dimension at 1.0 (all others 0.0) yields exactly that dim's weight.

        Pins the contract: weighted_sum is a pure linear combination over these 11 fields,
        no hidden dims, no aux-fields leakage.
        """
        s = WorkingSetScores(**{field: 1.0})
        assert ScoringWeights().weighted_sum(s) == pytest.approx(expected, abs=1e-9)

    def test_weighted_sum_ignores_auxiliary_params(self):
        """G5: the 5 auxiliary fields (half_life, evidence_refs_for_max, redundancy/contradiction
        thresholds, confidence_gap) do NOT enter weighted_sum -- pins the isolation contract."""
        w_default = ScoringWeights()
        w_extreme = ScoringWeights(
            recency_half_life_hours=999999,
            evidence_refs_for_max_score=999,
            redundancy_similarity_threshold=0.0,
            contradiction_similarity_threshold=0.0,
            contradiction_confidence_gap=0.0,
        )
        s = WorkingSetScores(turn_relevance=1.0, redundancy_penalty=1.0)
        assert w_default.weighted_sum(s) == pytest.approx(w_extreme.weighted_sum(s))

    def test_penalty_defaults_are_negative(self):
        """G6: the 3 penalty dims default negative so that high penalty scores subtract."""
        w = ScoringWeights()
        assert w.redundancy_penalty < 0
        assert w.contradiction_penalty < 0
        assert w.cost_penalty < 0

    def test_positive_penalty_weight_accepted_by_schema(self):
        """G7: schema accepts positive penalty weights (no le=0 constraint)."""
        # Schema permits positive penalty weights by design (#1147 -- no le=0 constraint).
        # Runtime ScoringTuner + profile presets uphold the negative convention.
        w = ScoringWeights(contradiction_penalty=1.0)
        assert w.contradiction_penalty == 1.0
        s = WorkingSetScores(contradiction_penalty=1.0)
        assert w.weighted_sum(s) == pytest.approx(1.0, abs=1e-9)

    def test_weighted_sum_zero_weights_returns_zero(self):
        """G8: zero out all 11 weights -> sum is 0 regardless of scores."""
        w = ScoringWeights(
            turn_relevance=0.0, session_goal_relevance=0.0,
            global_goal_relevance=0.0, recency=0.0,
            successful_use_prior=0.0, confidence=0.0,
            evidence_strength=0.0, novelty=0.0,
            redundancy_penalty=0.0, contradiction_penalty=0.0,
            cost_penalty=0.0,
        )
        s = WorkingSetScores(turn_relevance=1.0, redundancy_penalty=1.0)
        assert w.weighted_sum(s) == 0.0

    def test_weighted_sum_zero_scores_returns_zero(self):
        """G9: default WorkingSetScores (all zero) against any weights -> 0."""
        w = ScoringWeights()
        s = WorkingSetScores()
        assert w.weighted_sum(s) == 0.0


class TestWorkingSetScores:
    def test_defaults(self):
        s = WorkingSetScores()
        assert s.turn_relevance == 0.0
        assert s.final == 0.0
        assert s.global_goal_relevance == 0.0

    def test_has_11_dimensions_plus_final(self):
        fields = list(WorkingSetScores.model_fields)
        assert len(fields) == 12  # 11 dimensions + final


class TestWorkingSetItem:
    def test_valid_creation(self):
        item = WorkingSetItem(
            id="fact-1", source_type="fact", source_id=uuid.uuid4(), text="Hello",
        )
        assert item.system_prompt_eligible is False
        assert item.must_inject is False
        assert item.token_size == 0

    def test_json_round_trip(self):
        item = WorkingSetItem(
            id="fact-1", source_type="fact", source_id=uuid.uuid4(),
            text="Hello", must_inject=True, system_prompt_eligible=True,
        )
        data = item.model_dump(mode="json")
        restored = WorkingSetItem.model_validate(data)
        assert restored.must_inject is True
        assert restored.system_prompt_eligible is True

    def test_evidence_ref_ids(self):
        ids = [uuid.uuid4(), uuid.uuid4()]
        item = WorkingSetItem(
            id="x", source_type="fact", source_id=uuid.uuid4(),
            text="t", evidence_ref_ids=ids,
        )
        assert len(item.evidence_ref_ids) == 2


class TestWorkingSetSnapshot:
    def test_valid_creation(self):
        snap = WorkingSetSnapshot(session_id=uuid.uuid4(), token_budget=4000)
        assert snap.items == []
        assert snap.tokens_used == 0

    def test_token_budget_non_negative(self):
        with pytest.raises(ValidationError):
            WorkingSetSnapshot(session_id=uuid.uuid4(), token_budget=-1)
