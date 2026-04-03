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
