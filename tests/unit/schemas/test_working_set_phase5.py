"""Tests for Phase 5 working set schema additions."""
import uuid
from datetime import UTC, datetime

import pytest

from elephantbroker.schemas.config import ConflictDetectionConfig, ScoringConfig, VerificationMultipliers
from elephantbroker.schemas.fact import FactAssertion
from elephantbroker.schemas.procedure import ProcedureDefinition
from elephantbroker.schemas.working_set import (
    ScoringContext,
    ScoringWeights,
    WorkingSetItem,
    WorkingSetScores,
)


class TestScoringWeightsPhase5:
    def test_new_fields_have_defaults(self):
        w = ScoringWeights()
        assert w.recency_half_life_hours == 69.0
        assert w.evidence_refs_for_max_score == 3
        assert w.redundancy_similarity_threshold == 0.85
        assert w.contradiction_similarity_threshold == 0.9
        assert w.contradiction_confidence_gap == 0.3

    def test_custom_half_life(self):
        w = ScoringWeights(recency_half_life_hours=24.0)
        assert w.recency_half_life_hours == 24.0

    def test_weighted_sum_still_works(self):
        w = ScoringWeights(turn_relevance=2.0, cost_penalty=-1.0)
        scores = WorkingSetScores(turn_relevance=0.8, cost_penalty=0.5)
        result = w.weighted_sum(scores)
        assert abs(result - (2.0 * 0.8 + (-1.0) * 0.5)) < 1e-6


class TestWorkingSetItemPhase5:
    def test_metadata_defaults(self):
        item = WorkingSetItem(
            id="test", source_type="fact", source_id=uuid.uuid4(), text="test",
        )
        assert item.confidence == 1.0
        assert item.use_count == 0
        assert item.successful_use_count == 0
        assert item.created_at is None
        assert item.updated_at is None
        assert item.last_used_at is None
        assert item.category == "general"
        assert item.goal_ids == []
        assert item.goal_relevance_tags == {}

    def test_metadata_populated(self):
        now = datetime.now(UTC)
        gid = uuid.uuid4()
        item = WorkingSetItem(
            id="test", source_type="fact", source_id=uuid.uuid4(), text="test",
            confidence=0.8, use_count=5, successful_use_count=3,
            created_at=now, category="decision",
            goal_ids=[gid], goal_relevance_tags={"goal1": "direct"},
        )
        assert item.confidence == 0.8
        assert item.use_count == 5
        assert item.successful_use_count == 3
        assert item.category == "decision"
        assert item.goal_ids == [gid]
        assert item.goal_relevance_tags == {"goal1": "direct"}


class TestScoringContext:
    def test_defaults(self):
        ctx = ScoringContext()
        assert ctx.turn_text == ""
        assert ctx.turn_embedding == []
        assert ctx.session_goals == []
        assert ctx.global_goals == []
        assert ctx.goal_embeddings == {}
        assert ctx.compact_state_ids == set()
        assert ctx.token_budget == 8000
        assert ctx.evidence_index == {}
        assert ctx.verification_index == {}
        assert ctx.conflict_pairs == set()
        assert isinstance(ctx.weights, ScoringWeights)
        assert isinstance(ctx.verification_multipliers, VerificationMultipliers)
        assert isinstance(ctx.conflict_config, ConflictDetectionConfig)
        assert isinstance(ctx.scoring_config, ScoringConfig)

    def test_custom_values(self):
        ctx = ScoringContext(
            turn_text="query",
            turn_embedding=[0.1, 0.2],
            token_budget=4000,
            evidence_index={"fact1": 3},
        )
        assert ctx.turn_text == "query"
        assert ctx.turn_embedding == [0.1, 0.2]
        assert ctx.token_budget == 4000
        assert ctx.evidence_index["fact1"] == 3


class TestFactAssertionGoalRelevanceTags:
    def test_default_empty(self):
        f = FactAssertion(text="test")
        assert f.goal_relevance_tags == {}

    def test_populated(self):
        f = FactAssertion(text="test", goal_relevance_tags={"g1": "direct", "g2": "indirect"})
        assert f.goal_relevance_tags["g1"] == "direct"


class TestProcedureDefinitionEnabled:
    def test_default_enabled(self):
        p = ProcedureDefinition(name="test", is_manual_only=True)
        assert p.enabled is True

    def test_disabled(self):
        p = ProcedureDefinition(name="test", enabled=False, is_manual_only=True)
        assert p.enabled is False
