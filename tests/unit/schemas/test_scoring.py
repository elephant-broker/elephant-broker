"""Tests for scoring schemas."""
import pytest
from pydantic import ValidationError

from elephantbroker.schemas.scoring import ScoringDimension, TuningDelta, WeightPreset


class TestScoringDimension:
    def test_all_dimensions(self):
        assert len(ScoringDimension) == 11

    def test_from_string(self):
        assert ScoringDimension("turn_relevance") == ScoringDimension.TURN_RELEVANCE
        assert ScoringDimension("global_goal_relevance") == ScoringDimension.GLOBAL_GOAL_RELEVANCE

    def test_spec_dimensions_present(self):
        expected = {
            "TURN_RELEVANCE", "SESSION_GOAL_RELEVANCE", "GLOBAL_GOAL_RELEVANCE",
            "RECENCY", "SUCCESSFUL_USE_PRIOR", "CONFIDENCE", "EVIDENCE_STRENGTH",
            "NOVELTY", "REDUNDANCY_PENALTY", "CONTRADICTION_PENALTY", "COST_PENALTY",
        }
        assert {d.name for d in ScoringDimension} == expected


class TestWeightPreset:
    def test_valid_creation(self):
        preset = WeightPreset(name="default")
        assert preset.weights == {}

    def test_empty_name_rejected(self):
        with pytest.raises(ValidationError):
            WeightPreset(name="")

    def test_json_round_trip(self):
        preset = WeightPreset(
            name="coding",
            weights={ScoringDimension.TURN_RELEVANCE: 2.0, ScoringDimension.RECENCY: 1.5},
        )
        data = preset.model_dump(mode="json")
        restored = WeightPreset.model_validate(data)
        assert restored.name == "coding"
        assert len(restored.weights) == 2


class TestTuningDelta:
    def test_valid_creation(self):
        td = TuningDelta(dimension=ScoringDimension.NOVELTY, delta=0.1)
        assert td.reason == ""

    def test_json_round_trip(self):
        td = TuningDelta(dimension=ScoringDimension.COST_PENALTY, delta=-0.2, reason="too aggressive")
        data = td.model_dump(mode="json")
        restored = TuningDelta.model_validate(data)
        assert restored.delta == -0.2
