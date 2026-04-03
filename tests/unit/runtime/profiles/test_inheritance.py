"""Tests for profile inheritance engine."""
import pytest

from elephantbroker.runtime.profiles.inheritance import ProfileInheritanceEngine
from elephantbroker.runtime.profiles.presets import CODING_PROFILE, PROFILE_PRESETS
from elephantbroker.schemas.profile import ProfilePolicy


@pytest.fixture
def engine():
    return ProfileInheritanceEngine()


class TestProfileInheritance:
    def test_single_level_base_to_coding(self, engine):
        result = engine.flatten(CODING_PROFILE, PROFILE_PRESETS)
        assert result.id == "coding"
        assert result.scoring_weights.turn_relevance == 1.5

    def test_multi_level_custom_extends_coding(self, engine):
        custom = ProfilePolicy(
            id="acme-coding",
            name="Acme Coding",
            extends="coding",
            scoring_weights=CODING_PROFILE.scoring_weights.model_copy(update={"turn_relevance": 2.0}),
            budgets=CODING_PROFILE.budgets,
            compaction=CODING_PROFILE.compaction,
            autorecall=CODING_PROFILE.autorecall,
            retrieval=CODING_PROFILE.retrieval,
            guards=CODING_PROFILE.guards,
            assembly_placement=CODING_PROFILE.assembly_placement,
        )
        presets = {**PROFILE_PRESETS, "acme-coding": custom}
        result = engine.flatten(custom, presets)
        assert result.id == "acme-coding"
        assert result.scoring_weights.turn_relevance == 2.0

    def test_override_replaces_top_level_field(self, engine):
        result = engine.flatten(CODING_PROFILE, PROFILE_PRESETS, org_overrides={"session_data_ttl_seconds": 7200})
        assert result.session_data_ttl_seconds == 7200

    def test_nested_merge_preserves_unspecified_weights(self, engine):
        result = engine.flatten(
            CODING_PROFILE, PROFILE_PRESETS,
            org_overrides={"scoring_weights": {"evidence_strength": 0.99}},
        )
        assert result.scoring_weights.evidence_strength == 0.99
        # Other weights preserved from coding preset
        assert result.scoring_weights.turn_relevance == 1.5

    def test_nested_merge_preserves_unspecified_budgets(self, engine):
        result = engine.flatten(
            CODING_PROFILE, PROFILE_PRESETS,
            org_overrides={"budgets": {"max_prompt_tokens": 16000}},
        )
        assert result.budgets.max_prompt_tokens == 16000
        assert result.budgets.root_top_k == CODING_PROFILE.budgets.root_top_k

    def test_nested_merge_preserves_unspecified_retrieval(self, engine):
        result = engine.flatten(
            CODING_PROFILE, PROFILE_PRESETS,
            org_overrides={"retrieval": {"vector_weight": 0.9}},
        )
        assert result.retrieval.vector_weight == 0.9
        assert result.retrieval.structural_weight == CODING_PROFILE.retrieval.structural_weight

    def test_org_override_on_top_of_resolved(self, engine):
        result = engine.flatten(
            CODING_PROFILE, PROFILE_PRESETS,
            org_overrides={"scoring_weights": {"evidence_strength": 0.9}, "budgets": {"max_prompt_tokens": 10000}},
        )
        assert result.scoring_weights.evidence_strength == 0.9
        assert result.budgets.max_prompt_tokens == 10000

    def test_org_override_only_touches_specified_keys(self, engine):
        original = engine.flatten(CODING_PROFILE, PROFILE_PRESETS)
        overridden = engine.flatten(CODING_PROFILE, PROFILE_PRESETS, org_overrides={"session_data_ttl_seconds": 7200})
        # Scoring weights should be identical
        assert overridden.scoring_weights == original.scoring_weights
        assert overridden.session_data_ttl_seconds == 7200

    def test_no_extends_returns_as_is(self, engine):
        from elephantbroker.runtime.profiles.presets import BASE_PROFILE
        result = engine.flatten(BASE_PROFILE, PROFILE_PRESETS)
        assert result.id == "base"
        assert result.extends is None

    def test_circular_inheritance_raises(self, engine):
        a = ProfilePolicy(
            id="a", name="A", extends="b",
            scoring_weights=CODING_PROFILE.scoring_weights,
            budgets=CODING_PROFILE.budgets, compaction=CODING_PROFILE.compaction,
            autorecall=CODING_PROFILE.autorecall, retrieval=CODING_PROFILE.retrieval,
            guards=CODING_PROFILE.guards, assembly_placement=CODING_PROFILE.assembly_placement,
        )
        b = ProfilePolicy(
            id="b", name="B", extends="a",
            scoring_weights=CODING_PROFILE.scoring_weights,
            budgets=CODING_PROFILE.budgets, compaction=CODING_PROFILE.compaction,
            autorecall=CODING_PROFILE.autorecall, retrieval=CODING_PROFILE.retrieval,
            guards=CODING_PROFILE.guards, assembly_placement=CODING_PROFILE.assembly_placement,
        )
        presets = {"a": a, "b": b}
        with pytest.raises(ValueError, match="Circular"):
            engine.flatten(a, presets)

    def test_deep_merge_autorecall_policy(self, engine):
        result = engine.flatten(
            CODING_PROFILE, PROFILE_PRESETS,
            org_overrides={"autorecall": {"superseded_confidence_factor": 0.99}},
        )
        assert result.autorecall.superseded_confidence_factor == 0.99
        assert result.autorecall.extraction_focus == CODING_PROFILE.autorecall.extraction_focus

    def test_unknown_override_key_ignored(self, engine):
        # Should not raise, just log warning
        result = engine.flatten(CODING_PROFILE, PROFILE_PRESETS, org_overrides={"nonexistent_field": 42})
        assert result.id == "coding"

    def test_unknown_nested_key_ignored(self, engine):
        result = engine.flatten(
            CODING_PROFILE, PROFILE_PRESETS,
            org_overrides={"scoring_weights": {"nonexistent_dimension": 0.5}},
        )
        assert result.scoring_weights.turn_relevance == 1.5

    def test_result_is_deep_copy(self, engine):
        r1 = engine.flatten(CODING_PROFILE, PROFILE_PRESETS)
        r2 = engine.flatten(CODING_PROFILE, PROFILE_PRESETS)
        r1.scoring_weights.turn_relevance = 999.0
        assert r2.scoring_weights.turn_relevance == 1.5
