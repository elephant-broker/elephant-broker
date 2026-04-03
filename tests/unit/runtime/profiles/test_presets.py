"""Tests for profile preset definitions."""
from elephantbroker.runtime.profiles.presets import (
    BASE_PROFILE,
    CODING_PROFILE,
    MANAGERIAL_PROFILE,
    PERSONAL_ASSISTANT_PROFILE,
    PROFILE_PRESETS,
    RESEARCH_PROFILE,
    WORKER_PROFILE,
)
from elephantbroker.schemas.profile import GraphMode, IsolationLevel


class TestProfilePresets:
    def test_all_6_profiles_defined(self):
        assert len(PROFILE_PRESETS) == 6
        assert set(PROFILE_PRESETS.keys()) == {"base", "coding", "research", "managerial", "worker", "personal_assistant"}

    def test_base_has_no_extends(self):
        assert BASE_PROFILE.extends is None
        assert BASE_PROFILE.id == "base"

    def test_all_named_profiles_extend_base(self):
        for name in ["coding", "research", "managerial", "worker", "personal_assistant"]:
            assert PROFILE_PRESETS[name].extends == "base", f"{name} should extend base"

    def test_coding_weights_match_spec(self):
        w = CODING_PROFILE.scoring_weights
        assert w.turn_relevance == 1.5
        assert w.session_goal_relevance == 1.2
        assert w.global_goal_relevance == 0.3
        assert w.recency == 1.2
        assert w.recency_half_life_hours == 24.0
        assert w.evidence_refs_for_max_score == 2

    def test_research_weights_match_spec(self):
        w = RESEARCH_PROFILE.scoring_weights
        assert w.turn_relevance == 0.8
        assert w.evidence_strength == 0.9
        assert w.recency_half_life_hours == 168.0
        assert w.evidence_refs_for_max_score == 5

    def test_managerial_weights_match_spec(self):
        w = MANAGERIAL_PROFILE.scoring_weights
        assert w.session_goal_relevance == 1.5
        assert w.global_goal_relevance == 1.0
        assert w.recency_half_life_hours == 72.0

    def test_worker_weights_match_spec(self):
        w = WORKER_PROFILE.scoring_weights
        assert w.turn_relevance == 1.3
        assert w.session_goal_relevance == 1.4
        assert w.recency_half_life_hours == 12.0

    def test_personal_assistant_weights_match_spec(self):
        w = PERSONAL_ASSISTANT_PROFILE.scoring_weights
        assert w.successful_use_prior == 0.9
        assert w.recency_half_life_hours == 720.0
        assert w.contradiction_confidence_gap == 0.35

    def test_coding_graph_mode_local(self):
        assert CODING_PROFILE.graph_mode == GraphMode.LOCAL

    def test_research_graph_mode_hybrid(self):
        assert RESEARCH_PROFILE.graph_mode == GraphMode.HYBRID
        assert RESEARCH_PROFILE.retrieval.graph_mode == GraphMode.GLOBAL

    def test_worker_graph_mode_local(self):
        assert WORKER_PROFILE.graph_mode == GraphMode.LOCAL
        assert WORKER_PROFILE.retrieval.graph_max_depth == 1

    def test_personal_assistant_isolation_strict(self):
        assert PERSONAL_ASSISTANT_PROFILE.retrieval.isolation_level == IsolationLevel.STRICT

    def test_all_presets_have_guards(self):
        for name, p in PROFILE_PRESETS.items():
            assert p.guards is not None, f"{name} missing guards"
            assert p.guards.autonomy is not None, f"{name} missing autonomy"

    def test_all_presets_have_assembly_placement(self):
        for name, p in PROFILE_PRESETS.items():
            assert p.assembly_placement is not None, f"{name} missing assembly_placement"

    def test_all_presets_have_session_data_ttl(self):
        for name, p in PROFILE_PRESETS.items():
            assert p.session_data_ttl_seconds > 0, f"{name} missing session_data_ttl_seconds"
