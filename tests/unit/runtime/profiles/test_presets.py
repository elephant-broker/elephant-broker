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

    def test_per_profile_guard_autonomy_matrix(self):
        """TF-07-008: pin the per-profile guard autonomy matrix — preflight
        strictness, default autonomy level, and the three operationally
        critical domain overrides (financial / data_access / code_change).
        Catches accidental relaxation of profile presets that would weaken
        the agent's safety floor.

        Source of truth: ``elephantbroker/runtime/profiles/presets.py`` and
        the architectural decision in ``IMPLEMENTED-Phase-7.md``.
        """
        from elephantbroker.schemas.guards import AutonomyLevel

        expected = {
            # name:           (strictness, default,                 financial,                  data_access,               code_change)
            "coding":             ("medium", AutonomyLevel.INFORM,         AutonomyLevel.HARD_STOP,    AutonomyLevel.APPROVE_FIRST, AutonomyLevel.AUTONOMOUS),
            "research":           ("loose",  AutonomyLevel.INFORM,         AutonomyLevel.APPROVE_FIRST, AutonomyLevel.APPROVE_FIRST, AutonomyLevel.INFORM),
            "managerial":         ("strict", AutonomyLevel.APPROVE_FIRST,  AutonomyLevel.APPROVE_FIRST, AutonomyLevel.APPROVE_FIRST, AutonomyLevel.HARD_STOP),
            "worker":             ("medium", AutonomyLevel.INFORM,         AutonomyLevel.HARD_STOP,    AutonomyLevel.APPROVE_FIRST, AutonomyLevel.AUTONOMOUS),
            "personal_assistant": ("strict", AutonomyLevel.INFORM,         AutonomyLevel.HARD_STOP,    AutonomyLevel.HARD_STOP,    AutonomyLevel.APPROVE_FIRST),
        }
        for name, (strictness, default, fin, data, code) in expected.items():
            assert name in PROFILE_PRESETS, f"missing preset {name}"
            guards = PROFILE_PRESETS[name].guards
            assert guards is not None, f"{name}: guards missing"
            assert guards.preflight_check_strictness == strictness, (
                f"{name}.preflight_check_strictness drifted: "
                f"expected {strictness!r}, got {guards.preflight_check_strictness!r}"
            )
            autonomy = guards.autonomy
            assert autonomy.default_level == default, (
                f"{name}.autonomy.default_level drifted: "
                f"expected {default}, got {autonomy.default_level}"
            )
            assert autonomy.domain_levels.get("financial") == fin, (
                f"{name}.autonomy.financial drifted: "
                f"expected {fin}, got {autonomy.domain_levels.get('financial')}"
            )
            assert autonomy.domain_levels.get("data_access") == data, (
                f"{name}.autonomy.data_access drifted: "
                f"expected {data}, got {autonomy.domain_levels.get('data_access')}"
            )
            assert autonomy.domain_levels.get("code_change") == code, (
                f"{name}.autonomy.code_change drifted: "
                f"expected {code}, got {autonomy.domain_levels.get('code_change')}"
            )

    def test_per_profile_goal_reminder_interval_values(self):
        """TF-06-011 V-profiles: pin the per-profile goal_reminder_interval
        values for the four ``smart``-cadence presets that drive goal injection
        (lifecycle.py:_filter_goals_for_injection compares
        ``turn_count - last_turn >= placement.goal_reminder_interval``).

        Catches accidental drift in operator-tuned cadences. Numbers chosen
        to reflect each profile's working rhythm:
          - coding (5): code-review iteration
          - research (10): longer think windows between reminders
          - worker (3): tight goal-driven feedback for autonomous work
          - personal_assistant (8): assistive cadence

        Managerial intentionally uses ``goal_injection_cadence="always"`` and
        therefore bypasses the interval entirely; its interval value is not
        pinned here because it is dead code for that profile.
        """
        expected = {
            "coding": 5,
            "research": 10,
            "worker": 3,
            "personal_assistant": 8,
        }
        for name, want in expected.items():
            assert name in PROFILE_PRESETS, f"missing preset {name}"
            placement = PROFILE_PRESETS[name].assembly_placement
            assert placement.goal_injection_cadence == "smart", (
                f"{name}: cadence drifted from 'smart' "
                f"(got {placement.goal_injection_cadence}); interval "
                f"assertion below is meaningful only under smart cadence."
            )
            assert placement.goal_reminder_interval == want, (
                f"{name}: goal_reminder_interval drifted "
                f"(expected {want}, got {placement.goal_reminder_interval})"
            )
