"""Tests for profile schemas."""
import pytest
from pydantic import ValidationError

from elephantbroker.schemas.profile import (
    AutorecallPolicy,
    Budgets,
    CompactionPolicy,
    GraphMode,
    GuardPolicy,
    ProfilePolicy,
    VerificationPolicy,
)
from elephantbroker.schemas.working_set import ScoringWeights


class TestGraphMode:
    def test_all_modes(self):
        assert len(GraphMode) == 3
        assert GraphMode.LOCAL == "local"
        assert GraphMode.HYBRID == "hybrid"
        assert GraphMode.GLOBAL == "global"


class TestCompactionPolicy:
    def test_defaults(self):
        p = CompactionPolicy()
        assert p.cadence == "balanced"
        assert p.target_tokens == 4000
        assert p.preserve_goal_state is True
        assert p.preserve_open_questions is True
        assert p.preserve_evidence_refs is True

    def test_target_tokens_min(self):
        with pytest.raises(ValidationError):
            CompactionPolicy(target_tokens=50)


class TestAutorecallPolicy:
    def test_defaults(self):
        p = AutorecallPolicy()
        assert p.enabled is True
        assert p.require_successful_use_prior is False
        assert p.require_not_in_compact_state is True


class TestVerificationPolicy:
    def test_defaults(self):
        p = VerificationPolicy()
        assert p.proof_required_for_completion is False
        assert p.supervisor_sampling_rate == 0.0

    def test_sampling_rate_bounds(self):
        with pytest.raises(ValidationError):
            VerificationPolicy(supervisor_sampling_rate=1.5)


class TestGuardPolicy:
    def test_defaults(self):
        p = GuardPolicy()
        assert p.force_system_constraint_injection is True
        assert p.preflight_check_strictness == "medium"


class TestBudgets:
    def test_defaults(self):
        b = Budgets()
        assert b.mem0_fetch_k == 20
        assert b.graph_fetch_k == 15
        assert b.artifact_fetch_k == 10
        assert b.final_prompt_k == 30
        assert b.root_top_k == 40
        assert b.max_prompt_tokens == 8000
        assert b.max_system_overlay_tokens == 1500
        assert b.subagent_packet_tokens == 3000

    def test_min_bounds(self):
        with pytest.raises(ValidationError):
            Budgets(max_prompt_tokens=50)  # min 100


class TestProfilePolicy:
    def test_valid_creation(self):
        p = ProfilePolicy(id="coding", name="Coding")
        assert p.budgets.max_prompt_tokens == 8000
        assert p.extends is None
        assert p.graph_mode == GraphMode.HYBRID

    def test_empty_name_rejected(self):
        with pytest.raises(ValidationError):
            ProfilePolicy(id="x", name="")

    def test_empty_id_rejected(self):
        with pytest.raises(ValidationError):
            ProfilePolicy(id="", name="X")

    def test_json_round_trip(self):
        p = ProfilePolicy(id="research", name="Research")
        data = p.model_dump(mode="json")
        restored = ProfilePolicy.model_validate(data)
        assert restored.id == "research"
        assert restored.name == "Research"
        assert restored.budgets == p.budgets

    def test_scoring_weights_included(self):
        p = ProfilePolicy(id="x", name="X")
        assert isinstance(p.scoring_weights, ScoringWeights)
        assert p.scoring_weights.turn_relevance == 1.0

    def test_extends_field(self):
        p = ProfilePolicy(id="custom", name="Custom", extends="coding")
        assert p.extends == "coding"

    def test_graph_mode(self):
        p = ProfilePolicy(id="x", name="X", graph_mode=GraphMode.GLOBAL)
        assert p.graph_mode == GraphMode.GLOBAL


class TestIsolationLevel:
    def test_values(self):
        from elephantbroker.schemas.profile import IsolationLevel
        assert IsolationLevel.NONE == "none"
        assert IsolationLevel.LOOSE == "loose"
        assert IsolationLevel.STRICT == "strict"

class TestIsolationScope:
    def test_values(self):
        from elephantbroker.schemas.profile import IsolationScope
        assert IsolationScope.GLOBAL == "global"
        assert IsolationScope.SESSION_KEY == "session_key"
        assert IsolationScope.ACTOR == "actor"
        assert IsolationScope.SUBAGENT_INHERIT == "subagent_inherit"

class TestRetrievalPolicy:
    def test_defaults(self):
        from elephantbroker.schemas.profile import RetrievalPolicy, GraphMode
        p = RetrievalPolicy()
        assert p.structural_enabled is True
        assert p.keyword_enabled is True
        assert p.vector_enabled is True
        assert p.graph_expansion_enabled is True
        assert p.artifact_enabled is True
        assert p.root_top_k == 40
        assert p.structural_fetch_k == 20
        assert p.keyword_fetch_k == 15
        assert p.structural_weight == 0.4
        assert p.vector_weight == 0.5
        assert p.graph_max_depth == 2
        assert p.graph_mode == GraphMode.HYBRID

    def test_weight_constraints(self):
        from elephantbroker.schemas.profile import RetrievalPolicy
        # Weights are ge=0.0 with no upper bound per plan
        p = RetrievalPolicy(structural_weight=1.5)
        assert p.structural_weight == 1.5
        with pytest.raises(ValidationError):
            RetrievalPolicy(structural_weight=-0.1)

    def test_serialization(self):
        from elephantbroker.schemas.profile import RetrievalPolicy
        p = RetrievalPolicy(structural_weight=0.7, keyword_enabled=False)
        data = p.model_dump()
        restored = RetrievalPolicy.model_validate(data)
        assert restored.structural_weight == 0.7
        assert restored.keyword_enabled is False

class TestAutorecallPolicyExtended:
    def test_new_fields_defaults(self):
        from elephantbroker.schemas.profile import AutorecallPolicy
        a = AutorecallPolicy()
        assert a.auto_recall_injection_top_k == 10
        assert a.min_similarity == 0.3
        assert a.extraction_max_facts_per_batch_before_dedup == 5
        assert a.dedup_similarity == 0.95
        assert a.extraction_focus == []
        assert a.custom_categories == []
        assert a.superseded_confidence_factor == 0.3

    def test_retrieval_on_autorecall(self):
        from elephantbroker.schemas.profile import AutorecallPolicy, RetrievalPolicy
        a = AutorecallPolicy()
        assert isinstance(a.retrieval, RetrievalPolicy)

    def test_validation(self):
        from elephantbroker.schemas.profile import AutorecallPolicy
        with pytest.raises(ValidationError):
            AutorecallPolicy(dedup_similarity=1.5)

class TestProfilePolicyWithRetrieval:
    def test_retrieval_on_profile(self):
        from elephantbroker.schemas.profile import ProfilePolicy, RetrievalPolicy
        p = ProfilePolicy(id="test", name="Test")
        assert isinstance(p.retrieval, RetrievalPolicy)

    def test_serialization_with_retrieval(self):
        from elephantbroker.schemas.profile import ProfilePolicy, RetrievalPolicy
        p = ProfilePolicy(id="test", name="Test", retrieval=RetrievalPolicy(structural_weight=0.8))
        data = p.model_dump(mode="json")
        restored = ProfilePolicy.model_validate(data)
        assert restored.retrieval.structural_weight == 0.8
