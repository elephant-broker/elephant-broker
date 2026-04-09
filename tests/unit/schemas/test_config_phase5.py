"""Tests for Phase 5 config schema additions."""
import pytest

from elephantbroker.schemas.config import (
    AuditConfig,
    BlockerExtractionConfig,
    CompactionLLMConfig,
    ConflictDetectionConfig,
    EmbeddingCacheConfig,
    ElephantBrokerConfig,
    GoalInjectionConfig,
    GoalRefinementConfig,
    ProcedureCandidateConfig,
    RerankerConfig,
    ScoringConfig,
    SuccessfulUseConfig,
    VerificationMultipliers,
)


class TestEmbeddingCacheConfig:
    def test_defaults(self):
        c = EmbeddingCacheConfig()
        assert c.enabled is True
        assert c.ttl_seconds == 3600
        assert c.key_prefix == "eb:emb_cache"

    def test_ttl_minimum(self):
        with pytest.raises(Exception):
            EmbeddingCacheConfig(ttl_seconds=10)


class TestScoringConfig:
    def test_defaults(self):
        c = ScoringConfig()
        assert c.neutral_use_prior == 0.5
        assert c.cheap_prune_max_candidates == 80
        assert c.semantic_blend_weight == 0.6
        assert c.merge_similarity_threshold == 0.95
        assert c.snapshot_ttl_seconds == 300
        assert c.session_goals_ttl_seconds == 86400
        assert c.working_set_build_global_goals_filter_by_actors is True


class TestVerificationMultipliers:
    def test_defaults(self):
        v = VerificationMultipliers()
        assert v.supervisor_verified == 1.0
        assert v.tool_supported == 0.9
        assert v.self_supported == 0.7
        assert v.unverified == 0.5
        assert v.no_claim == 0.8

    def test_custom_values(self):
        v = VerificationMultipliers(supervisor_verified=1.5, unverified=0.3)
        assert v.supervisor_verified == 1.5
        assert v.unverified == 0.3


class TestConflictDetectionConfig:
    def test_defaults(self):
        c = ConflictDetectionConfig()
        assert c.supersession_penalty == 1.0
        assert c.contradiction_edge_penalty == 0.9
        assert c.layer2_penalty == 0.7


class TestGoalInjectionConfig:
    def test_defaults(self):
        c = GoalInjectionConfig()
        assert c.enabled is True
        assert c.max_session_goals == 5
        assert c.max_persistent_goals == 3
        assert c.include_persistent_goals is True


class TestGoalRefinementConfig:
    def test_defaults(self):
        c = GoalRefinementConfig()
        assert c.hints_enabled is True
        assert c.refinement_task_enabled is True
        assert c.model == "gemini/gemini-2.5-flash-lite"
        assert c.max_subgoals_per_session == 10
        assert c.progress_confidence_delta == 0.1
        assert c.subgoal_dedup_threshold == 0.6


class TestProcedureCandidateConfig:
    def test_defaults(self):
        c = ProcedureCandidateConfig()
        assert c.enabled is True
        assert c.filter_by_relevance is True
        assert c.relevance_threshold == 0.3
        assert c.top_k == 3
        assert c.always_include_proof_required is True


class TestAuditConfig:
    def test_defaults(self):
        c = AuditConfig()
        assert c.procedure_audit_enabled is True
        assert c.session_goal_audit_enabled is True


class TestRerankerConfigExtensions:
    def test_new_defaults(self):
        c = RerankerConfig()
        assert c.enabled is True
        assert c.timeout_seconds == 10.0
        assert c.batch_size == 32
        assert c.max_documents == 100
        assert c.fallback_on_error is True
        assert c.top_n is None


class TestSuccessfulUseConfig:
    def test_defaults(self):
        c = SuccessfulUseConfig()
        assert c.enabled is False
        assert c.model == "gemini/gemini-2.5-flash-lite"


class TestFlashLiteModelDefaults:
    """Regression for Task #36 — every flash-class config default must point at
    the working flash-lite alias. The unsuffixed "gemini/gemini-2.5-flash"
    alias on the staging LiteLLM proxy resolves to a deleted Gemini preview
    and returns HTTP 404. This test pins all four configs so a future
    operator cannot silently revert one of them."""

    def test_all_flash_configs_use_flash_lite(self):
        expected = "gemini/gemini-2.5-flash-lite"
        assert SuccessfulUseConfig().model == expected
        assert GoalRefinementConfig().model == expected
        assert CompactionLLMConfig().model == expected
        assert BlockerExtractionConfig().model == expected

    def test_default_yaml_matches_python_defaults(self, monkeypatch):
        """Pin YAML ↔ Python-default drift. Task #36 landed after 1e0cb47
        changed GoalRefinementConfig.model in the Python schema but forgot
        to update default.yaml — YAML values win at load time, so the fix
        was silently ignored on any deployment that ships default.yaml (i.e.
        every deployment). This regression test asserts that the packaged
        default.yaml's four flash-class models all resolve to flash-lite
        when loaded end-to-end."""
        # Strip env overrides so only the YAML file drives the load.
        for key in list(
            [
                "EB_COMPACTION_LLM_MODEL",
                "EB_SUCCESSFUL_USE_MODEL",
                "EB_BLOCKER_EXTRACTION_MODEL",
            ]
        ):
            monkeypatch.delenv(key, raising=False)
        config = ElephantBrokerConfig.load()
        expected = "gemini/gemini-2.5-flash-lite"
        assert config.successful_use.model == expected
        assert config.goal_refinement.model == expected
        assert config.compaction_llm.model == expected
        assert config.blocker_extraction.model == expected


class TestElephantBrokerConfigPhase5:
    def test_has_all_phase5_sections(self):
        config = ElephantBrokerConfig()
        assert isinstance(config.embedding_cache, EmbeddingCacheConfig)
        assert isinstance(config.scoring, ScoringConfig)
        assert isinstance(config.verification_multipliers, VerificationMultipliers)
        assert isinstance(config.conflict_detection, ConflictDetectionConfig)
        assert isinstance(config.successful_use, SuccessfulUseConfig)
        assert isinstance(config.goal_injection, GoalInjectionConfig)
        assert isinstance(config.goal_refinement, GoalRefinementConfig)
        assert isinstance(config.procedure_candidates, ProcedureCandidateConfig)
        assert isinstance(config.audit, AuditConfig)

    def test_json_roundtrip_with_phase5(self):
        config = ElephantBrokerConfig()
        data = config.model_dump(mode="json")
        restored = ElephantBrokerConfig(**data)
        assert restored.scoring.neutral_use_prior == 0.5
        assert restored.verification_multipliers.supervisor_verified == 1.0
