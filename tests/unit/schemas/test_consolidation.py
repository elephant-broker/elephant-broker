"""Tests for consolidation schemas."""
import uuid

from elephantbroker.schemas.consolidation import (
    CanonicalResult,
    ConsolidationConfig,
    ConsolidationContext,
    ConsolidationReport,
    ConsolidationSummary,
    DecayResult,
    DomainSuggestion,
    DuplicateCluster,
    PromotionResult,
    StageResult,
    StrengthenResult,
)


class TestConsolidationConfig:
    def test_defaults(self):
        cfg = ConsolidationConfig()
        assert cfg.batch_size == 500
        assert cfg.cluster_similarity_threshold == 0.92
        assert cfg.ema_alpha == 0.3
        assert cfg.max_weight_adjustment_pct == 0.05
        assert cfg.llm_calls_per_run_cap == 50

    def test_scope_multipliers_default(self):
        cfg = ConsolidationConfig()
        assert cfg.decay_scope_multipliers["session"] == 1.5
        assert cfg.decay_scope_multipliers["global"] == 0.5


class TestDuplicateCluster:
    def test_creation(self):
        c = DuplicateCluster(
            fact_ids=["a", "b"], canonical_candidate_id="a",
            avg_similarity=0.95, session_keys=["s1", "s2"],
        )
        assert len(c.fact_ids) == 2
        assert isinstance(c.cluster_id, uuid.UUID)


class TestCanonicalResult:
    def test_creation(self):
        r = CanonicalResult(
            cluster_id=uuid.uuid4(), new_canonical_fact_id="new",
            canonical_text="merged text", archived_fact_ids=["a", "b"],
            merged_provenance=[], merged_use_count=5,
            merged_successful_use_count=3, merged_goal_ids=[],
        )
        assert r.llm_used is False


class TestStrengthenResult:
    def test_creation(self):
        r = StrengthenResult(fact_id="f1", old_confidence=0.5, new_confidence=0.7, success_ratio=0.8, boosted=True)
        assert r.boosted


class TestDecayResult:
    def test_creation(self):
        r = DecayResult(fact_id="f1", old_confidence=0.8, new_confidence=0.5, decay_reason="recalled_unused", archived=False)
        assert not r.archived


class TestPromotionResult:
    def test_creation(self):
        r = PromotionResult(
            fact_id="f1", old_memory_class="episodic", new_memory_class="semantic",
            old_scope="session", new_scope="actor", reason="recurring_with_goal", sessions_seen=5,
        )
        assert r.sessions_seen == 5


class TestConsolidationContext:
    def test_facts_field(self):
        ctx = ConsolidationContext(org_id="org", gateway_id="gw")
        assert ctx.facts == []
        assert ctx.llm_calls_cap == 50

    def test_clusters_field(self):
        ctx = ConsolidationContext(org_id="org", gateway_id="gw")
        assert ctx.clusters == []


class TestConsolidationReport:
    def test_defaults(self):
        r = ConsolidationReport(org_id="org", gateway_id="gw")
        assert r.status == "running"
        assert r.error is None

    def test_json_round_trip(self):
        r = ConsolidationReport(org_id="org", gateway_id="gw", profile_id="coding")
        data = r.model_dump(mode="json")
        restored = ConsolidationReport.model_validate(data)
        assert restored.org_id == "org"


class TestDomainSuggestion:
    def test_creation(self):
        s = DomainSuggestion(
            action_target="test_tool", suggested_domain="financial",
            occurrences=10, similarity_to_existing=0.85,
        )
        assert s.gateway_id == ""
