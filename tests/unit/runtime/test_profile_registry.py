"""Tests for ProfileRegistry."""
import pytest

from elephantbroker.runtime.profiles.registry import ProfileRegistry
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.schemas.profile import GraphMode


class TestProfileRegistry:
    def _make(self):
        return ProfileRegistry(TraceLedger())

    async def test_resolve_coding_profile(self):
        reg = self._make()
        p = await reg.resolve_profile("coding")
        assert p.id == "coding"
        assert p.graph_mode == GraphMode.LOCAL

    async def test_resolve_research_profile(self):
        reg = self._make()
        p = await reg.resolve_profile("research")
        assert p.id == "research"
        assert p.graph_mode == GraphMode.HYBRID

    async def test_all_five_presets_available(self):
        reg = self._make()
        for name in ("coding", "research", "managerial", "worker", "personal_assistant"):
            p = await reg.resolve_profile(name)
            assert p.id == name

    async def test_coding_weights_match_spec(self):
        reg = self._make()
        w = await reg.get_scoring_weights("coding")
        assert w.turn_relevance == 1.5
        assert w.session_goal_relevance == 1.2
        assert w.recency == 1.2
        assert w.contradiction_penalty == -1.0

    async def test_unknown_profile_raises(self):
        reg = self._make()
        with pytest.raises(KeyError):
            await reg.resolve_profile("nonexistent")

    async def test_get_scoring_weights(self):
        reg = self._make()
        w = await reg.get_scoring_weights("research")
        assert w.evidence_strength == 0.9
        assert w.confidence == 0.8


class TestProfileRegistryRetrievalPolicies:
    def _make(self):
        return ProfileRegistry(TraceLedger())

    async def test_all_presets_have_retrieval_policy(self):
        from elephantbroker.schemas.profile import RetrievalPolicy
        registry = self._make()
        for name in ["coding", "research", "managerial", "worker", "personal_assistant"]:
            policy = await registry.resolve_profile(name)
            assert isinstance(policy.retrieval, RetrievalPolicy)

    async def test_all_presets_have_extended_autorecall(self):
        registry = self._make()
        for name in ["coding", "research", "managerial", "worker", "personal_assistant"]:
            policy = await registry.resolve_profile(name)
            assert len(policy.autorecall.extraction_focus) > 0
            assert policy.autorecall.superseded_confidence_factor >= 0

    async def test_coding_retrieval_weights(self):
        registry = self._make()
        p = await registry.resolve_profile("coding")
        assert p.retrieval.structural_weight == 0.5
        assert p.retrieval.keyword_weight == 0.4
        assert p.retrieval.vector_weight == 0.3

    async def test_research_retrieval_weights(self):
        registry = self._make()
        p = await registry.resolve_profile("research")
        assert p.retrieval.vector_weight == 0.5
        assert p.retrieval.graph_expansion_weight == 0.3
        assert p.retrieval.structural_weight == 0.3
        assert p.retrieval.keyword_weight == 0.2

    async def test_managerial_graph_depth(self):
        registry = self._make()
        p = await registry.resolve_profile("managerial")
        assert p.retrieval.graph_max_depth == 2
        assert p.retrieval.graph_expansion_weight == 0.4

    async def test_personal_assistant_retrieval_weights(self):
        registry = self._make()
        p = await registry.resolve_profile("personal_assistant")
        assert p.retrieval.vector_weight == 0.5
        assert p.retrieval.graph_expansion_weight == 0.2
        assert p.retrieval.graph_max_depth == 2

    async def test_coding_extraction_focus(self):
        registry = self._make()
        p = await registry.resolve_profile("coding")
        assert "code decisions" in p.autorecall.extraction_focus

    async def test_research_cadence_is_minimal(self):
        registry = self._make()
        p = await registry.resolve_profile("research")
        assert p.compaction.cadence == "minimal"

    async def test_coding_autorecall_retrieval_fast(self):
        registry = self._make()
        p = await registry.resolve_profile("coding")
        assert p.autorecall.retrieval.vector_enabled is False
        assert p.autorecall.retrieval.graph_expansion_enabled is False

    async def test_personal_assistant_strict_isolation(self):
        registry = self._make()
        p = await registry.resolve_profile("personal_assistant")
        from elephantbroker.schemas.profile import IsolationLevel
        assert p.retrieval.isolation_level == IsolationLevel.STRICT
