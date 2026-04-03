"""Tests for RetrievalOrchestrator."""
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from elephantbroker.runtime.interfaces.retrieval import RetrievalCandidate
from elephantbroker.runtime.retrieval.orchestrator import RetrievalOrchestrator
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.schemas.fact import MemoryClass
from elephantbroker.schemas.profile import IsolationLevel, IsolationScope, RetrievalPolicy
from tests.fixtures.factories import make_fact_assertion


def _fact_props(fact=None, **overrides):
    fid = str(fact.id) if fact else str(uuid.uuid4())
    text = fact.text if fact else "test"
    base = {
        "eb_id": fid, "text": text, "category": "general",
        "scope": "session", "confidence": 1.0, "memory_class": "episodic",
        "eb_created_at": 0, "eb_updated_at": 0, "use_count": 0,
        "successful_use_count": 0, "provenance_refs": [], "target_actor_ids": [],
        "goal_ids": [],
    }
    base.update(overrides)
    return base


class TestRetrievalOrchestrator:
    def _make(self):
        vector = AsyncMock()
        graph = AsyncMock()
        graph.query_cypher = AsyncMock(return_value=[])
        graph.get_entity = AsyncMock(return_value=None)
        embeddings = AsyncMock()
        embeddings.embed_text = AsyncMock(return_value=[0.1] * 1024)
        ledger = TraceLedger()
        return RetrievalOrchestrator(vector, graph, embeddings, ledger, dataset_name="test_ds"), vector, graph

    async def test_retrieve_candidates(self, monkeypatch, mock_cognee):
        orch, vector, graph = self._make()
        monkeypatch.setattr("elephantbroker.runtime.retrieval.orchestrator.cognee", mock_cognee)
        vector.search_similar = AsyncMock(return_value=[])
        results = await orch.retrieve_candidates("test")
        assert isinstance(results, list)

    async def test_get_exact_hits(self, monkeypatch, mock_cognee):
        orch, _, graph = self._make()
        monkeypatch.setattr("elephantbroker.runtime.retrieval.orchestrator.cognee", mock_cognee)
        results = await orch.get_exact_hits("test")
        assert results == []

    async def test_get_semantic_hits(self, monkeypatch, mock_cognee):
        orch, vector, graph = self._make()
        monkeypatch.setattr("elephantbroker.runtime.retrieval.orchestrator.cognee", mock_cognee)
        vector.search_similar = AsyncMock(return_value=[])
        results = await orch.get_semantic_hits("test")
        assert results == []

    async def test_get_exact_hits_returns_facts(self, monkeypatch, mock_cognee):
        orch, _, graph = self._make()
        monkeypatch.setattr("elephantbroker.runtime.retrieval.orchestrator.cognee", mock_cognee)
        graph.query_cypher = AsyncMock(return_value=[{"props": _fact_props(text="exact"), "relations": []}])
        results = await orch.get_exact_hits("exact")
        assert len(results) == 1
        assert results[0].text == "exact"

    async def test_get_semantic_hits_returns_facts(self, monkeypatch, mock_cognee):
        orch, vector, graph = self._make()
        fact_id = str(uuid.uuid4())
        props = _fact_props(text="semantic hit", eb_id=fact_id)
        mock_cognee.search = AsyncMock(return_value=[props])
        monkeypatch.setattr("elephantbroker.runtime.retrieval.orchestrator.cognee", mock_cognee)
        results = await orch.get_semantic_hits("semantic")
        assert len(results) == 1

    async def test_get_semantic_hits_fallback(self, monkeypatch, mock_cognee):
        orch, vector, graph = self._make()
        mock_cognee.search = AsyncMock(side_effect=RuntimeError("fail"))
        monkeypatch.setattr("elephantbroker.runtime.retrieval.orchestrator.cognee", mock_cognee)
        vector.search_similar = AsyncMock(return_value=[])
        results = await orch.get_semantic_hits("test")
        assert results == []


class TestRetrievalOrchestratorPhase4:
    """Phase 4: 5-source retrieval with weighted merge, isolation, dedup."""

    def _make(self):
        vector = AsyncMock()
        graph = AsyncMock()
        graph.query_cypher = AsyncMock(return_value=[])
        graph.get_entity = AsyncMock(return_value=None)
        vector.search_similar = AsyncMock(return_value=[])
        embeddings = AsyncMock()
        embeddings.embed_text = AsyncMock(return_value=[0.1] * 1024)
        ledger = TraceLedger()
        return RetrievalOrchestrator(vector, graph, embeddings, ledger, dataset_name="test_ds"), graph, vector, embeddings

    async def test_structural_hits_by_session_key(self, monkeypatch, mock_cognee):
        orch, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.retrieval.orchestrator.cognee", mock_cognee)
        fact = make_fact_assertion(session_key="sk1")
        graph.query_cypher = AsyncMock(return_value=[
            {"props": _fact_props(fact, session_key="sk1"), "relations": []}
        ])
        results = await orch.get_structural_hits(session_key="sk1")
        cypher = graph.query_cypher.call_args[0][0]
        assert "f.session_key = $session_key" in cypher
        assert len(results) == 1

    async def test_structural_hits_by_memory_class(self, monkeypatch, mock_cognee):
        orch, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.retrieval.orchestrator.cognee", mock_cognee)
        await orch.get_structural_hits(memory_class=MemoryClass.SEMANTIC)
        cypher = graph.query_cypher.call_args[0][0]
        assert "f.memory_class = $memory_class" in cypher

    async def test_structural_score_is_1(self, monkeypatch, mock_cognee):
        orch, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.retrieval.orchestrator.cognee", mock_cognee)
        fact = make_fact_assertion()
        graph.query_cypher = AsyncMock(return_value=[
            {"props": _fact_props(fact), "relations": []}
        ])
        results = await orch.get_structural_hits()
        assert results[0].score == 1.0

    async def test_structural_returns_relations(self, monkeypatch, mock_cognee):
        orch, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.retrieval.orchestrator.cognee", mock_cognee)
        fact = make_fact_assertion()
        graph.query_cypher = AsyncMock(return_value=[
            {"props": _fact_props(fact), "relations": [{"type": "CREATED_BY"}]}
        ])
        results = await orch.get_structural_hits()
        assert len(results[0].relations) == 1

    async def test_keyword_hits_via_cognee(self, monkeypatch, mock_cognee):
        orch, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.retrieval.orchestrator.cognee", mock_cognee)
        await orch.get_keyword_hits("test", "ds", 20)
        mock_cognee.search.assert_called_once()
        from cognee.modules.search.types import SearchType
        assert mock_cognee.search.call_args.kwargs.get("query_type") == SearchType.CHUNKS_LEXICAL

    async def test_keyword_hits_graceful_on_error(self, monkeypatch, mock_cognee):
        orch, *_ = self._make()
        mock_cognee.search = AsyncMock(side_effect=RuntimeError("fail"))
        monkeypatch.setattr("elephantbroker.runtime.retrieval.orchestrator.cognee", mock_cognee)
        results = await orch.get_keyword_hits("test", "ds", 20)
        assert results == []

    async def test_weighted_merge_applies_weights(self, monkeypatch, mock_cognee):
        orch, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.retrieval.orchestrator.cognee", mock_cognee)
        fact = make_fact_assertion()
        graph.query_cypher = AsyncMock(return_value=[
            {"props": _fact_props(fact), "relations": []}
        ])
        policy = RetrievalPolicy(
            structural_weight=0.5, keyword_enabled=False,
            vector_enabled=False, graph_expansion_enabled=False, artifact_enabled=False,
        )
        results = await orch.retrieve_candidates("test", policy=policy)
        assert len(results) == 1
        assert results[0].score == 0.5

    async def test_dedup_keeps_highest_score(self, monkeypatch, mock_cognee):
        orch, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.retrieval.orchestrator.cognee", mock_cognee)
        fact = make_fact_assertion()
        graph.query_cypher = AsyncMock(return_value=[
            {"props": _fact_props(fact), "relations": []}
        ])
        mock_cognee.search = AsyncMock(return_value=[_fact_props(fact)])
        policy = RetrievalPolicy(
            structural_weight=0.5, keyword_weight=0.3,
            vector_enabled=False, graph_expansion_enabled=False, artifact_enabled=False,
        )
        results = await orch.retrieve_candidates("test", policy=policy)
        assert len(results) == 1
        assert results[0].score == 0.5  # Kept structural (higher)

    async def test_root_top_k_caps_results(self, monkeypatch, mock_cognee):
        orch, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.retrieval.orchestrator.cognee", mock_cognee)
        facts = [make_fact_assertion(text=f"fact {i}") for i in range(5)]
        graph.query_cypher = AsyncMock(return_value=[
            {"props": _fact_props(f), "relations": []} for f in facts
        ])
        policy = RetrievalPolicy(
            root_top_k=3, keyword_enabled=False,
            vector_enabled=False, graph_expansion_enabled=False, artifact_enabled=False,
        )
        results = await orch.retrieve_candidates("test", policy=policy)
        assert len(results) == 3

    async def test_disabled_source_skipped(self, monkeypatch, mock_cognee):
        orch, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.retrieval.orchestrator.cognee", mock_cognee)
        policy = RetrievalPolicy(
            keyword_enabled=False, structural_enabled=False,
            vector_enabled=False, graph_expansion_enabled=False, artifact_enabled=False,
        )
        results = await orch.retrieve_candidates("test", policy=policy)
        assert results == []

    async def test_isolation_none_no_filter(self, monkeypatch, mock_cognee):
        orch, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.retrieval.orchestrator.cognee", mock_cognee)
        fact = make_fact_assertion(session_key="other")
        graph.query_cypher = AsyncMock(return_value=[
            {"props": _fact_props(fact, session_key="other"), "relations": []}
        ])
        policy = RetrievalPolicy(
            isolation_level=IsolationLevel.NONE, isolation_scope=IsolationScope.GLOBAL,
            keyword_enabled=False, vector_enabled=False,
            graph_expansion_enabled=False, artifact_enabled=False,
        )
        results = await orch.retrieve_candidates("test", policy=policy, session_key="mine")
        assert len(results) == 1

    async def test_isolation_strict_disables_keyword(self, monkeypatch, mock_cognee):
        orch, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.retrieval.orchestrator.cognee", mock_cognee)
        policy = RetrievalPolicy(
            isolation_level=IsolationLevel.STRICT,
            keyword_enabled=True,
            structural_enabled=False, vector_enabled=False,
            graph_expansion_enabled=False, artifact_enabled=False,
        )
        await orch.retrieve_candidates("test", policy=policy)
        mock_cognee.search.assert_not_called()

    async def test_retrieve_empty_query(self, monkeypatch, mock_cognee):
        orch, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.retrieval.orchestrator.cognee", mock_cognee)
        results = await orch.retrieve_candidates("")
        assert isinstance(results, list)


class TestPhase9ArchivedAndBlacklistFilters:
    """Phase 9: Verify archived + autorecall_blacklisted Cypher filters."""

    def _make(self):
        graph = AsyncMock()
        vector = AsyncMock()
        embeddings = AsyncMock()
        ledger = TraceLedger()
        orch = RetrievalOrchestrator(
            graph=graph, vector=vector, embeddings=embeddings,
            trace_ledger=ledger, dataset_name="test", gateway_id="gw-1",
        )
        return orch, graph

    async def test_archived_facts_always_excluded(self):
        """Structural Cypher must include archived filter regardless of auto_recall."""
        orch, graph = self._make()
        graph.query_cypher = AsyncMock(return_value=[])
        await orch.get_structural_hits(limit=10, auto_recall=False)
        cypher_call = graph.query_cypher.call_args
        assert "f.archived IS NULL OR f.archived = false" in cypher_call[0][0]

    async def test_blacklist_excluded_when_auto_recall_true(self):
        """When auto_recall=True, blacklisted facts must be excluded."""
        orch, graph = self._make()
        graph.query_cypher = AsyncMock(return_value=[])
        await orch.get_structural_hits(limit=10, auto_recall=True)
        cypher_call = graph.query_cypher.call_args
        assert "f.autorecall_blacklisted IS NULL OR f.autorecall_blacklisted = false" in cypher_call[0][0]

    async def test_blacklist_not_excluded_when_auto_recall_false(self):
        """When auto_recall=False (explicit search), blacklisted facts remain findable."""
        orch, graph = self._make()
        graph.query_cypher = AsyncMock(return_value=[])
        await orch.get_structural_hits(limit=10, auto_recall=False)
        cypher_call = graph.query_cypher.call_args
        assert "autorecall_blacklisted" not in cypher_call[0][0]
