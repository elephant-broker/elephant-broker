"""Tests for MemoryStoreFacade."""
import uuid
from unittest.mock import AsyncMock

import pytest

from elephantbroker.runtime.adapters.cognee.vector import VectorSearchResult
from elephantbroker.runtime.memory.facade import DedupSkipped, MemoryStoreFacade
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.schemas.base import Scope
from elephantbroker.schemas.fact import FactAssertion, MemoryClass
from tests.fixtures.factories import make_fact_assertion


class TestMemoryStoreFacade:
    def _make(self):
        graph = AsyncMock()
        vector = AsyncMock()
        embeddings = AsyncMock()
        embeddings.embed_text = AsyncMock(return_value=[0.1] * 1024)
        ledger = TraceLedger()
        return MemoryStoreFacade(graph, vector, embeddings, ledger, dataset_name="test_ds"), graph, vector, embeddings, ledger

    async def test_store_fact(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, vector, emb, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion()
        result = await facade.store(fact)
        assert result.id == fact.id
        assert len(mock_add_data_points.calls) == 1

    async def test_search_returns_results_via_structural(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, vector, emb, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion()
        graph.query_cypher = AsyncMock(return_value=[{
            "props": {
                "eb_id": str(fact.id), "text": fact.text, "category": "general",
                "scope": "session", "confidence": 1.0, "eb_created_at": 0,
                "eb_updated_at": 0, "use_count": 0, "successful_use_count": 0,
                "provenance_refs": [], "target_actor_ids": [], "goal_ids": [],
            }
        }])
        results = await facade.search("test query", scope=Scope.SESSION)
        assert len(results) == 1

    async def test_promote_changes_scope(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, _, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion()
        graph.get_entity = AsyncMock(return_value={
            "eb_id": str(fact.id), "text": fact.text, "category": "general",
            "scope": "session", "confidence": 1.0, "eb_created_at": 0,
            "eb_updated_at": 0, "use_count": 0, "successful_use_count": 0,
            "provenance_refs": [], "target_actor_ids": [], "goal_ids": [],
        })
        result = await facade.promote(fact.id, Scope.GLOBAL)
        assert result.scope == Scope.GLOBAL

    async def test_decay_reduces_confidence(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, _, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion(confidence=0.8)
        graph.get_entity = AsyncMock(return_value={
            "eb_id": str(fact.id), "text": fact.text, "category": "general",
            "scope": "session", "confidence": 0.8, "eb_created_at": 0,
            "eb_updated_at": 0, "use_count": 0, "successful_use_count": 0,
            "provenance_refs": [], "target_actor_ids": [], "goal_ids": [],
        })
        result = await facade.decay(fact.id, 0.5)
        assert result.confidence == 0.4

    async def test_get_by_scope(self):
        facade, graph, _, _, _ = self._make()
        graph.query_cypher = AsyncMock(return_value=[])
        results = await facade.get_by_scope(Scope.SESSION)
        assert results == []

    async def test_store_emits_trace_event(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, vector, emb, ledger = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        await facade.store(make_fact_assertion())
        from elephantbroker.schemas.trace import TraceQuery
        events = await ledger.query_trace(TraceQuery())
        assert len(events) == 1

    async def test_store_calls_add_data_points(self, monkeypatch, mock_add_data_points, mock_cognee):
        """store() calls add_data_points with FactDataPoint."""
        facade, graph, vector, emb, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion()
        await facade.store(fact)
        assert len(mock_add_data_points.calls) == 1
        dp = mock_add_data_points.calls[0]["data_points"][0]
        assert dp.eb_id == str(fact.id)

    async def test_store_calls_cognee_add_with_fact_text(self, monkeypatch, mock_add_data_points, mock_cognee):
        """store() calls cognee.add() with fact.text."""
        facade, graph, vector, emb, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion(text="Important fact")
        await facade.store(fact)
        mock_cognee.add.assert_called_once()
        text = mock_cognee.add.call_args[0][0]
        assert text == "Important fact"

    async def test_store_does_not_call_vector_index_embedding(self, monkeypatch, mock_add_data_points, mock_cognee):
        """store() no longer calls VectorAdapter methods directly."""
        facade, graph, vector, emb, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        await facade.store(make_fact_assertion())
        # VectorAdapter should not have any write methods called
        assert not hasattr(vector, 'index_embedding') or not vector.index_embedding.called
        assert not hasattr(vector, 'ensure_collection') or not vector.ensure_collection.called

    async def test_promote_calls_add_data_points_not_cognee_add(self, monkeypatch, mock_add_data_points, mock_cognee):
        """UPDATE: promote() calls add_data_points but NOT cognee.add()."""
        facade, graph, _, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion()
        graph.get_entity = AsyncMock(return_value={
            "eb_id": str(fact.id), "text": fact.text, "category": "general",
            "scope": "session", "confidence": 1.0, "eb_created_at": 0,
            "eb_updated_at": 0, "use_count": 0, "successful_use_count": 0,
            "provenance_refs": [], "target_actor_ids": [], "goal_ids": [],
        })
        await facade.promote(fact.id, Scope.GLOBAL)
        assert len(mock_add_data_points.calls) == 1
        mock_cognee.add.assert_not_called()

    async def test_decay_calls_add_data_points_not_cognee_add(self, monkeypatch, mock_add_data_points, mock_cognee):
        """UPDATE: decay() calls add_data_points but NOT cognee.add()."""
        facade, graph, _, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion(confidence=0.8)
        graph.get_entity = AsyncMock(return_value={
            "eb_id": str(fact.id), "text": fact.text, "category": "general",
            "scope": "session", "confidence": 0.8, "eb_created_at": 0,
            "eb_updated_at": 0, "use_count": 0, "successful_use_count": 0,
            "provenance_refs": [], "target_actor_ids": [], "goal_ids": [],
        })
        await facade.decay(fact.id, 0.5)
        assert len(mock_add_data_points.calls) == 1
        mock_cognee.add.assert_not_called()

    async def test_search_hybrid_calls_cognee_search(self, monkeypatch, mock_add_data_points, mock_cognee):
        """search() calls cognee.search(GRAPH_COMPLETION)."""
        facade, graph, vector, emb, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        await facade.search("test query")
        mock_cognee.search.assert_called_once()

    async def test_search_hybrid_calls_structural_cypher_with_scope(self, monkeypatch, mock_add_data_points, mock_cognee):
        """search(scope=...) issues a structural Cypher query."""
        facade, graph, vector, emb, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        graph.query_cypher = AsyncMock(return_value=[])
        await facade.search("test query", scope=Scope.SESSION)
        graph.query_cypher.assert_called_once()
        cypher = graph.query_cypher.call_args[0][0]
        assert "f.scope = $scope" in cypher

    async def test_search_deduplicates_results(self, monkeypatch, mock_add_data_points, mock_cognee):
        """search() deduplicates when both GRAPH_COMPLETION and structural return the same fact."""
        facade, graph, vector, emb, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion()
        fact_props = {
            "eb_id": str(fact.id), "text": fact.text, "category": "general",
            "scope": "session", "confidence": 1.0, "eb_created_at": 0,
            "eb_updated_at": 0, "use_count": 0, "successful_use_count": 0,
            "provenance_refs": [], "target_actor_ids": [], "goal_ids": [],
        }
        # cognee.search returns same fact as structural query
        mock_cognee.search = AsyncMock(return_value=[fact_props])
        graph.query_cypher = AsyncMock(return_value=[{"props": fact_props}])
        results = await facade.search("test", scope=Scope.SESSION)
        # Should deduplicate to 1 result
        assert len(results) == 1

    async def test_promote_raises_on_missing_fact(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, _, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        graph.get_entity = AsyncMock(return_value=None)
        with pytest.raises(KeyError):
            await facade.promote(uuid.uuid4(), Scope.GLOBAL)

    async def test_decay_raises_on_missing_fact(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, _, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        graph.get_entity = AsyncMock(return_value=None)
        with pytest.raises(KeyError):
            await facade.decay(uuid.uuid4(), 0.5)

    async def test_get_by_scope_returns_facts(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, _, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion()
        graph.query_cypher = AsyncMock(return_value=[{
            "props": {
                "eb_id": str(fact.id), "text": fact.text, "category": "general",
                "scope": "session", "confidence": 1.0, "eb_created_at": 0,
                "eb_updated_at": 0, "use_count": 0, "successful_use_count": 0,
                "provenance_refs": [], "target_actor_ids": [], "goal_ids": [],
            }
        }])
        results = await facade.get_by_scope(Scope.SESSION)
        assert len(results) == 1
        assert results[0].text == fact.text

    async def test_search_with_actor_id_filter(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, _, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        graph.query_cypher = AsyncMock(return_value=[])
        await facade.search("test", actor_id="abc")
        cypher = graph.query_cypher.call_args[0][0]
        assert "f.source_actor_id = $actor_id" in cypher

    async def test_search_with_scope_and_actor_id(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, _, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        graph.query_cypher = AsyncMock(return_value=[])
        await facade.search("test", scope=Scope.GLOBAL, actor_id="abc")
        cypher = graph.query_cypher.call_args[0][0]
        assert "f.scope = $scope" in cypher
        assert "f.source_actor_id = $actor_id" in cypher
        assert " AND " in cypher

    async def test_decay_clamps_to_zero(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, _, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion(confidence=0.8)
        graph.get_entity = AsyncMock(return_value={
            "eb_id": str(fact.id), "text": fact.text, "category": "general",
            "scope": "session", "confidence": 0.8, "eb_created_at": 0,
            "eb_updated_at": 0, "use_count": 0, "successful_use_count": 0,
            "provenance_refs": [], "target_actor_ids": [], "goal_ids": [],
        })
        result = await facade.decay(fact.id, 0)
        assert result.confidence == 0.0

    async def test_decay_clamps_to_one(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, _, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion(confidence=0.8)
        graph.get_entity = AsyncMock(return_value={
            "eb_id": str(fact.id), "text": fact.text, "category": "general",
            "scope": "session", "confidence": 0.8, "eb_created_at": 0,
            "eb_updated_at": 0, "use_count": 0, "successful_use_count": 0,
            "provenance_refs": [], "target_actor_ids": [], "goal_ids": [],
        })
        result = await facade.decay(fact.id, 2.0)
        assert result.confidence == 1.0

    async def test_search_graceful_when_cognee_fails(self, monkeypatch, mock_add_data_points, mock_cognee):
        """search() falls back to structural when cognee.search() raises."""
        facade, graph, vector, emb, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        mock_cognee.search = AsyncMock(side_effect=RuntimeError("connection failed"))
        fact = make_fact_assertion()
        graph.query_cypher = AsyncMock(return_value=[{
            "props": {
                "eb_id": str(fact.id), "text": fact.text, "category": "general",
                "scope": "session", "confidence": 1.0, "eb_created_at": 0,
                "eb_updated_at": 0, "use_count": 0, "successful_use_count": 0,
                "provenance_refs": [], "target_actor_ids": [], "goal_ids": [],
            }
        }])
        results = await facade.search("test", scope=Scope.SESSION)
        assert len(results) == 1


class TestMemoryStoreFacadePhase4:
    """Phase 4 additions: dedup, edges, delete, get_by_id, update, promote_class."""

    def _make(self):
        graph = AsyncMock()
        vector = AsyncMock()
        embeddings = AsyncMock()
        embeddings.embed_text = AsyncMock(return_value=[0.1] * 1024)
        ledger = TraceLedger()
        return MemoryStoreFacade(graph, vector, embeddings, ledger, dataset_name="test_ds"), graph, vector, embeddings, ledger

    def _fact_props(self, fact, **overrides):
        base = {
            "eb_id": str(fact.id), "text": fact.text, "category": "general",
            "scope": "session", "confidence": 1.0, "memory_class": "episodic",
            "eb_created_at": 0, "eb_updated_at": 0, "use_count": 0,
            "successful_use_count": 0, "provenance_refs": [], "target_actor_ids": [],
            "goal_ids": [],
        }
        base.update(overrides)
        return base

    async def test_store_computes_token_size(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion(text="Hello world")
        result = await facade.store(fact)
        assert result.token_size is not None
        assert result.token_size > 0

    async def test_store_sets_embedding_ref(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion()
        result = await facade.store(fact)
        assert result.embedding_ref == f"FactDataPoint_text:{fact.id}"

    async def test_store_creates_created_by_edge(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        actor_id = uuid.uuid4()
        fact = make_fact_assertion(source_actor_id=actor_id)
        await facade.store(fact)
        graph.add_relation.assert_any_call(str(fact.id), str(actor_id), "CREATED_BY")

    async def test_store_creates_about_actor_edges(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        tid = uuid.uuid4()
        fact = make_fact_assertion(target_actor_ids=[tid])
        await facade.store(fact)
        graph.add_relation.assert_any_call(str(fact.id), str(tid), "ABOUT_ACTOR")

    async def test_store_creates_serves_goal_edges(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        gid = uuid.uuid4()
        fact = make_fact_assertion(goal_ids=[gid])
        await facade.store(fact)
        graph.add_relation.assert_any_call(str(fact.id), str(gid), "SERVES_GOAL")

    async def test_store_edge_failure_is_nonfatal(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, vector, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        # Ensure dedup check passes (no near-duplicate)
        vector.search_similar = AsyncMock(return_value=[])
        graph.add_relation = AsyncMock(side_effect=RuntimeError("edge fail"))
        fact = make_fact_assertion(source_actor_id=uuid.uuid4())
        result = await facade.store(fact)
        assert isinstance(result, FactAssertion)
        assert result.id == fact.id  # Store succeeds despite edge failure

    async def test_store_no_edge_when_no_actor(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion()
        await facade.store(fact)
        graph.add_relation.assert_not_called()

    async def test_store_dedup_skips_near_duplicate(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, vector, emb, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        vector.search_similar = AsyncMock(return_value=[VectorSearchResult(id="dup", score=0.98, payload={})])
        fact = make_fact_assertion()
        with pytest.raises(DedupSkipped) as exc_info:
            await facade.store(fact, dedup_threshold=0.95)
        assert exc_info.value.existing_fact_id == "dup"
        assert len(mock_add_data_points.calls) == 0  # Skipped

    async def test_store_dedup_allows_different(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, vector, emb, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        vector.search_similar = AsyncMock(return_value=[VectorSearchResult(id="diff", score=0.5, payload={})])
        fact = make_fact_assertion()
        result = await facade.store(fact, dedup_threshold=0.95)
        assert result is not None
        assert len(mock_add_data_points.calls) == 1  # Stored

    async def test_store_dedup_uses_precomputed_embedding(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, vector, emb, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        vector.search_similar = AsyncMock(return_value=[VectorSearchResult(id="diff", score=0.5, payload={})])
        pre_emb = [0.2] * 1024
        await facade.store(make_fact_assertion(), dedup_threshold=0.95, precomputed_embedding=pre_emb)
        emb.embed_text.assert_not_called()  # Used precomputed
        vector.search_similar.assert_called_once()
        call_emb = vector.search_similar.call_args[0][1]
        assert call_emb == pre_emb

    async def test_store_dedup_runs_with_default_threshold_when_none_passed(self, monkeypatch, mock_add_data_points, mock_cognee):
        """H2 fix: dedup runs with default threshold (0.85) when no explicit
        threshold is passed. Near-duplicates above default are skipped."""
        facade, graph, vector, emb, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        # Simulate a near-exact duplicate (score 0.98 > default 0.85)
        vector.search_similar = AsyncMock(return_value=[VectorSearchResult(id="dup", score=0.98, payload={})])
        fact = make_fact_assertion()
        # Should be skipped -- add_data_points NOT called, DedupSkipped raised
        with pytest.raises(DedupSkipped):
            await facade.store(fact)  # no dedup_threshold kwarg
        assert len(mock_add_data_points.calls) == 0
        vector.search_similar.assert_called_once()

    async def test_store_dedup_default_allows_different_enough(self, monkeypatch, mock_add_data_points, mock_cognee):
        """H2 fix: dedup with default threshold allows facts whose similarity
        is below 0.85."""
        facade, graph, vector, emb, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        # Similarity 0.7 is below default 0.85 -- should store
        vector.search_similar = AsyncMock(return_value=[VectorSearchResult(id="diff", score=0.7, payload={})])
        fact = make_fact_assertion()
        result = await facade.store(fact)  # no dedup_threshold kwarg
        assert result is not None
        assert len(mock_add_data_points.calls) == 1  # Stored

    async def test_search_default_max_results_20(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        graph.query_cypher = AsyncMock(return_value=[])
        # Default signature
        import inspect
        sig = inspect.signature(facade.search)
        assert sig.parameters["max_results"].default == 20

    async def test_search_respects_memory_class_filter(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        graph.query_cypher = AsyncMock(return_value=[])
        await facade.search("test", memory_class=MemoryClass.SEMANTIC)
        cypher = graph.query_cypher.call_args[0][0]
        assert "f.memory_class = $memory_class" in cypher

    async def test_search_respects_session_key_filter(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        graph.query_cypher = AsyncMock(return_value=[])
        await facade.search("test", session_key="agent:main:main")
        cypher = graph.query_cypher.call_args[0][0]
        assert "f.session_key = $session_key" in cypher

    async def test_search_computes_freshness_score(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion()
        graph.query_cypher = AsyncMock(return_value=[{"props": self._fact_props(fact), "relations": []}])
        results = await facade.search("test", scope=Scope.SESSION)
        assert len(results) == 1
        assert results[0].freshness_score is not None
        assert 0.99 < results[0].freshness_score <= 1.0  # Just created

    async def test_promote_scope_renames_correctly(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion()
        graph.get_entity = AsyncMock(return_value=self._fact_props(fact))
        result = await facade.promote_scope(fact.id, Scope.GLOBAL)
        assert result.scope == Scope.GLOBAL

    async def test_promote_class_changes_memory_class(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion()
        graph.get_entity = AsyncMock(return_value=self._fact_props(fact))
        result = await facade.promote_class(fact.id, MemoryClass.SEMANTIC)
        assert result.memory_class == MemoryClass.SEMANTIC

    async def test_get_by_id_returns_fact(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        fact = make_fact_assertion()
        graph.get_entity = AsyncMock(return_value=self._fact_props(fact))
        result = await facade.get_by_id(fact.id)
        assert result is not None
        assert result.text == fact.text

    async def test_get_by_id_returns_none_for_missing(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        graph.get_entity = AsyncMock(return_value=None)
        result = await facade.get_by_id(uuid.uuid4())
        assert result is None

    async def test_update_changes_fields(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion(confidence=1.0)
        graph.get_entity = AsyncMock(return_value=self._fact_props(fact))
        result = await facade.update(fact.id, {"confidence": 0.5})
        assert result.confidence == 0.5

    async def test_update_reembeds_when_text_changes(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, vector, emb, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion()
        graph.get_entity = AsyncMock(return_value=self._fact_props(fact))
        await facade.update(fact.id, {"text": "new text"})
        emb.embed_text.assert_called_once_with("new text")
        mock_cognee.add.assert_called_once()

    async def test_update_no_reembed_when_text_unchanged(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, vector, emb, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion()
        graph.get_entity = AsyncMock(return_value=self._fact_props(fact))
        await facade.update(fact.id, {"confidence": 0.5})
        emb.embed_text.assert_not_called()

    async def test_update_preserves_immutable_fields(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion()
        graph.get_entity = AsyncMock(return_value=self._fact_props(fact))
        result = await facade.update(fact.id, {"id": str(uuid.uuid4())})
        assert str(result.id) == str(fact.id)  # id unchanged

    async def test_update_raises_for_missing(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        graph.get_entity = AsyncMock(return_value=None)
        with pytest.raises(KeyError):
            await facade.update(uuid.uuid4(), {"confidence": 0.5})

    async def test_delete_removes_from_graph(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, vector, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        fact = make_fact_assertion()
        graph.get_entity = AsyncMock(return_value=self._fact_props(fact))
        await facade.delete(fact.id)
        graph.delete_entity.assert_called_once_with(str(fact.id))

    async def test_delete_removes_from_vector(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, vector, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        fact = make_fact_assertion()
        graph.get_entity = AsyncMock(return_value=self._fact_props(fact))
        await facade.delete(fact.id)
        vector.delete_embedding.assert_called_once_with("FactDataPoint_text", str(fact.id))

    async def test_delete_emits_trace_without_content(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, vector, _, ledger = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        fact = make_fact_assertion()
        graph.get_entity = AsyncMock(return_value=self._fact_props(fact))
        await facade.delete(fact.id)
        from elephantbroker.schemas.trace import TraceEventType, TraceQuery
        events = await ledger.query_trace(TraceQuery(event_types=[TraceEventType.GDPR_DELETE]))
        assert len(events) == 1
        assert "text" not in events[0].payload

    async def test_delete_raises_for_missing(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        graph.get_entity = AsyncMock(return_value=None)
        with pytest.raises(KeyError):
            await facade.delete(uuid.uuid4())

    async def test_delete_qdrant_failure_still_succeeds(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, vector, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        fact = make_fact_assertion()
        graph.get_entity = AsyncMock(return_value=self._fact_props(fact))
        vector.delete_embedding = AsyncMock(side_effect=RuntimeError("qdrant down"))
        await facade.delete(fact.id)  # Should not raise
        graph.delete_entity.assert_called_once()

    # --- TF-ER-003 Tier A: recent_facts GDPR scrub on delete ---

    def _make_with_buffer(self):
        import json as _json

        from elephantbroker.pipelines.turn_ingest.buffer import IngestBuffer
        from elephantbroker.schemas.config import LLMConfig

        class _FakeRedis:
            def __init__(self):
                self._kv: dict[str, str] = {}

            async def get(self, key):
                return self._kv.get(key)

            async def set(self, key, value, ex=None):
                self._kv[key] = value

            async def delete(self, key):
                self._kv.pop(key, None)

        graph = AsyncMock()
        vector = AsyncMock()
        embeddings = AsyncMock()
        embeddings.embed_text = AsyncMock(return_value=[0.1] * 1024)
        ledger = TraceLedger()
        redis = _FakeRedis()
        buffer = IngestBuffer(redis=redis, config=LLMConfig(), redis_keys=None)
        facade = MemoryStoreFacade(
            graph, vector, embeddings, ledger, dataset_name="test_ds", ingest_buffer=buffer,
        )
        return facade, graph, vector, redis, _json

    async def test_delete_scrubs_fact_from_recent_facts_buffer(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, vector, redis, _json = self._make_with_buffer()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        fact = make_fact_assertion(session_key="sk:test")
        props = self._fact_props(fact, session_key="sk:test")
        graph.get_entity = AsyncMock(return_value=props)
        buffer_key = "eb:recent_facts:sk:test"
        redis._kv[buffer_key] = _json.dumps([
            {"id": str(fact.id), "text": fact.text, "category": "general"},
        ])
        await facade.delete(fact.id)
        # Key deleted when scrub empties the list
        assert buffer_key not in redis._kv

    async def test_delete_preserves_other_facts_in_buffer(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, vector, redis, _json = self._make_with_buffer()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        fact = make_fact_assertion(session_key="sk:test")
        props = self._fact_props(fact, session_key="sk:test")
        graph.get_entity = AsyncMock(return_value=props)
        other_1 = {"id": str(uuid.uuid4()), "text": "other one", "category": "general"}
        other_2 = {"id": str(uuid.uuid4()), "text": "other two", "category": "general"}
        target = {"id": str(fact.id), "text": fact.text, "category": "general"}
        buffer_key = "eb:recent_facts:sk:test"
        redis._kv[buffer_key] = _json.dumps([other_1, target, other_2])
        await facade.delete(fact.id)
        remaining = _json.loads(redis._kv[buffer_key])
        ids = [e["id"] for e in remaining]
        assert str(fact.id) not in ids
        assert other_1["id"] in ids
        assert other_2["id"] in ids
        assert len(remaining) == 2

    async def test_delete_idempotent_when_fact_not_in_buffer(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, vector, redis, _json = self._make_with_buffer()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        fact = make_fact_assertion(session_key="sk:test")
        props = self._fact_props(fact, session_key="sk:test")
        graph.get_entity = AsyncMock(return_value=props)
        # Buffer populated with unrelated facts only
        unrelated = [
            {"id": str(uuid.uuid4()), "text": "a", "category": "general"},
            {"id": str(uuid.uuid4()), "text": "b", "category": "general"},
        ]
        buffer_key = "eb:recent_facts:sk:test"
        redis._kv[buffer_key] = _json.dumps(unrelated)
        await facade.delete(fact.id)  # Should not raise
        # Buffer contents unchanged
        assert _json.loads(redis._kv[buffer_key]) == unrelated
        graph.delete_entity.assert_called_once()

    async def test_update_recomputes_token_size(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion()
        graph.get_entity = AsyncMock(return_value=self._fact_props(fact))
        result = await facade.update(fact.id, {"text": "much longer text here for testing"})
        assert result.token_size is not None
        assert result.token_size > 0

    # --- TD-50 regression: cognee_data_id capture + cascade-on-update ---

    async def test_store_captures_cognee_data_id(self, monkeypatch, mock_add_data_points, mock_cognee):
        """store() captures data_id returned by cognee.add() onto fact.cognee_data_id
        and persists exactly once via a single add_data_points() MERGE."""
        from types import SimpleNamespace
        facade, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        returned_data_id = uuid.uuid4()
        mock_cognee.add = AsyncMock(return_value=SimpleNamespace(
            data_ingestion_info=[{"data_id": returned_data_id}],
        ))
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion()
        result = await facade.store(fact)
        assert result.cognee_data_id == returned_data_id
        # Single MERGE: cognee.add() captured the id BEFORE add_data_points(),
        # so we persist once — no double-MERGE and no cognee_data_id=None window.
        assert len(mock_add_data_points.calls) == 1
        persisted_dp = mock_add_data_points.calls[0]["data_points"][0]
        assert persisted_dp.cognee_data_id == str(returned_data_id)

    async def test_update_text_change_refreshes_cognee_data_id_and_cascades_old(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """update() with a new text re-ingests into Cognee, refreshes
        fact.cognee_data_id to the NEW data_id, then cascades the OLD
        data_id through the same cascade helper used by delete()."""
        from types import SimpleNamespace
        facade, graph, _, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)

        old_data_id = uuid.uuid4()
        new_data_id = uuid.uuid4()
        fact = make_fact_assertion()
        graph.get_entity = AsyncMock(return_value=self._fact_props(
            fact, cognee_data_id=str(old_data_id),
        ))

        mock_cognee.add = AsyncMock(return_value=SimpleNamespace(
            data_ingestion_info=[{"data_id": new_data_id}],
        ))
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)

        cascade_spy = AsyncMock()
        monkeypatch.setattr(facade, "_cascade_cognee_data", cascade_spy)

        result = await facade.update(fact.id, {"text": "rewritten text"})

        # NEW id is on the fact
        assert result.cognee_data_id == new_data_id
        # cognee.add() was called with the new text (re-ingest path)
        mock_cognee.add.assert_called_once()
        assert mock_cognee.add.call_args[0][0] == "rewritten text"
        # OLD id was cascaded — with update_text_change context, after MERGE
        cascade_spy.assert_called_once()
        call_args = cascade_spy.call_args
        assert call_args[0][0] == old_data_id  # positional: cognee_data_id=OLD
        assert call_args.kwargs["fact_id"] == fact.id
        assert call_args.kwargs["context"] == "update_text_change"

    async def test_update_metadata_only_leaves_cognee_data_id_untouched(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """update() without a text change must NOT re-ingest into Cognee
        and must NOT cascade the existing cognee_data_id — metadata-only
        edits leave the Cognee-owned document intact."""
        facade, graph, _, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)

        existing_data_id = uuid.uuid4()
        fact = make_fact_assertion(confidence=1.0)
        graph.get_entity = AsyncMock(return_value=self._fact_props(
            fact, cognee_data_id=str(existing_data_id),
        ))

        cascade_spy = AsyncMock()
        monkeypatch.setattr(facade, "_cascade_cognee_data", cascade_spy)

        result = await facade.update(fact.id, {"confidence": 0.5})

        # cognee_data_id is untouched
        assert result.cognee_data_id == existing_data_id
        # No re-ingest
        mock_cognee.add.assert_not_called()
        # No cascade
        cascade_spy.assert_not_called()

    # --- Observability tests (R1-C13, R1-C17) ---

    async def test_dedup_skip_emits_trace_event_with_session_fields(self, monkeypatch, mock_add_data_points, mock_cognee):
        """DEDUP_TRIGGERED trace event has session_key and session_id as top-level fields."""
        facade, graph, vector, emb, ledger = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        vector.search_similar = AsyncMock(return_value=[VectorSearchResult(id="dup-123", score=0.98, payload={})])
        sid = uuid.uuid4()
        fact = make_fact_assertion(session_key="sk:test", session_id=sid)
        with pytest.raises(DedupSkipped):
            await facade.store(fact, dedup_threshold=0.95)
        from elephantbroker.schemas.trace import TraceEventType, TraceQuery
        events = await ledger.query_trace(TraceQuery(event_types=[TraceEventType.DEDUP_TRIGGERED]))
        assert len(events) == 1
        assert events[0].session_key == "sk:test"
        assert events[0].session_id == sid
        assert events[0].payload["existing_fact_id"] == "dup-123"

    async def test_search_calls_inc_retrieval_metric(self, monkeypatch, mock_add_data_points, mock_cognee):
        """facade.search() calls inc_retrieval() with correct labels."""
        from unittest.mock import MagicMock
        facade, graph, vector, emb, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        metrics = MagicMock()
        facade._metrics = metrics
        graph.query_cypher = AsyncMock(return_value=[])
        await facade.search("test query", profile_name="coding", auto_recall=True)
        metrics.inc_retrieval.assert_called_once_with(auto_recall="true", profile_name="coding")

    async def test_delete_permission_error_emits_authority_check_trace(self, monkeypatch, mock_add_data_points, mock_cognee):
        """PermissionError on delete emits AUTHORITY_CHECK_FAILED trace event."""
        facade, graph, vector, emb, ledger = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        fact = make_fact_assertion()
        graph.get_entity = AsyncMock(return_value={**self._fact_props(fact), "gateway_id": "other-gw"})
        with pytest.raises(PermissionError):
            await facade.delete(fact.id)
        from elephantbroker.schemas.trace import TraceEventType, TraceQuery
        events = await ledger.query_trace(TraceQuery(event_types=[TraceEventType.AUTHORITY_CHECK_FAILED]))
        assert len(events) == 1
        assert events[0].payload["fact_id"] == str(fact.id)
        assert events[0].payload["owner_gateway"] == "other-gw"
