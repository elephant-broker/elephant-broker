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

    async def test_decay_rejects_factor_above_one_post_R2P9_fix(self, monkeypatch, mock_add_data_points, mock_cognee):
        """Pre-R2-P9 ``decay(factor=2.0)`` returned a clamped 1.0
        confidence — implicit boost-then-clamp that contradicted the
        method's monotonic-decrease semantics. Post-R2-P9
        (#1184 RESOLVED) the same input raises ``ValueError``.
        """
        facade, graph, _, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion(confidence=0.8)
        # get_entity not strictly needed for the validation path (the
        # ValueError fires before the entity lookup), but keep the mock
        # consistent with the sibling test.
        graph.get_entity = AsyncMock(return_value={
            "eb_id": str(fact.id), "text": fact.text, "category": "general",
            "scope": "session", "confidence": 0.8, "eb_created_at": 0,
            "eb_updated_at": 0, "use_count": 0, "successful_use_count": 0,
            "provenance_refs": [], "target_actor_ids": [], "goal_ids": [],
        })
        with pytest.raises(ValueError, match=r"decay factor must be in \[0\.0, 1\.0\]"):
            await facade.decay(fact.id, 2.0)

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

    async def test_search_use_count_mutation_in_place(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """TF-04-015 #1455: ``facade.search()`` mutates ``use_count`` in
        place on the FactAssertion objects it returns, via a background
        ``asyncio.create_task(self._update_use_counts(...))`` (facade.py:345).

        The fire-and-forget task increments ``fact.use_count += 1`` on the
        same Python objects the route layer hands to JSON serialization,
        so any caller who holds a reference and re-reads ``use_count``
        sees the value mutate underneath them. This documents the risk
        — it is intentional today (no copy at the boundary) but a future
        regression that breaks shared identity (e.g. round-tripping
        through ``model_validate`` mid-search) would silently zero out
        the counter.

        We capture ``asyncio.create_task`` and await the coroutine
        explicitly so the assertion is deterministic — without that the
        task races the test teardown.
        """
        facade, graph, vector, emb, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion()
        graph.query_cypher = AsyncMock(return_value=[{
            "props": {
                "eb_id": str(fact.id), "text": fact.text, "category": "general",
                "scope": "session", "confidence": 1.0, "eb_created_at": 0,
                "eb_updated_at": 0, "use_count": 5, "successful_use_count": 0,
                "provenance_refs": [], "target_actor_ids": [], "goal_ids": [],
            }
        }])
        captured = []

        def _capture(coro):
            captured.append(coro)

            class _Dummy:
                def cancel(self_inner):
                    pass

            return _Dummy()

        monkeypatch.setattr(
            "elephantbroker.runtime.memory.facade.asyncio.create_task", _capture,
        )
        results = await facade.search("test query", scope=Scope.SESSION)
        assert len(results) == 1
        assert results[0].use_count == 5
        # Drain the fire-and-forget task to trigger the mutation.
        assert len(captured) == 1
        await captured[0]
        # The same object the caller holds now reads as 6 — in-place mutation.
        assert results[0].use_count == 6


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

    async def test_update_reingests_to_cognee_when_text_changes(self, monkeypatch, mock_add_data_points, mock_cognee):
        # TODO-5-612 / TODO-5-701: update path no longer makes a standalone
        # embed_text() call — cognee.add() re-embeds internally, and the
        # update path has no dedup pre-check to consume an external
        # embedding. Pin that: embed_text must NOT be called, but
        # cognee.add must be called exactly once (the re-ingest path).
        facade, graph, vector, emb, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion()
        graph.get_entity = AsyncMock(return_value=self._fact_props(fact))
        await facade.update(fact.id, {"text": "new text"})
        emb.embed_text.assert_not_called()
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

            async def eval(self, script, numkeys, *keys_and_args):
                # Minimal Lua eval emulation for the scrub script (5-101).
                # Redis runs Lua atomically server-side. This Python mock is
                # trivially "atomic" against concurrent coroutines because the
                # body contains zero `await` points — asyncio cannot interleave
                # another coroutine's eval() on the same key. (The Python GIL
                # is about thread scheduling; asyncio atomicity here comes
                # from the absence of suspension points, not the GIL.)
                key = keys_and_args[0]
                target = keys_and_args[1]
                ttl = int(keys_and_args[2])
                data = self._kv.get(key)
                if not data:
                    return 0
                # 5-317: non-table decode results DEL the corrupt key and
                # return 0 — mirrors the Lua script's defense-in-depth branch.
                try:
                    entries = _json.loads(data)
                except (_json.JSONDecodeError, TypeError):
                    self._kv.pop(key, None)
                    return 0
                if not isinstance(entries, list):
                    self._kv.pop(key, None)
                    return 0
                filtered = [
                    e for e in entries
                    if not (isinstance(e, dict) and str(e.get("id")) == target)
                ]
                removed = len(entries) - len(filtered)
                if removed == 0:
                    return 0
                if filtered:
                    self._kv[key] = _json.dumps(filtered)
                else:
                    self._kv.pop(key, None)
                _ = ttl  # TTL applied by real Redis; the fake has no TTL store
                return removed

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
        buffer_key = "eb::recent_facts:sk:test"
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
        buffer_key = "eb::recent_facts:sk:test"
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
        buffer_key = "eb::recent_facts:sk:test"
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

    # --- 5-210: delete/scrub ordering. Scrub must run BEFORE graph.delete
    # so the recent_facts window is already clean when the cascade begins.
    # If a concurrent turn-ingest cycle reads recent_facts between scrub
    # and graph-delete, it will observe the already-purged state.

    async def test_delete_scrubs_before_graph_delete(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        facade, graph, vector, redis, _json = self._make_with_buffer()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        fact = make_fact_assertion(session_key="sk:test")
        props = self._fact_props(fact, session_key="sk:test")
        graph.get_entity = AsyncMock(return_value=props)
        redis._kv["eb::recent_facts:sk:test"] = _json.dumps([
            {"id": str(fact.id), "text": fact.text, "category": "general"},
        ])

        call_order: list[str] = []

        orig_scrub = facade._ingest_buffer.scrub_fact_from_recent

        async def _track_scrub(session_key, fact_id):
            call_order.append("scrub")
            return await orig_scrub(session_key, fact_id)

        async def _track_graph_delete(fact_id):
            call_order.append("graph_delete")

        facade._ingest_buffer.scrub_fact_from_recent = _track_scrub  # type: ignore[assignment]
        graph.delete_entity = AsyncMock(side_effect=_track_graph_delete)

        await facade.delete(fact.id)

        assert call_order == ["scrub", "graph_delete"], (
            f"5-210: scrub must precede graph delete, got {call_order}"
        )

    async def test_delete_then_concurrent_ingest_does_not_resurface(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """5-210 end-state guarantee: after facade.delete() completes, the
        recent_facts window no longer contains the deleted fact. A
        subsequent ingest cycle that reads the window observes the purged
        state."""
        facade, graph, vector, redis, _json = self._make_with_buffer()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        fact = make_fact_assertion(session_key="sk:test")
        props = self._fact_props(fact, session_key="sk:test")
        graph.get_entity = AsyncMock(return_value=props)
        buffer_key = "eb::recent_facts:sk:test"
        redis._kv[buffer_key] = _json.dumps([
            {"id": str(fact.id), "text": fact.text, "category": "general"},
            {"id": str(uuid.uuid4()), "text": "survivor", "category": "general"},
        ])

        await facade.delete(fact.id)

        # Simulate a subsequent ingest cycle loading recent_facts.
        loaded = await facade._ingest_buffer.load_recent_facts("sk:test")
        loaded_ids = {e["id"] for e in loaded}
        assert str(fact.id) not in loaded_ids

    # --- eb_recent_facts_scrubbed_total metric (PR #5 TODOs 5-501, 5-602) ---
    # The merge report's TF-ER-003 flow-result line claims this metric
    # increments on every /memory/{id} delete that reaches the scrub path.
    # The three tests below pin the three status labels exhaustively so a
    # future regression on any branch (scrub hit / scrub no-op / Redis
    # error) fires a test.

    async def test_delete_scrub_success_increments_scrubbed_metric(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        from unittest.mock import MagicMock
        facade, graph, vector, redis, _json = self._make_with_buffer()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        metrics = MagicMock()
        facade._metrics = metrics
        fact = make_fact_assertion(session_key="sk:test")
        props = self._fact_props(fact, session_key="sk:test")
        graph.get_entity = AsyncMock(return_value=props)
        redis._kv["eb::recent_facts:sk:test"] = _json.dumps([
            {"id": str(fact.id), "text": fact.text, "category": "general"},
        ])
        await facade.delete(fact.id)
        metrics.inc_recent_facts_scrubbed.assert_called_once_with("scrubbed")

    async def test_delete_scrub_noop_increments_noop_metric(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """Fact was never buffered (expired TTL, different session) → noop."""
        from unittest.mock import MagicMock
        facade, graph, vector, redis, _json = self._make_with_buffer()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        metrics = MagicMock()
        facade._metrics = metrics
        fact = make_fact_assertion(session_key="sk:test")
        props = self._fact_props(fact, session_key="sk:test")
        graph.get_entity = AsyncMock(return_value=props)
        # recent_facts key exists but contains only unrelated entries.
        redis._kv["eb::recent_facts:sk:test"] = _json.dumps([
            {"id": str(uuid.uuid4()), "text": "other", "category": "general"},
        ])
        await facade.delete(fact.id)
        metrics.inc_recent_facts_scrubbed.assert_called_once_with("noop")

    async def test_delete_scrub_failure_increments_failure_metric(
        self, monkeypatch, mock_add_data_points, mock_cognee, caplog,
    ):
        """Redis raises during scrub → failure label, warning logged, delete
        continues (does not propagate the exception)."""
        from unittest.mock import MagicMock
        facade, graph, vector, redis, _json = self._make_with_buffer()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        metrics = MagicMock()
        facade._metrics = metrics
        fact = make_fact_assertion(session_key="sk:test")
        props = self._fact_props(fact, session_key="sk:test")
        graph.get_entity = AsyncMock(return_value=props)
        # Monkey-patch the buffer's scrub to raise.
        async def _boom(session_key, fact_id):
            raise RuntimeError("redis down")
        facade._ingest_buffer.scrub_fact_from_recent = _boom
        with caplog.at_level("WARNING", logger="elephantbroker.memory.facade"):
            await facade.delete(fact.id)  # Must not raise.
        metrics.inc_recent_facts_scrubbed.assert_called_once_with("failure")
        assert any(
            "recent_facts scrub failed" in rec.message and str(fact.id) in rec.message
            for rec in caplog.records
        ), "failure branch must still emit the WARNING log"
        graph.delete_entity.assert_called_once()

    # --- Cascade observability + GDPR_DELETE payload (PR #5 TODOs 5-502,
    # 5-503, 5-607). The three cascade steps (graph / vector / cognee_data)
    # must each run independently — a failure in one must NOT short-circuit
    # the rest — and every step-failure must emit a DEGRADED_OPERATION trace
    # plus an eb_fact_delete_cascade_failures_total increment. The GDPR_DELETE
    # event is emitted on every delete (including partial-failure) and
    # carries the per-step cascade_status dict + session_key so auditors can
    # tell clean-delete from degraded-delete without joining against the
    # degraded-ops stream.
    # ---

    async def test_delete_cascade_all_success_emits_clean_gdpr_delete(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """All three cascade steps succeed → GDPR_DELETE cascade_status is
        all-ok, session_key promoted to TraceEvent field + payload, no
        DEGRADED_OPERATION events emitted, cascade-failure metric untouched."""
        from unittest.mock import AsyncMock, MagicMock
        facade, graph, vector, _, ledger = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        metrics = MagicMock()
        facade._metrics = metrics
        sid = uuid.uuid4()
        fact = make_fact_assertion(session_key="sk:test", session_id=sid)
        graph.get_entity = AsyncMock(return_value=self._fact_props(
            fact, session_key="sk:test", session_id=str(sid),
            cognee_data_id=str(uuid.uuid4()),
        ))
        monkeypatch.setattr(facade, "_cascade_cognee_data", AsyncMock(return_value="ok"))

        await facade.delete(fact.id)

        from elephantbroker.schemas.trace import TraceEventType, TraceQuery
        degraded = await ledger.query_trace(
            TraceQuery(event_types=[TraceEventType.DEGRADED_OPERATION]),
        )
        assert degraded == [], "clean cascade must not emit DEGRADED_OPERATION"
        metrics.inc_fact_delete_cascade_failure.assert_not_called()

        gdpr = await ledger.query_trace(TraceQuery(event_types=[TraceEventType.GDPR_DELETE]))
        assert len(gdpr) == 1
        event = gdpr[0]
        assert event.payload["cascade_status"] == {
            "graph": "ok", "vector": "ok", "cognee_data": "ok",
        }
        assert event.payload["session_key"] == "sk:test"
        assert event.payload["fact_id"] == str(fact.id)
        # TraceEvent first-class fields (so /trace?session_key= filters hit)
        assert event.session_key == "sk:test"
        assert event.session_id == sid

    async def test_delete_cascade_graph_failure_continues_vector_and_cognee(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """Graph DETACH DELETE raises → steps 2+3 still run (5-607), per-step
        DEGRADED_OPERATION + metric(step=graph), and GDPR_DELETE is emitted
        with cascade_status graph=failed, vector=ok, cognee_data=ok."""
        from unittest.mock import AsyncMock, MagicMock
        facade, graph, vector, _, ledger = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        metrics = MagicMock()
        facade._metrics = metrics
        fact = make_fact_assertion(session_key="sk:test")
        graph.get_entity = AsyncMock(return_value=self._fact_props(
            fact, session_key="sk:test", cognee_data_id=str(uuid.uuid4()),
        ))
        graph.delete_entity = AsyncMock(side_effect=RuntimeError("neo4j down"))
        cascade_spy = AsyncMock(return_value="ok")
        monkeypatch.setattr(facade, "_cascade_cognee_data", cascade_spy)

        await facade.delete(fact.id)  # must not raise

        # Steps 2 + 3 still attempted (core 5-607 assertion).
        vector.delete_embedding.assert_called_once_with("FactDataPoint_text", str(fact.id))
        cascade_spy.assert_called_once()

        # Metric fired once with step=graph, operation=delete.
        metrics.inc_fact_delete_cascade_failure.assert_called_once_with(
            "graph", operation="delete",
        )

        from elephantbroker.schemas.trace import TraceEventType, TraceQuery
        degraded = await ledger.query_trace(
            TraceQuery(event_types=[TraceEventType.DEGRADED_OPERATION]),
        )
        assert len(degraded) == 1
        assert degraded[0].payload["step"] == "graph"
        assert degraded[0].payload["component"] == "memory_facade"
        assert degraded[0].payload["operation"] == "delete"
        assert degraded[0].payload["fact_id"] == str(fact.id)

        gdpr = await ledger.query_trace(TraceQuery(event_types=[TraceEventType.GDPR_DELETE]))
        assert len(gdpr) == 1
        assert gdpr[0].payload["cascade_status"] == {
            "graph": "failed", "vector": "ok", "cognee_data": "ok",
        }

    async def test_delete_cascade_vector_failure_alone(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """Vector delete raises (Qdrant down), graph + cognee succeed →
        DEGRADED_OPERATION(step=vector) + metric(step=vector), GDPR_DELETE
        reflects vector=failed and graph/cognee=ok."""
        from unittest.mock import AsyncMock, MagicMock
        facade, graph, vector, _, ledger = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        metrics = MagicMock()
        facade._metrics = metrics
        fact = make_fact_assertion(session_key="sk:test")
        graph.get_entity = AsyncMock(return_value=self._fact_props(
            fact, session_key="sk:test", cognee_data_id=str(uuid.uuid4()),
        ))
        vector.delete_embedding = AsyncMock(side_effect=RuntimeError("qdrant down"))
        monkeypatch.setattr(facade, "_cascade_cognee_data", AsyncMock(return_value="ok"))

        await facade.delete(fact.id)

        metrics.inc_fact_delete_cascade_failure.assert_called_once_with(
            "vector", operation="delete",
        )
        from elephantbroker.schemas.trace import TraceEventType, TraceQuery
        degraded = await ledger.query_trace(
            TraceQuery(event_types=[TraceEventType.DEGRADED_OPERATION]),
        )
        assert len(degraded) == 1
        assert degraded[0].payload["step"] == "vector"
        gdpr = await ledger.query_trace(TraceQuery(event_types=[TraceEventType.GDPR_DELETE]))
        assert gdpr[0].payload["cascade_status"] == {
            "graph": "ok", "vector": "failed", "cognee_data": "ok",
        }

    async def test_delete_cascade_cognee_failure_alone(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """_cascade_cognee_data returns 'failed' → re-emitted at the call
        site as DEGRADED_OPERATION(step=cognee_data) + metric; GDPR_DELETE
        reflects cognee_data=failed. Pins the 'helper returns status →
        facade emits observability' contract so a future refactor cannot
        silently drop the cognee-step signal."""
        from unittest.mock import AsyncMock, MagicMock
        facade, graph, vector, _, ledger = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        metrics = MagicMock()
        facade._metrics = metrics
        fact = make_fact_assertion(session_key="sk:test")
        graph.get_entity = AsyncMock(return_value=self._fact_props(
            fact, session_key="sk:test", cognee_data_id=str(uuid.uuid4()),
        ))
        monkeypatch.setattr(facade, "_cascade_cognee_data", AsyncMock(return_value="failed"))

        await facade.delete(fact.id)

        metrics.inc_fact_delete_cascade_failure.assert_called_once_with(
            "cognee_data", operation="delete",
        )
        from elephantbroker.schemas.trace import TraceEventType, TraceQuery
        degraded = await ledger.query_trace(
            TraceQuery(event_types=[TraceEventType.DEGRADED_OPERATION]),
        )
        assert len(degraded) == 1
        assert degraded[0].payload["step"] == "cognee_data"
        gdpr = await ledger.query_trace(TraceQuery(event_types=[TraceEventType.GDPR_DELETE]))
        assert gdpr[0].payload["cascade_status"] == {
            "graph": "ok", "vector": "ok", "cognee_data": "failed",
        }

    async def test_delete_cascade_multi_step_failure_emits_each_step(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """Graph + vector both fail; cognee_data succeeds → two
        DEGRADED_OPERATION events (one per failed step), two metric
        increments, GDPR_DELETE still emitted with cascade_status reflecting
        BOTH failures. Pins the independence of per-step observability:
        even when multiple layers are down the operator sees each signal
        separately in the degraded-ops stream."""
        from unittest.mock import AsyncMock, MagicMock
        facade, graph, vector, _, ledger = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        metrics = MagicMock()
        facade._metrics = metrics
        fact = make_fact_assertion(session_key="sk:test")
        graph.get_entity = AsyncMock(return_value=self._fact_props(
            fact, session_key="sk:test", cognee_data_id=str(uuid.uuid4()),
        ))
        graph.delete_entity = AsyncMock(side_effect=RuntimeError("neo4j down"))
        vector.delete_embedding = AsyncMock(side_effect=RuntimeError("qdrant down"))
        monkeypatch.setattr(facade, "_cascade_cognee_data", AsyncMock(return_value="ok"))

        await facade.delete(fact.id)

        # Two metric increments, both step labels distinct; both tagged delete.
        assert metrics.inc_fact_delete_cascade_failure.call_count == 2
        calls = metrics.inc_fact_delete_cascade_failure.call_args_list
        called_steps = {c.args[0] for c in calls}
        assert called_steps == {"graph", "vector"}
        assert all(c.kwargs.get("operation") == "delete" for c in calls)

        from elephantbroker.schemas.trace import TraceEventType, TraceQuery
        degraded = await ledger.query_trace(
            TraceQuery(event_types=[TraceEventType.DEGRADED_OPERATION]),
        )
        assert len(degraded) == 2
        assert {e.payload["step"] for e in degraded} == {"graph", "vector"}

        gdpr = await ledger.query_trace(TraceQuery(event_types=[TraceEventType.GDPR_DELETE]))
        assert len(gdpr) == 1
        assert gdpr[0].payload["cascade_status"] == {
            "graph": "failed", "vector": "failed", "cognee_data": "ok",
        }

    async def test_delete_gdpr_payload_includes_session_key_on_traceevent_fields(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """GDPR_DELETE TraceEvent carries session_key + session_id as
        first-class fields (not only inside payload) so /trace?session_key=
        filters hit delete events. Without this, a compliance auditor
        querying by session would miss GDPR removals against that session."""
        from unittest.mock import AsyncMock
        facade, graph, _, _, ledger = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        sid = uuid.uuid4()
        fact = make_fact_assertion(session_key="sk:compliance", session_id=sid)
        graph.get_entity = AsyncMock(return_value=self._fact_props(
            fact, session_key="sk:compliance", session_id=str(sid),
        ))

        await facade.delete(fact.id)

        from elephantbroker.schemas.trace import TraceEventType, TraceQuery
        gdpr = await ledger.query_trace(TraceQuery(event_types=[TraceEventType.GDPR_DELETE]))
        assert len(gdpr) == 1
        event = gdpr[0]
        # TraceEvent first-class fields populated.
        assert event.session_key == "sk:compliance"
        assert event.session_id == sid
        # Payload mirrors for consumers that only read payload.
        assert event.payload["session_key"] == "sk:compliance"
        assert event.payload["fact_id"] == str(fact.id)
        # Query by session_key must surface this event.
        by_session = await ledger.query_trace(TraceQuery(
            event_types=[TraceEventType.GDPR_DELETE],
            session_key="sk:compliance",
        ))
        assert len(by_session) == 1
        assert by_session[0].id == event.id

    # --- TD-50 regression: cognee_data_id capture + cascade-on-update ---

    async def test_store_captures_cognee_data_id(self, monkeypatch, mock_add_data_points, mock_cognee):
        """store() captures data_id returned by cognee.add() and threads it
        onto the persisted FactDataPoint via from_schema(cognee_data_id=...),
        NOT onto FactAssertion (TODO-5-307 — storage-backend identifiers do
        not leak into the semantic schema). Persists exactly once via a
        single add_data_points() MERGE."""
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
        # Return value is pure FactAssertion — no storage-backend field.
        assert not hasattr(result, "cognee_data_id") or getattr(result, "cognee_data_id", None) is None, (
            "TODO-5-307: FactAssertion must not carry cognee_data_id"
        )
        # Single MERGE: cognee.add() captured the id BEFORE add_data_points(),
        # so we persist once — no double-MERGE and no cognee_data_id=None window.
        assert len(mock_add_data_points.calls) == 1
        persisted_dp = mock_add_data_points.calls[0]["data_points"][0]
        assert persisted_dp.cognee_data_id == str(returned_data_id)

    async def test_update_text_change_refreshes_cognee_data_id_and_cascades_old(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """update() with a new text re-ingests into Cognee, refreshes the
        persisted FactDataPoint.cognee_data_id to the NEW data_id, then
        cascades the OLD data_id through the same cascade helper used by
        delete(). TODO-5-307: FactAssertion no longer carries
        cognee_data_id — the value lives on the DataPoint / graph node."""
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

        await facade.update(fact.id, {"text": "rewritten text"})

        # NEW id is on the persisted DataPoint (storage-backend identifier).
        assert len(mock_add_data_points.calls) == 1
        persisted_dp = mock_add_data_points.calls[0]["data_points"][0]
        assert persisted_dp.cognee_data_id == str(new_data_id)
        # cognee.add() was called with the new text (re-ingest path)
        mock_cognee.add.assert_called_once()
        assert mock_cognee.add.call_args[0][0] == "rewritten text"
        # OLD id was cascaded — with update_text_change context, after MERGE
        cascade_spy.assert_called_once()
        call_args = cascade_spy.call_args
        assert call_args[0][0] == old_data_id  # positional: cognee_data_id=OLD
        assert call_args.kwargs["fact_id"] == fact.id
        assert call_args.kwargs["context"] == "update_text_change"

    async def test_update_cascade_failed_emits_degraded_operation_trace(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """TODO-5-110: when the update-path cascade helper returns
        "failed" (cognee.datasets.delete_data raised for a reason other
        than the recoverable Qdrant-404), facade.update() must mirror
        delete()'s observability pattern — emit `_emit_cascade_failure`
        with operation="update" + step="cognee_data" so the trio
        (metric + DEGRADED_OPERATION trace) fires. Pre-fix the update
        path discarded the status and the failure was silent."""
        from types import SimpleNamespace
        facade, graph, _, _, ledger = self._make()
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

        monkeypatch.setattr(
            facade, "_cascade_cognee_data", AsyncMock(return_value="failed"),
        )

        await facade.update(fact.id, {"text": "rewritten text"})

        from elephantbroker.schemas.trace import TraceEventType, TraceQuery
        events = await ledger.query_trace(
            TraceQuery(event_types=[TraceEventType.DEGRADED_OPERATION]),
        )
        cascade_events = [
            e for e in events
            if e.payload.get("failure") == "cascade_step"
        ]
        assert len(cascade_events) == 1
        payload = cascade_events[0].payload
        assert payload["operation"] == "update"
        assert payload["step"] == "cognee_data"
        assert payload["fact_id"] == str(fact.id)
        assert payload["exception_type"] == "RuntimeError"

    async def test_update_cascade_failure_metric_carries_operation_update_label(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """TODO-5-511: the shared cascade-failure counter must carry the
        operation label so dashboards can split delete-path failures from
        update-path failures. Pre-fix the metric had step only; a spike in
        update-path cascade failures looked identical to a spike in the
        delete-path cascade, masking the root cause."""
        from types import SimpleNamespace
        from unittest.mock import AsyncMock, MagicMock
        facade, graph, _, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        metrics = MagicMock()
        facade._metrics = metrics

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
        monkeypatch.setattr(
            facade, "_cascade_cognee_data", AsyncMock(return_value="failed"),
        )

        await facade.update(fact.id, {"text": "rewritten"})

        metrics.inc_fact_delete_cascade_failure.assert_called_once_with(
            "cognee_data", operation="update",
        )

    async def test_update_cascade_ok_does_not_emit_degraded_operation(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """TODO-5-110 negative: a clean cascade ("ok" / "ok_idempotent")
        on the update path must NOT emit the cascade-failure trio —
        otherwise dashboards would tag every clean text-change update
        as a degraded operation."""
        from types import SimpleNamespace
        facade, graph, _, _, ledger = self._make()
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
        monkeypatch.setattr(
            facade, "_cascade_cognee_data", AsyncMock(return_value="ok"),
        )

        await facade.update(fact.id, {"text": "rewritten text"})

        from elephantbroker.schemas.trace import TraceEventType, TraceQuery
        events = await ledger.query_trace(
            TraceQuery(event_types=[TraceEventType.DEGRADED_OPERATION]),
        )
        cascade_events = [
            e for e in events
            if e.payload.get("failure") == "cascade_step"
        ]
        assert cascade_events == []

    async def test_update_metadata_only_leaves_cognee_data_id_untouched(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """update() without a text change must NOT re-ingest into Cognee
        and must NOT cascade the existing cognee_data_id — metadata-only
        edits leave the Cognee-owned document intact. TODO-5-307: the
        existing cognee_data_id is carried forward onto the persisted
        DataPoint (MERGE preserves the graph property)."""
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

        await facade.update(fact.id, {"confidence": 0.5})

        # cognee_data_id is untouched on the re-MERGEd DataPoint.
        assert len(mock_add_data_points.calls) == 1
        persisted_dp = mock_add_data_points.calls[0]["data_points"][0]
        assert persisted_dp.cognee_data_id == str(existing_data_id)
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

    # --- PR #5 TODO 5-601: facade.update() gateway-ownership pre-check ---
    # Without this check, PATCH /memory/{fact_id} was a cross-tenant
    # mutation vector. These two tests pin the facade layer:
    #   1. cross-gateway attempt raises PermissionError + emits
    #      AUTHORITY_CHECK_FAILED trace with action="update" discriminator
    #      (so auditors can tell update-attempts from delete-attempts).
    #   2. matching gateway proceeds normally — the new check must NOT
    #      regress the happy path for in-tenant updates.

    async def test_update_permission_error_emits_authority_check_trace(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """Cross-tenant PATCH attempt: facade.update() raises PermissionError
        + emits AUTHORITY_CHECK_FAILED trace carrying action=update plus
        owner/caller gateway ids. The 'action' discriminator is the piece
        that lets operators tell mutation-attempts from deletion-attempts
        in the forensic stream (delete() uses the same event type but
        without an action label today — the update() site adds it)."""
        facade, graph, _, _, ledger = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion()
        graph.get_entity = AsyncMock(return_value={
            **self._fact_props(fact), "gateway_id": "tenant-other",
        })
        with pytest.raises(PermissionError):
            await facade.update(
                fact.id, {"text": "cross-tenant"}, caller_gateway_id="tenant-local",
            )
        from elephantbroker.schemas.trace import TraceEventType, TraceQuery
        events = await ledger.query_trace(
            TraceQuery(event_types=[TraceEventType.AUTHORITY_CHECK_FAILED]),
        )
        assert len(events) == 1
        payload = events[0].payload
        assert payload["fact_id"] == str(fact.id)
        assert payload["owner_gateway"] == "tenant-other"
        assert payload["caller_gateway"] == "tenant-local"
        assert payload["action"] == "update"
        # add_data_points must NOT have been called — the update never
        # reached the persistence path.
        assert len(mock_add_data_points.calls) == 0

    async def test_update_same_gateway_proceeds(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """Matching gateway: ownership check passes, update proceeds
        normally. Guards against a future regression that accidentally
        flips the comparison operator."""
        facade, graph, _, _, ledger = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion()
        graph.get_entity = AsyncMock(return_value={
            **self._fact_props(fact), "gateway_id": "tenant-local",
        })
        result = await facade.update(
            fact.id, {"confidence": 0.42}, caller_gateway_id="tenant-local",
        )
        assert result.confidence == 0.42
        from elephantbroker.schemas.trace import TraceEventType, TraceQuery
        events = await ledger.query_trace(
            TraceQuery(event_types=[TraceEventType.AUTHORITY_CHECK_FAILED]),
        )
        assert events == [], "same-gateway update must not emit AUTHORITY_CHECK_FAILED"

    # --- PR #5 TODO 5-204: eb_memory_store_total{status="failure"} ---
    # Before this fix, the failure label was declared on the counter but
    # never incremented — any cognee.add / add_data_points / graph-level
    # failure that propagated out of store() or delete() left the
    # metric stream looking as if the operation had succeeded. The four
    # tests below pin both the success and failure emission paths for
    # both operations.

    async def test_store_success_increments_success_status(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """Happy-path store — status=success emitted exactly once."""
        from unittest.mock import MagicMock
        facade, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        metrics = MagicMock()
        facade._metrics = metrics
        await facade.store(make_fact_assertion())
        metrics.inc_store.assert_called_once_with("store", "success")

    async def test_store_success_increments_facts_stored(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """Gap #11: inc_facts_stored(memory_class, profile_name) fires on store success."""
        from unittest.mock import MagicMock
        facade, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        metrics = MagicMock()
        facade._metrics = metrics
        fact = make_fact_assertion()
        await facade.store(fact)
        mc = fact.memory_class.value if hasattr(fact.memory_class, "value") else str(fact.memory_class)
        metrics.inc_facts_stored.assert_called_once_with(mc, "unknown")

    async def test_store_failure_emits_failure_status_and_reraises(
        self, monkeypatch, mock_add_data_points,
    ):
        """cognee.add raises → store re-raises + inc_store("store","failure")
        emitted. Previously this path incremented no metric at all."""
        from unittest.mock import MagicMock
        facade, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        metrics = MagicMock()
        facade._metrics = metrics
        # Mock cognee.add to blow up mid-store.
        class _BoomCognee:
            async def add(self, *args, **kwargs):
                raise RuntimeError("cognee.add exploded")
            async def cognify(self, *args, **kwargs):
                return None
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", _BoomCognee())
        with pytest.raises(RuntimeError, match="cognee.add exploded"):
            await facade.store(make_fact_assertion())
        metrics.inc_store.assert_called_once_with("store", "failure")

    async def test_store_dedup_skip_does_not_emit_failure_status(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """DedupSkipped is a legitimate skip, NOT a failure. inc_dedup
        handles it; inc_store must NOT be called with status=failure
        (or status=success — the store never actually ran)."""
        from unittest.mock import MagicMock
        facade, _, vector, emb, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        metrics = MagicMock()
        facade._metrics = metrics
        # Return a near-duplicate hit to trigger DedupSkipped.
        vector.search_similar = AsyncMock(return_value=[
            VectorSearchResult(id=str(uuid.uuid4()), score=0.99, payload={}),
        ])
        with pytest.raises(DedupSkipped):
            await facade.store(make_fact_assertion())
        # inc_store must NOT have been called — dedup has its own metric.
        assert all(
            call.args[:2] != ("store", "failure")
            for call in metrics.inc_store.call_args_list
        ), "DedupSkipped must not emit eb_memory_store_total{status=failure}"
        assert all(
            call.args[:2] != ("store", "success")
            for call in metrics.inc_store.call_args_list
        ), "DedupSkipped must not emit eb_memory_store_total{status=success}"

    async def test_delete_success_increments_success_status(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """Happy-path delete — status=success emitted exactly once."""
        from unittest.mock import MagicMock
        facade, graph, vector, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        metrics = MagicMock()
        facade._metrics = metrics
        fact = make_fact_assertion()
        graph.get_entity = AsyncMock(return_value=self._fact_props(fact))
        await facade.delete(fact.id)
        metrics.inc_store.assert_called_once_with("delete", "success")

    async def test_delete_fact_not_found_emits_failure_status_and_reraises(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """KeyError on missing fact → inc_store("delete","failure") +
        KeyError propagates. The route layer translates KeyError → 404."""
        from unittest.mock import MagicMock
        facade, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        metrics = MagicMock()
        facade._metrics = metrics
        graph.get_entity = AsyncMock(return_value=None)  # not found
        with pytest.raises(KeyError):
            await facade.delete(uuid.uuid4())
        metrics.inc_store.assert_called_once_with("delete", "failure")

    async def test_delete_permission_error_emits_failure_status_and_reraises(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """Cross-tenant delete → PermissionError + inc_store("delete","failure").
        AUTHORITY_CHECK_FAILED still emits (pre-existing behavior, C7); the
        new failure-metric emission is additive."""
        from unittest.mock import MagicMock
        facade, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        metrics = MagicMock()
        facade._metrics = metrics
        fact = make_fact_assertion()
        graph.get_entity = AsyncMock(return_value={
            **self._fact_props(fact), "gateway_id": "tenant-other",
        })
        with pytest.raises(PermissionError):
            await facade.delete(fact.id, caller_gateway_id="tenant-local")
        metrics.inc_store.assert_called_once_with("delete", "failure")

    # --- PR #5 C19: eb_memory_store_total for update() ---
    # update() was the third entry point on the store-total counter; pre-fix
    # it emitted neither success nor failure. The three tests below pin the
    # success path (metadata-only update, no cognee.add), the not-found
    # failure path (graph.get_entity returns None), and the cross-tenant
    # failure path (PermissionError). All three mirror the delete() matrix.

    async def test_update_success_increments_success_status(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """Metadata-only update (no text change) — status=success emitted once."""
        from unittest.mock import MagicMock
        facade, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        metrics = MagicMock()
        facade._metrics = metrics
        fact = make_fact_assertion()
        graph.get_entity = AsyncMock(return_value=self._fact_props(fact))
        await facade.update(fact.id, {"confidence": 0.77})
        metrics.inc_store.assert_called_once_with("update", "success")

    async def test_update_fact_not_found_emits_failure_status_and_reraises(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """KeyError on missing fact → inc_store("update","failure") + KeyError
        propagates. The route layer translates KeyError → 404."""
        from unittest.mock import MagicMock
        facade, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        metrics = MagicMock()
        facade._metrics = metrics
        graph.get_entity = AsyncMock(return_value=None)
        with pytest.raises(KeyError):
            await facade.update(uuid.uuid4(), {"confidence": 0.5})
        metrics.inc_store.assert_called_once_with("update", "failure")

    async def test_update_permission_error_emits_failure_status_and_reraises(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """Cross-tenant update → PermissionError + inc_store("update","failure").
        AUTHORITY_CHECK_FAILED still emits (pre-existing behavior, C7); the
        new failure-metric emission is additive."""
        from unittest.mock import MagicMock
        facade, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        metrics = MagicMock()
        facade._metrics = metrics
        fact = make_fact_assertion()
        graph.get_entity = AsyncMock(return_value={
            **self._fact_props(fact), "gateway_id": "tenant-other",
        })
        with pytest.raises(PermissionError):
            await facade.update(
                fact.id, {"text": "cross-tenant"}, caller_gateway_id="tenant-local",
            )
        metrics.inc_store.assert_called_once_with("update", "failure")

    # --- PR #5 TODO 5-504: RETRIEVAL_PERFORMED payload auto_recall field ---
    # PROGRAM.md §5.3 requires auto_recall in the trace payload so auditors
    # can distinguish explicit-search calls from before_prompt_build
    # auto-recalls without cross-referencing the metrics stream. Pre-fix
    # the field was missing on facade.search's RETRIEVAL_PERFORMED event.

    async def test_search_retrieval_performed_payload_carries_auto_recall_true(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        facade, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        ledger = facade._trace
        graph.query_cypher = AsyncMock(return_value=[])
        await facade.search("q", profile_name="coding", auto_recall=True)
        from elephantbroker.schemas.trace import TraceEventType, TraceQuery
        events = await ledger.query_trace(
            TraceQuery(event_types=[TraceEventType.RETRIEVAL_PERFORMED]),
        )
        assert len(events) == 1
        assert events[0].payload["auto_recall"] is True

    async def test_search_retrieval_performed_payload_carries_auto_recall_false(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        facade, graph, *_ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        ledger = facade._trace
        graph.query_cypher = AsyncMock(return_value=[])
        await facade.search("q", profile_name="coding")  # auto_recall defaults False
        from elephantbroker.schemas.trace import TraceEventType, TraceQuery
        events = await ledger.query_trace(
            TraceQuery(event_types=[TraceEventType.RETRIEVAL_PERFORMED]),
        )
        assert len(events) == 1
        assert events[0].payload["auto_recall"] is False

    # 5-205: Stage 1 (semantic) exception observability —
    # log + metric + DEGRADED_OPERATION trace; search still returns a list.

    async def test_search_stage1_exception_emits_metric_trace_log(
        self, monkeypatch, mock_add_data_points, mock_cognee, caplog,
    ):
        """Stage 1 (cognee semantic) raises → metric emitted with
        (stage="semantic", exception_type), DEGRADED_OPERATION trace
        emitted with matching payload, WARNING log emitted with gateway
        + query context. Final result is still a list (empty here)."""
        from unittest.mock import MagicMock
        facade, graph, *_ = self._make()
        facade._gateway_id = "gw-test"
        metrics = MagicMock()
        facade._metrics = metrics
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        mock_cognee.search = AsyncMock(side_effect=RuntimeError("cognee down"))
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        graph.query_cypher = AsyncMock(return_value=[])

        with caplog.at_level("WARNING", logger="elephantbroker.memory.facade"):
            results = await facade.search("test query", scope=Scope.SESSION)

        assert isinstance(results, list)  # downgrade preserved
        metrics.inc_search_stage_failure.assert_called_once_with("semantic", "RuntimeError")

        from elephantbroker.schemas.trace import TraceEventType, TraceQuery
        degraded = await facade._trace.query_trace(
            TraceQuery(event_types=[TraceEventType.DEGRADED_OPERATION]),
        )
        assert len(degraded) == 1
        payload = degraded[0].payload
        assert payload["component"] == "memory_facade"
        assert payload["operation"] == "search"
        assert payload["failure"] == "stage_exception"
        assert payload["stage"] == "semantic"
        assert payload["exception_type"] == "RuntimeError"
        assert "cognee down" in payload["exception"]

        # WARNING log captured with context.
        warnings = [r for r in caplog.records if r.levelname == "WARNING"]
        assert any("Stage 1" in r.getMessage() for r in warnings)

    async def test_search_stage1_exception_still_yields_structural_results(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """Stage 1 raises but Stage 2 (structural Cypher) returns a row —
        search returns the structural hit rather than empty."""
        from unittest.mock import MagicMock
        facade, graph, *_ = self._make()
        facade._metrics = MagicMock()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        mock_cognee.search = AsyncMock(side_effect=RuntimeError("boom"))
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion()
        graph.query_cypher = AsyncMock(return_value=[{
            "props": self._fact_props(fact),
        }])
        results = await facade.search("q", scope=Scope.SESSION)
        assert len(results) == 1
        assert str(results[0].id) == str(fact.id)

    async def test_search_stage1_success_does_not_emit_degraded_metric_trace(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """Happy path (no Stage 1 exception) must not emit the metric or
        DEGRADED_OPERATION trace — regression guard against spurious
        emission on success."""
        from unittest.mock import MagicMock
        facade, graph, *_ = self._make()
        metrics = MagicMock()
        facade._metrics = metrics
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        graph.query_cypher = AsyncMock(return_value=[])

        await facade.search("q")

        metrics.inc_search_stage_failure.assert_not_called()
        from elephantbroker.schemas.trace import TraceEventType, TraceQuery
        degraded = await facade._trace.query_trace(
            TraceQuery(event_types=[TraceEventType.DEGRADED_OPERATION]),
        )
        assert degraded == []


class TestCogneeDataIdCaptureObservability:
    """TD-50 capture-failure observability (PR #5 TODO 5-301).

    When cognee.add() returns a shape the facade cannot extract a data_id
    from, the fact is persisted with cognee_data_id=None and the delete
    cascade will miss the Cognee-side artifacts. The previous code only
    logged a warning — no metric, no trace event, effectively silent.
    These tests pin the three observability surfaces:
      1. `eb_cognee_data_id_capture_failures_total` counter increments
         (via MetricsContext.inc_cognee_capture_failure).
      2. DEGRADED_OPERATION trace event with component=memory_facade,
         operation=store|update, failure=cognee_data_id_capture.
      3. Existing WARNING log is preserved.
    The capture-success path must NOT emit any of the three.
    """

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

    async def test_store_capture_failure_emits_metric_and_trace(
        self, monkeypatch, mock_add_data_points, mock_cognee, caplog,
    ):
        """cognee.add() returns None → metric + DEGRADED_OPERATION trace +
        WARNING log; fact persisted with cognee_data_id=None so the rest of
        the store flow still completes."""
        from unittest.mock import MagicMock
        facade, _, _, _, ledger = self._make()
        metrics = MagicMock()
        facade._metrics = metrics
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        # Malformed cognee return — missing data_ingestion_info entirely.
        mock_cognee.add = AsyncMock(return_value=None)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)

        fact = make_fact_assertion()
        with caplog.at_level("WARNING", logger="elephantbroker.memory.facade"):
            result = await facade.store(fact)

        # Behaviour: fact is still stored, persisted DataPoint has
        # cognee_data_id=None (storage-backend id could not be captured).
        # TODO-5-307: FactAssertion no longer carries the field.
        assert result.id == fact.id
        assert len(mock_add_data_points.calls) == 1
        persisted_dp = mock_add_data_points.calls[0]["data_points"][0]
        assert persisted_dp.cognee_data_id is None

        # Metric emitted with operation=store.
        metrics.inc_cognee_capture_failure.assert_called_once_with("store")

        # Trace event emitted with expected payload.
        from elephantbroker.schemas.trace import TraceEventType, TraceQuery
        events = await ledger.query_trace(
            TraceQuery(event_types=[TraceEventType.DEGRADED_OPERATION]),
        )
        assert len(events) == 1
        payload = events[0].payload
        assert payload["component"] == "memory_facade"
        assert payload["operation"] == "store"
        assert payload["failure"] == "cognee_data_id_capture"
        assert payload["fact_id"] == str(fact.id)
        assert payload["exception_type"] in {"AttributeError", "TypeError"}

        # Log line preserved.
        assert any(
            "cognee_data_id" in rec.message and str(fact.id) in rec.message
            for rec in caplog.records
        ), "capture failure must still emit a WARNING log"

    async def test_update_capture_failure_emits_metric_and_trace(
        self, monkeypatch, mock_add_data_points, mock_cognee, caplog,
    ):
        """update(text=...) re-ingest where cognee.add() returns malformed
        shape → metric(op=update) + DEGRADED_OPERATION(operation=update);
        persisted DataPoint.cognee_data_id reset to None. The OLD doc is
        still cascaded because the old text is stale regardless — the
        orphan we cannot reach is the NEW (never-captured) doc, not the
        OLD one. TODO-5-307: storage-backend id no longer on FactAssertion."""
        from unittest.mock import MagicMock
        facade, graph, _, _, ledger = self._make()
        metrics = MagicMock()
        facade._metrics = metrics
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)

        existing_data_id = uuid.uuid4()
        fact = make_fact_assertion()
        graph.get_entity = AsyncMock(return_value=self._fact_props(
            fact, cognee_data_id=str(existing_data_id),
        ))
        # Malformed return on the update re-ingest path.
        mock_cognee.add = AsyncMock(return_value=None)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)

        cascade_spy = AsyncMock()
        monkeypatch.setattr(facade, "_cascade_cognee_data", cascade_spy)

        with caplog.at_level("WARNING", logger="elephantbroker.memory.facade"):
            await facade.update(fact.id, {"text": "rewritten text"})

        # Behaviour: update proceeds, persisted DP.cognee_data_id is None.
        assert len(mock_add_data_points.calls) == 1
        persisted_dp = mock_add_data_points.calls[0]["data_points"][0]
        assert persisted_dp.cognee_data_id is None
        # OLD data_id is still cascaded (old text is stale regardless of
        # whether we captured a new data_id).
        cascade_spy.assert_called_once()
        assert cascade_spy.call_args[0][0] == existing_data_id
        assert cascade_spy.call_args.kwargs["context"] == "update_text_change"

        metrics.inc_cognee_capture_failure.assert_called_once_with("update")

        from elephantbroker.schemas.trace import TraceEventType, TraceQuery
        events = await ledger.query_trace(
            TraceQuery(event_types=[TraceEventType.DEGRADED_OPERATION]),
        )
        assert len(events) == 1
        payload = events[0].payload
        assert payload["component"] == "memory_facade"
        assert payload["operation"] == "update"
        assert payload["failure"] == "cognee_data_id_capture"
        assert payload["fact_id"] == str(fact.id)

        assert any(
            "cognee_data_id" in rec.message and str(fact.id) in rec.message
            for rec in caplog.records
        ), "update capture failure must still emit a WARNING log"

    async def test_store_capture_success_emits_no_degraded_op(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """Happy path: cognee.add() returns a well-formed result →
        cognee_data_id captured, metric NOT incremented, no
        DEGRADED_OPERATION trace event. Pins the absence so a future
        regression that always fires the metric is caught."""
        from types import SimpleNamespace
        from unittest.mock import MagicMock
        facade, _, _, _, ledger = self._make()
        metrics = MagicMock()
        facade._metrics = metrics
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        returned_data_id = uuid.uuid4()
        mock_cognee.add = AsyncMock(return_value=SimpleNamespace(
            data_ingestion_info=[{"data_id": returned_data_id}],
        ))
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)

        fact = make_fact_assertion()
        await facade.store(fact)

        # TODO-5-307: the captured id lands on the persisted DataPoint,
        # not on FactAssertion.
        assert len(mock_add_data_points.calls) == 1
        persisted_dp = mock_add_data_points.calls[0]["data_points"][0]
        assert persisted_dp.cognee_data_id == str(returned_data_id)
        metrics.inc_cognee_capture_failure.assert_not_called()

        from elephantbroker.schemas.trace import TraceEventType, TraceQuery
        events = await ledger.query_trace(
            TraceQuery(event_types=[TraceEventType.DEGRADED_OPERATION]),
        )
        assert events == [], "success path must not emit DEGRADED_OPERATION"

    async def test_store_capture_failure_on_non_uuid_data_id(
        self, monkeypatch, mock_add_data_points, mock_cognee, caplog,
    ):
        """TODO-5-003 / TODO-5-211: cognee.add() returns a data_id that is
        NOT UUID-parseable → the UUID coercion raises ValueError, which is
        routed through _emit_capture_failure exactly like a shape mismatch.
        Fact persisted with cognee_data_id=None; metric + DEGRADED_OPERATION
        still fire. Pre-fix the except tuple omitted ValueError, so this
        path crashed store() with an unhandled exception."""
        from types import SimpleNamespace
        from unittest.mock import MagicMock
        facade, _, _, _, ledger = self._make()
        metrics = MagicMock()
        facade._metrics = metrics
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        mock_cognee.add = AsyncMock(return_value=SimpleNamespace(
            data_ingestion_info=[{"data_id": "not-a-uuid-at-all"}],
        ))
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)

        fact = make_fact_assertion()
        with caplog.at_level("WARNING", logger="elephantbroker.memory.facade"):
            result = await facade.store(fact)

        # TODO-5-307: assert against persisted DataPoint, not FactAssertion.
        assert result.id == fact.id
        assert len(mock_add_data_points.calls) == 1
        persisted_dp = mock_add_data_points.calls[0]["data_points"][0]
        assert persisted_dp.cognee_data_id is None
        metrics.inc_cognee_capture_failure.assert_called_once_with("store")

        from elephantbroker.schemas.trace import TraceEventType, TraceQuery
        events = await ledger.query_trace(
            TraceQuery(event_types=[TraceEventType.DEGRADED_OPERATION]),
        )
        assert len(events) == 1
        assert events[0].payload["exception_type"] == "ValueError"
        assert events[0].payload["failure"] == "cognee_data_id_capture"

    async def test_store_coerces_string_uuid_data_id_to_uuid(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """TODO-5-003: cognee.add() returns data_id as a UUID-parseable
        STRING (not a uuid.UUID instance) → capture coerces to uuid.UUID
        so downstream code sees a guaranteed-type value. Pre-fix the
        string leaked through and later failed at cascade parse time."""
        from types import SimpleNamespace
        from unittest.mock import MagicMock
        facade, _, _, _, _ = self._make()
        metrics = MagicMock()
        facade._metrics = metrics
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        canonical_id = uuid.uuid4()
        mock_cognee.add = AsyncMock(return_value=SimpleNamespace(
            data_ingestion_info=[{"data_id": str(canonical_id)}],
        ))
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)

        fact = make_fact_assertion()
        await facade.store(fact)

        # TODO-5-307: DataPoint stores cognee_data_id as `str | None`.
        # Capture coerces to uuid.UUID internally (guaranteeing parseability)
        # and re-stringifies for persistence. The canonical-id round-trip
        # proves the coercion happened (a bad string would not survive it).
        assert len(mock_add_data_points.calls) == 1
        persisted_dp = mock_add_data_points.calls[0]["data_points"][0]
        assert persisted_dp.cognee_data_id == str(canonical_id)
        metrics.inc_cognee_capture_failure.assert_not_called()

    async def test_update_capture_failure_on_non_uuid_data_id(
        self, monkeypatch, mock_add_data_points, mock_cognee, caplog,
    ):
        """TODO-5-003 / TODO-5-211: update(text=...) re-ingest returns a
        non-UUID-parseable data_id → ValueError routed through
        _emit_capture_failure, persisted DataPoint.cognee_data_id reset
        to None, old doc still cascades (old text is stale regardless).
        TODO-5-307: storage-backend id no longer on FactAssertion."""
        from types import SimpleNamespace
        from unittest.mock import MagicMock
        facade, graph, _, _, ledger = self._make()
        metrics = MagicMock()
        facade._metrics = metrics
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)

        existing_data_id = uuid.uuid4()
        fact = make_fact_assertion()
        graph.get_entity = AsyncMock(return_value=self._fact_props(
            fact, cognee_data_id=str(existing_data_id),
        ))
        mock_cognee.add = AsyncMock(return_value=SimpleNamespace(
            data_ingestion_info=[{"data_id": "definitely-not-a-uuid"}],
        ))
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        cascade_spy = AsyncMock()
        monkeypatch.setattr(facade, "_cascade_cognee_data", cascade_spy)

        with caplog.at_level("WARNING", logger="elephantbroker.memory.facade"):
            await facade.update(fact.id, {"text": "rewritten text"})

        assert len(mock_add_data_points.calls) == 1
        persisted_dp = mock_add_data_points.calls[0]["data_points"][0]
        assert persisted_dp.cognee_data_id is None
        cascade_spy.assert_called_once()
        metrics.inc_cognee_capture_failure.assert_called_once_with("update")

        from elephantbroker.schemas.trace import TraceEventType, TraceQuery
        events = await ledger.query_trace(
            TraceQuery(event_types=[TraceEventType.DEGRADED_OPERATION]),
        )
        assert any(
            e.payload.get("failure") == "cognee_data_id_capture"
            and e.payload.get("exception_type") == "ValueError"
            for e in events
        ), "update capture ValueError must surface as cognee_data_id_capture"


class TestCascadeCogneeDataGuards:
    """TODO-5-109 + TODO-5-309: explicit guards in _cascade_cognee_data.

    Two code-hygiene guards verified here:
      - "skipped_no_dataset" fires when get_datasets_by_name returns [],
        confirming datasets[0].id indexing is unreachable on empty input
        (TODO-5-309 safety via pre-existing `if not datasets:` guard).
      - "skipped_bad_data_id" fires when the stored cognee_data_id is
        not UUID-parseable (TODO-5-109), distinguishing legacy/corrupted
        data from a Cognee-side delete failure. Pre-5-109 both paths
        collapsed to "failed" and the distinction was lost.
    """

    def _make(self):
        graph = AsyncMock()
        vector = AsyncMock()
        embeddings = AsyncMock()
        embeddings.embed_text = AsyncMock(return_value=[0.1] * 1024)
        ledger = TraceLedger()
        return MemoryStoreFacade(graph, vector, embeddings, ledger, dataset_name="test_ds")

    async def test_cascade_returns_skipped_no_dataset_when_empty(
        self, monkeypatch, mock_cognee,
    ):
        """TODO-5-309: get_datasets_by_name returns [] → cascade returns
        'skipped_no_dataset' and NEVER reaches datasets[0].id. Pins the
        indexing safety — an empty list can never IndexError here."""
        facade = self._make()
        fake_user = type("U", (), {"id": uuid.uuid4()})()
        monkeypatch.setattr(
            "elephantbroker.runtime.memory.cascade_helper.get_default_user",
            AsyncMock(return_value=fake_user),
        )
        monkeypatch.setattr(
            "elephantbroker.runtime.memory.cascade_helper.get_datasets_by_name",
            AsyncMock(return_value=[]),
        )
        mock_cognee.datasets.delete_data = AsyncMock()
        monkeypatch.setattr("elephantbroker.runtime.memory.cascade_helper.cognee", mock_cognee)

        status = await facade._cascade_cognee_data(
            uuid.uuid4(), fact_id=uuid.uuid4(), context="test",
        )
        assert status == "skipped_no_dataset"
        mock_cognee.datasets.delete_data.assert_not_called()

    async def test_cascade_returns_skipped_bad_data_id_on_non_uuid(
        self, monkeypatch, mock_cognee,
    ):
        """TODO-5-109: stored cognee_data_id is a non-UUID-parseable
        string (legacy row from before TODO-5-003 coercion) → cascade
        returns 'skipped_bad_data_id' WITHOUT attempting the Cognee call.
        Distinct from 'failed' so operators can tell bad-data-at-rest
        from a Cognee-side failure."""
        facade = self._make()
        fake_user = type("U", (), {"id": uuid.uuid4()})()
        fake_ds = type("D", (), {"id": uuid.uuid4()})()
        monkeypatch.setattr(
            "elephantbroker.runtime.memory.cascade_helper.get_default_user",
            AsyncMock(return_value=fake_user),
        )
        monkeypatch.setattr(
            "elephantbroker.runtime.memory.cascade_helper.get_datasets_by_name",
            AsyncMock(return_value=[fake_ds]),
        )
        mock_cognee.datasets.delete_data = AsyncMock()
        monkeypatch.setattr("elephantbroker.runtime.memory.cascade_helper.cognee", mock_cognee)

        status = await facade._cascade_cognee_data(
            "this-is-not-a-uuid", fact_id=uuid.uuid4(), context="test",
        )
        assert status == "skipped_bad_data_id"
        mock_cognee.datasets.delete_data.assert_not_called()

    async def test_cascade_ok_on_uuid_instance_or_parseable_string(
        self, monkeypatch, mock_cognee,
    ):
        """Happy path: both a real uuid.UUID instance and a parseable
        string coerce cleanly and reach the Cognee delete call. Guards
        against a regression that would reject strings by mistake."""
        facade = self._make()
        fake_user = type("U", (), {"id": uuid.uuid4()})()
        fake_ds = type("D", (), {"id": uuid.uuid4()})()
        monkeypatch.setattr(
            "elephantbroker.runtime.memory.cascade_helper.get_default_user",
            AsyncMock(return_value=fake_user),
        )
        monkeypatch.setattr(
            "elephantbroker.runtime.memory.cascade_helper.get_datasets_by_name",
            AsyncMock(return_value=[fake_ds]),
        )
        mock_cognee.datasets.delete_data = AsyncMock(return_value={"deleted": True})
        monkeypatch.setattr("elephantbroker.runtime.memory.cascade_helper.cognee", mock_cognee)

        data_id = uuid.uuid4()
        status_from_uuid = await facade._cascade_cognee_data(
            data_id, fact_id=uuid.uuid4(), context="test",
        )
        status_from_str = await facade._cascade_cognee_data(
            str(data_id), fact_id=uuid.uuid4(), context="test",
        )
        assert status_from_uuid == "ok"
        assert status_from_str == "ok"
        assert mock_cognee.datasets.delete_data.await_count == 2

    # --- TD-Cognee-Qdrant-404 (Cluster Cfx) ---
    # Cognee 0.5.3's delete_from_graph_and_vector calls
    # vector_engine.delete_data_points without a has_collection guard.
    # When a Data row is added but never cognify()'d, the derived Qdrant
    # collection doesn't exist → 404 → UnexpectedResponse → outer
    # delete_data aborts before removing the Data ↔ Dataset association.
    # _cascade_cognee_data now classifies that specific shape as benign
    # and manually completes the metadata removal, returning the new
    # "ok_idempotent" cascade_status. Non-404 UnexpectedResponse and any
    # raise from the recovery path fall through to "failed".
    # See local/TECHNICAL-DEBT.md §TD-Cognee-Qdrant-404 for removal
    # criteria once the upstream fix ships.

    async def test_cascade_recovers_from_qdrant_404_on_delete(
        self, monkeypatch, mock_cognee,
    ):
        """Qdrant 404 from inside cognee.datasets.delete_data → cascade
        manually fetches the Data row via get_dataset_data, completes the
        Data ↔ Dataset unbind via the inner delete_data method, and
        returns 'ok_idempotent' (NOT 'failed'). This is the TD-Cognee-
        Qdrant-404 upstream-bug workaround that unblocks the TD-50
        integration test."""
        from qdrant_client.http.exceptions import UnexpectedResponse
        from httpx import Headers

        facade = self._make()
        fake_user = type("U", (), {"id": uuid.uuid4()})()
        fake_ds_id = uuid.uuid4()
        fake_ds = type("D", (), {"id": fake_ds_id})()
        data_id = uuid.uuid4()
        fake_data_row = type("Data", (), {
            "id": data_id,
            "__tablename__": "data",
        })()

        monkeypatch.setattr(
            "elephantbroker.runtime.memory.cascade_helper.get_default_user",
            AsyncMock(return_value=fake_user),
        )
        monkeypatch.setattr(
            "elephantbroker.runtime.memory.cascade_helper.get_datasets_by_name",
            AsyncMock(return_value=[fake_ds]),
        )
        # Inner Qdrant delete raises the exact shape we see in prod.
        qdrant_404 = UnexpectedResponse(
            status_code=404,
            reason_phrase="Not Found",
            content=b'{"status":{"error":"Collection not found"}}',
            headers=Headers({}),
        )
        mock_cognee.datasets.delete_data = AsyncMock(side_effect=qdrant_404)
        monkeypatch.setattr("elephantbroker.runtime.memory.cascade_helper.cognee", mock_cognee)

        # Recovery imports: get_dataset_data returns the Data row for our
        # data_id; _delete_data_row is the inner Cognee method that Cognee's
        # outer delete_data would have called on its last line.
        get_dataset_data_mock = AsyncMock(return_value=[fake_data_row])
        delete_data_row_mock = AsyncMock(return_value=None)
        monkeypatch.setattr(
            "elephantbroker.runtime.memory.cascade_helper.get_dataset_data",
            get_dataset_data_mock,
        )
        monkeypatch.setattr(
            "elephantbroker.runtime.memory.cascade_helper._delete_data_row",
            delete_data_row_mock,
        )

        status = await facade._cascade_cognee_data(
            data_id, fact_id=uuid.uuid4(), context="update_text_change",
        )

        assert status == "ok_idempotent", (
            "TD-Cognee-Qdrant-404: 404 from inner Qdrant delete must be "
            "treated as benign-idempotent, not 'failed'."
        )
        # Recovery path actually executed the metadata-unbind.
        get_dataset_data_mock.assert_awaited_once_with(fake_ds_id)
        delete_data_row_mock.assert_awaited_once_with(fake_data_row, fake_ds_id)

    async def test_cascade_returns_failed_when_qdrant_non_404(
        self, monkeypatch, mock_cognee,
    ):
        """Non-404 UnexpectedResponse (e.g. Qdrant 5xx) is NOT benign —
        the recovery branch is skipped and the outer broad except reports
        'failed'. Prevents the workaround from masking genuine Qdrant
        failures."""
        from qdrant_client.http.exceptions import UnexpectedResponse
        from httpx import Headers

        facade = self._make()
        fake_user = type("U", (), {"id": uuid.uuid4()})()
        fake_ds = type("D", (), {"id": uuid.uuid4()})()
        monkeypatch.setattr(
            "elephantbroker.runtime.memory.cascade_helper.get_default_user",
            AsyncMock(return_value=fake_user),
        )
        monkeypatch.setattr(
            "elephantbroker.runtime.memory.cascade_helper.get_datasets_by_name",
            AsyncMock(return_value=[fake_ds]),
        )
        qdrant_503 = UnexpectedResponse(
            status_code=503,
            reason_phrase="Service Unavailable",
            content=b'{"error":"qdrant down"}',
            headers=Headers({}),
        )
        mock_cognee.datasets.delete_data = AsyncMock(side_effect=qdrant_503)
        monkeypatch.setattr("elephantbroker.runtime.memory.cascade_helper.cognee", mock_cognee)

        # Recovery helpers should NOT be called on non-404.
        get_dataset_data_mock = AsyncMock()
        delete_data_row_mock = AsyncMock()
        monkeypatch.setattr(
            "elephantbroker.runtime.memory.cascade_helper.get_dataset_data",
            get_dataset_data_mock,
        )
        monkeypatch.setattr(
            "elephantbroker.runtime.memory.cascade_helper._delete_data_row",
            delete_data_row_mock,
        )

        status = await facade._cascade_cognee_data(
            uuid.uuid4(), fact_id=uuid.uuid4(), context="test",
        )
        assert status == "failed"
        get_dataset_data_mock.assert_not_awaited()
        delete_data_row_mock.assert_not_awaited()

    async def test_cascade_returns_failed_when_recovery_path_also_raises(
        self, monkeypatch, mock_cognee, caplog,
    ):
        """Negative companion: Qdrant 404 fires the recovery branch, but
        the manual metadata-unbind itself raises (e.g. relational DB down,
        Cognee internal signature drift). Cascade must surface 'failed'
        AND log both the outer UnexpectedResponse and the inner exception
        so post-mortem has the full picture."""
        import logging
        from qdrant_client.http.exceptions import UnexpectedResponse
        from httpx import Headers

        facade = self._make()
        fake_user = type("U", (), {"id": uuid.uuid4()})()
        fake_ds = type("D", (), {"id": uuid.uuid4()})()
        data_id = uuid.uuid4()
        fake_data_row = type("Data", (), {"id": data_id, "__tablename__": "data"})()

        monkeypatch.setattr(
            "elephantbroker.runtime.memory.cascade_helper.get_default_user",
            AsyncMock(return_value=fake_user),
        )
        monkeypatch.setattr(
            "elephantbroker.runtime.memory.cascade_helper.get_datasets_by_name",
            AsyncMock(return_value=[fake_ds]),
        )
        qdrant_404 = UnexpectedResponse(
            status_code=404,
            reason_phrase="Not Found",
            content=b"",
            headers=Headers({}),
        )
        mock_cognee.datasets.delete_data = AsyncMock(side_effect=qdrant_404)
        monkeypatch.setattr("elephantbroker.runtime.memory.cascade_helper.cognee", mock_cognee)

        # get_dataset_data succeeds, but _delete_data_row blows up —
        # simulates a relational-db failure or Cognee internal drift.
        inner_exc = RuntimeError("relational engine unavailable")
        monkeypatch.setattr(
            "elephantbroker.runtime.memory.cascade_helper.get_dataset_data",
            AsyncMock(return_value=[fake_data_row]),
        )
        monkeypatch.setattr(
            "elephantbroker.runtime.memory.cascade_helper._delete_data_row",
            AsyncMock(side_effect=inner_exc),
        )

        with caplog.at_level(logging.WARNING, logger="elephantbroker.memory.facade"):
            status = await facade._cascade_cognee_data(
                data_id, fact_id=uuid.uuid4(), context="update_text_change",
            )

        assert status == "failed"
        # Log must mention both the outer UnexpectedResponse and the inner
        # RuntimeError so an operator reading journald sees the full chain.
        joined = "\n".join(r.getMessage() for r in caplog.records)
        assert "recovery" in joined.lower()
        assert "relational engine unavailable" in joined


class TestCascadePointerPreservation:
    """TODO-5-008: MERGE-by-ID upserts must preserve cognee_data_id.

    Regression bundle for five call sites that previously invoked
    FactDataPoint.from_schema(fact) without the cognee_data_id kwarg.
    Post-C21, FactAssertion no longer carries the storage-backend id, so
    the default None wiped the graph property on MERGE — re-orphaning
    TD-50 cascades for any searched-then-mutated fact.

    Each test drives one call site, lets it MERGE, and asserts the DP
    actually passed to add_data_points carries the expected
    cognee_data_id (not None).
    """

    def _make(self, gateway_id: str = "gw-test"):
        graph = AsyncMock()
        vector = AsyncMock()
        embeddings = AsyncMock()
        embeddings.embed_text = AsyncMock(return_value=[0.1] * 1024)
        ledger = TraceLedger(gateway_id=gateway_id)
        facade = MemoryStoreFacade(
            graph, vector, embeddings, ledger,
            dataset_name="test_ds", gateway_id=gateway_id,
        )
        return facade, graph

    def _fact_props_with_data_id(self, fact, cognee_data_id: str, gateway_id: str = "gw-test"):
        return {
            "eb_id": str(fact.id), "text": fact.text, "category": "general",
            "scope": "session", "confidence": 1.0, "memory_class": "episodic",
            "eb_created_at": 0, "eb_updated_at": 0, "use_count": 0,
            "successful_use_count": 0, "provenance_refs": [], "target_actor_ids": [],
            "goal_ids": [], "gateway_id": gateway_id,
            "cognee_data_id": cognee_data_id,
        }

    # Site 1 — facade.py:_update_use_counts
    async def test_update_use_counts_preserves_cognee_data_id_via_batch_fetch(
        self, monkeypatch, mock_add_data_points,
    ):
        """_update_use_counts takes a FactAssertion list (no data_id in
        scope) → must batch-fetch cognee_data_id from the graph BEFORE
        MERGE so the on-graph property is not wiped."""
        facade, graph = self._make()
        monkeypatch.setattr(
            "elephantbroker.runtime.memory.facade.add_data_points",
            mock_add_data_points,
        )
        fact = make_fact_assertion()
        expected_data_id = str(uuid.uuid4())
        graph.query_cypher = AsyncMock(return_value=[
            {"eb_id": str(fact.id), "cognee_data_id": expected_data_id},
        ])

        await facade._update_use_counts([fact])

        assert len(mock_add_data_points.calls) == 1
        stored_dp = mock_add_data_points.calls[0]["data_points"][0]
        assert stored_dp.cognee_data_id == expected_data_id, (
            "MERGE dropped the cascade pointer — would re-orphan TD-50 "
            "on any later delete of this fact."
        )

    # Site 2 — facade.py:promote_scope
    async def test_promote_scope_preserves_cognee_data_id_through_merge(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """promote_scope holds a DP in scope after clean_graph_props →
        FactDataPoint(**props). The dp.cognee_data_id must be forwarded
        into the rebuilt DP or the MERGE wipes the graph property."""
        facade, graph = self._make()
        monkeypatch.setattr(
            "elephantbroker.runtime.memory.facade.add_data_points",
            mock_add_data_points,
        )
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion()
        expected_data_id = str(uuid.uuid4())
        graph.get_entity = AsyncMock(
            return_value=self._fact_props_with_data_id(fact, expected_data_id),
        )

        await facade.promote_scope(fact.id, Scope.GLOBAL)

        assert len(mock_add_data_points.calls) == 1
        stored_dp = mock_add_data_points.calls[0]["data_points"][0]
        assert stored_dp.cognee_data_id == expected_data_id

    # Site 3 — facade.py:promote_class
    async def test_promote_class_preserves_cognee_data_id_through_merge(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """Same contract as promote_scope — memory_class mutation path
        must not wipe the cascade pointer."""
        facade, graph = self._make()
        monkeypatch.setattr(
            "elephantbroker.runtime.memory.facade.add_data_points",
            mock_add_data_points,
        )
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion()
        expected_data_id = str(uuid.uuid4())
        graph.get_entity = AsyncMock(
            return_value=self._fact_props_with_data_id(fact, expected_data_id),
        )

        await facade.promote_class(fact.id, MemoryClass.SEMANTIC)

        assert len(mock_add_data_points.calls) == 1
        stored_dp = mock_add_data_points.calls[0]["data_points"][0]
        assert stored_dp.cognee_data_id == expected_data_id

    # Site 4 — facade.py:decay
    async def test_decay_preserves_cognee_data_id_through_merge(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """Same contract as promote_scope — confidence-decay path must
        not wipe the cascade pointer."""
        facade, graph = self._make()
        monkeypatch.setattr(
            "elephantbroker.runtime.memory.facade.add_data_points",
            mock_add_data_points,
        )
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion(confidence=0.8)
        expected_data_id = str(uuid.uuid4())
        graph.get_entity = AsyncMock(
            return_value=self._fact_props_with_data_id(
                fact, expected_data_id,
            ) | {"confidence": 0.8},
        )

        await facade.decay(fact.id, 0.5)

        assert len(mock_add_data_points.calls) == 1
        stored_dp = mock_add_data_points.calls[0]["data_points"][0]
        assert stored_dp.cognee_data_id == expected_data_id
