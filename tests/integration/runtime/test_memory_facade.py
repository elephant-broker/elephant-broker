"""Integration tests for MemoryStoreFacade with real Neo4j + Qdrant."""
from __future__ import annotations

import pytest

from elephantbroker.schemas.base import Scope
from tests.fixtures.factories import make_fact_assertion


@pytest.mark.integration
class TestMemoryStoreFacadeIntegration:
    async def test_store_fact_to_graph_and_vector(self, memory_facade):
        fact = make_fact_assertion()
        result = await memory_facade.store(fact)
        assert result.id == fact.id

    async def test_search_returns_stored_fact(self, memory_facade):
        fact = make_fact_assertion(text="The capital of France is Paris", scope=Scope.SESSION)
        await memory_facade.store(fact)
        # Use scope filter so structural Cypher fallback fires even when
        # GRAPH_COMPLETION fails (empty graph before cognify()).
        results = await memory_facade.search("capital of France", max_results=5, scope=Scope.SESSION)
        assert len(results) >= 1
        assert any("Paris" in r.text for r in results)

    async def test_store_fact_then_auto_recall_returns_it(self, memory_facade, retrieval_orchestrator):
        # TD-60 + TD-61 regression guard: a fact stored under session-A must
        # surface when the orchestrator runs under session-B with
        # auto_recall=True. Pre-TD-61 the post-retrieval SESSION_KEY
        # isolation filter silently dropped all cross-session vector hits
        # regardless of auto_recall, breaking before_agent_start recall.
        # Exercises retrieve_candidates directly — memory_facade.search uses
        # a different code path that does not apply the isolation filter.
        fact = make_fact_assertion(
            text="Project codename PELICAN uses TypeScript on the frontend",
            scope=Scope.SESSION,
            session_key="session-A",
        )
        await memory_facade.store(fact)

        candidates = await retrieval_orchestrator.retrieve_candidates(
            query="PELICAN project frontend TypeScript",
            session_key="session-B",
            auto_recall=True,
        )

        fact_ids = [str(c.fact.id) for c in candidates]
        assert str(fact.id) in fact_ids, (
            f"Fact {fact.id} stored under session-A should surface under "
            f"session-B with auto_recall=True; got {len(candidates)} "
            f"candidates: {fact_ids}"
        )

    async def test_promote_updates_scope_in_graph(self, memory_facade):
        fact = make_fact_assertion(scope=Scope.SESSION)
        await memory_facade.store(fact)
        promoted = await memory_facade.promote(fact.id, Scope.GLOBAL)
        assert promoted.scope == Scope.GLOBAL

    async def test_decay_updates_confidence_in_graph(self, memory_facade):
        fact = make_fact_assertion(confidence=0.8)
        await memory_facade.store(fact)
        decayed = await memory_facade.decay(fact.id, 0.5)
        assert decayed.confidence == pytest.approx(0.4, abs=0.01)
