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
        #
        # TODO 5-405 alignment: both fixtures now share dataset_name
        # "test_integration". Pre-fix the orchestrator defaulted to
        # "elephantbroker" while MemoryStoreFacade stored into
        # "test_integration", so every Cognee-backed source (keyword/graph/
        # artifact) silently pointed at an empty dataset and returned zero
        # hits. The test still passed because the structural Cypher path
        # and the direct-Qdrant vector fallback do not depend on
        # dataset_name — so the regression guard only validated those two
        # paths. With the fixtures aligned, Cognee-backed sources now
        # target the correct dataset.
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

        # TODO 5-405: cross-source coverage check. The previous test only
        # validated surfacing via the structural Cypher path. Post-fix we
        # require at least one non-structural source to also produce
        # candidates — proving retrieval is exercising more than the
        # Cypher fallback. Accepts any Cognee-backed source ("keyword",
        # "graph", "artifact") or the vector path ("vector"); direct-
        # Qdrant fallback is also labeled "vector" and is a valid signal
        # that vector retrieval is actually running end-to-end.
        non_structural_sources = {c.source for c in candidates if c.source != "structural"}
        assert non_structural_sources, (
            f"Retrieval produced only structural candidates "
            f"({len(candidates)} total). Pre-5-405 the dataset_name "
            f"mismatch caused every Cognee-backed source to return empty; "
            f"post-fix at least one of keyword/graph/artifact/vector must "
            f"surface results. Candidate sources: "
            f"{sorted({c.source for c in candidates})}"
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

    async def test_update_text_change_cascades_old_and_delete_cascades_new(
        self, memory_facade,
    ):
        # TD-50 regression: update(text=...) must cascade the OLD cognee doc
        # (not just orphan it by losing the pointer), and a subsequent
        # delete() must cascade the NEW doc. After the full flow, the
        # dataset holds neither — confirming no orphan was left behind.
        #
        # TODO-5-307: cognee_data_id lives on the graph node only (not on
        # FactAssertion). Read it back from the graph after each facade op.
        import uuid as _uuid

        from cognee.modules.data.methods import get_dataset_data, get_datasets_by_name
        from cognee.modules.users.methods import get_default_user

        async def _read_cognee_data_id(fact_id) -> _uuid.UUID | None:
            entity = await memory_facade._graph.get_entity(str(fact_id))
            if not isinstance(entity, dict):
                return None
            raw = entity.get("cognee_data_id")
            return _uuid.UUID(str(raw)) if raw else None

        fact = make_fact_assertion(text="The capital of France is Paris")
        await memory_facade.store(fact)
        old_data_id = await _read_cognee_data_id(fact.id)
        assert old_data_id is not None, "store() must capture cognee_data_id"

        await memory_facade.update(
            fact.id, {"text": "The capital of Germany is Berlin"},
        )
        new_data_id = await _read_cognee_data_id(fact.id)
        assert new_data_id is not None, (
            "update(text=...) must capture a new cognee_data_id"
        )
        assert new_data_id != old_data_id, (
            "text change must refresh cognee_data_id to the new ingest"
        )

        user = await get_default_user()
        datasets = await get_datasets_by_name(["test_integration"], user.id)
        assert datasets, "test_integration dataset should exist after store()"
        dataset_id = datasets[0].id

        data_ids_after_update = {d.id for d in await get_dataset_data(dataset_id)}
        assert old_data_id not in data_ids_after_update, (
            f"TD-50 regression: OLD cognee doc {old_data_id} was orphaned by "
            f"update() — dataset still contains it alongside NEW {new_data_id}"
        )

        await memory_facade.delete(fact.id)

        data_ids_after_delete = {d.id for d in await get_dataset_data(dataset_id)}
        assert new_data_id not in data_ids_after_delete, (
            f"delete() left NEW cognee doc {new_data_id} orphaned in the dataset"
        )
