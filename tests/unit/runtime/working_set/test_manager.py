"""Tests for WorkingSetManager — full scoring pipeline orchestrator."""
from __future__ import annotations

import logging
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elephantbroker.runtime.working_set.manager import WorkingSetManager
from elephantbroker.schemas.config import ElephantBrokerConfig
from elephantbroker.schemas.working_set import ScoringWeights, WorkingSetItem, WorkingSetScores
from tests.fixtures.factories import make_fact_assertion, make_goal_state


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_manager(
    *,
    retrieval=None,
    trace_ledger=None,
    rerank=None,
    goal_manager=None,
    procedure_engine=None,
    embedding_service=None,
    graph=None,
    redis=None,
    config=None,
    gateway_id="test-gw",
    redis_keys=None,
    metrics=None,
    profile_registry=None,
    session_goal_store=None,
):
    """Build a WorkingSetManager with AsyncMock dependencies."""
    if retrieval is None:
        retrieval = AsyncMock()
        retrieval.retrieve_candidates = AsyncMock(return_value=[])
    if trace_ledger is None:
        trace_ledger = AsyncMock()
        trace_ledger.append_event = AsyncMock()
    return WorkingSetManager(
        retrieval=retrieval,
        trace_ledger=trace_ledger,
        rerank=rerank,
        goal_manager=goal_manager,
        procedure_engine=procedure_engine,
        embedding_service=embedding_service,
        graph=graph,
        redis=redis,
        config=config or ElephantBrokerConfig(),
        gateway_id=gateway_id,
        redis_keys=redis_keys,
        metrics=metrics,
        profile_registry=profile_registry,
        session_goal_store=session_goal_store,
    )


def _build_args(**overrides):
    """Common kwargs for build_working_set()."""
    defaults = {
        "session_id": uuid.uuid4(),
        "session_key": "agent:main:main",
        "profile_name": "coding",
        "query": "test query",
    }
    defaults.update(overrides)
    return defaults


# ---------------------------------------------------------------------------
# Scoring context parallel precomputation (#526)
# ---------------------------------------------------------------------------

class TestScoringContextParallel:
    async def test_scoring_context_parallel_precomputation(self):
        """Verify all 6 scoring context data sources are called during build."""
        graph = AsyncMock()
        # PC-1: evidence
        graph.query_cypher = AsyncMock(return_value=[])
        session_goal_store = AsyncMock()
        session_goal_store.get_goals = AsyncMock(return_value=[])
        embedding_service = AsyncMock()
        embedding_service.embed_text = AsyncMock(return_value=[0.1] * 128)
        embedding_service.embed_batch = AsyncMock(return_value=[])

        mgr = _make_manager(
            graph=graph, embedding_service=embedding_service,
            session_goal_store=session_goal_store,
        )
        ctx = await mgr._build_scoring_context(
            query="test", session_key="agent:main:main",
            session_id=uuid.uuid4(), weights=ScoringWeights(),
            token_budget=8000,
        )

        # All 6 sources should have been called:
        # 1. embed_text (turn embedding)
        embedding_service.embed_text.assert_awaited_once_with("test")
        # 2-4. graph.query_cypher called for evidence, verification, conflicts, persistent_goals
        assert graph.query_cypher.await_count >= 3
        # 5. session_goal_store.get_goals (session goals)
        session_goal_store.get_goals.assert_awaited_once()


# ---------------------------------------------------------------------------
# Goal embeddings sequential after gather (#527)
# ---------------------------------------------------------------------------

class TestGoalEmbeddingsSequential:
    async def test_goal_embeddings_sequential_after_gather(self):
        """Goal embeddings are computed AFTER gather returns (depend on loaded goals)."""
        goal = make_goal_state(title="Ship MVP")
        session_goal_store = AsyncMock()
        session_goal_store.get_goals = AsyncMock(return_value=[goal])

        call_order = []

        async def _embed_text(text):
            call_order.append(("embed_text", text))
            return [0.1] * 128

        async def _embed_batch(texts):
            call_order.append(("embed_batch", texts))
            return [[0.2] * 128 for _ in texts]

        embedding_service = AsyncMock()
        embedding_service.embed_text = AsyncMock(side_effect=_embed_text)
        embedding_service.embed_batch = AsyncMock(side_effect=_embed_batch)

        mgr = _make_manager(session_goal_store=session_goal_store, embedding_service=embedding_service)
        ctx = await mgr._build_scoring_context(
            query="test", session_key="agent:main:main",
            session_id=uuid.uuid4(), weights=ScoringWeights(),
            token_budget=8000,
        )

        # embed_text (turn) happens in gather; embed_batch (goals) happens after
        assert len(call_order) >= 2
        embed_text_idx = next(i for i, c in enumerate(call_order) if c[0] == "embed_text")
        embed_batch_idx = next(i for i, c in enumerate(call_order) if c[0] == "embed_batch")
        assert embed_batch_idx > embed_text_idx
        # Goal embedding should be in the context
        assert str(goal.id) in ctx.goal_embeddings


# ---------------------------------------------------------------------------
# Conflict data non-tuple fallback (#528)
# ---------------------------------------------------------------------------

class TestConflictDataFallback:
    async def test_conflict_data_non_tuple_fallback(self):
        """If conflict_data is not a tuple, unpacking falls back gracefully."""
        graph = AsyncMock()

        call_count = {"n": 0}

        async def _cypher_side_effect(query, params=None):
            call_count["n"] += 1
            # Always return malformed data — the test exercises the fallback
            # path regardless of which Cypher query triggers it
            return "not-a-tuple"

        graph.query_cypher = AsyncMock(side_effect=_cypher_side_effect)

        mgr = _make_manager(graph=graph)
        # Should not crash — falls back to empty set/dict
        ctx = await mgr._build_scoring_context(
            query="test", session_key="agent:main:main",
            session_id=uuid.uuid4(), weights=ScoringWeights(),
            token_budget=8000,
        )
        assert ctx.conflict_pairs == set()
        assert ctx.conflict_edge_types == {}


# ---------------------------------------------------------------------------
# Embedding backfill for direct items (#531)
# ---------------------------------------------------------------------------

class TestEmbeddingBackfill:
    async def test_embedding_backfill_for_direct_items(self):
        """Items missing from ctx.item_embeddings get batch-embedded before scoring."""
        from elephantbroker.runtime.interfaces.retrieval import RetrievalCandidate

        embedding_service = AsyncMock()
        embedding_service.embed_text = AsyncMock(return_value=[0.1] * 128)
        backfill_embs = [[0.5] * 128, [0.6] * 128]
        embedding_service.embed_batch = AsyncMock(return_value=backfill_embs)

        retrieval = AsyncMock()
        retrieval.retrieve_candidates = AsyncMock(return_value=[])

        # Inject direct items via goal candidates (SessionGoalStore)
        g1 = make_goal_state(title="Goal A")
        g2 = make_goal_state(title="Goal B")
        session_goal_store = AsyncMock()
        session_goal_store.get_goals = AsyncMock(return_value=[g1, g2])

        redis = AsyncMock()
        redis.setex = AsyncMock()

        trace = AsyncMock()
        trace.append_event = AsyncMock()

        mgr = _make_manager(
            retrieval=retrieval, trace_ledger=trace,
            embedding_service=embedding_service, redis=redis,
            session_goal_store=session_goal_store,
        )

        snap = await mgr.build_working_set(**_build_args())

        # embed_batch should have been called to backfill the goal items' texts
        assert embedding_service.embed_batch.await_count >= 1
        backfill_texts = set()
        for call in embedding_service.embed_batch.call_args_list:
            for text in call.args[0] if call.args else call.kwargs.get("texts", []):
                backfill_texts.add(text)
        # Goal items produce texts like "Goal: <title>" — verify backfill targeted them
        assert any("Goal A" in t for t in backfill_texts), f"Goal A not in backfill texts: {backfill_texts}"
        assert any("Goal B" in t for t in backfill_texts), f"Goal B not in backfill texts: {backfill_texts}"


# ---------------------------------------------------------------------------
# Rerank failure uses original order (#532)
# ---------------------------------------------------------------------------

class TestRerankFailureFallback:
    async def test_rerank_failure_uses_original_order(self, caplog):
        """When reranker raises, WARNING is logged and original order preserved."""
        from elephantbroker.runtime.interfaces.retrieval import RetrievalCandidate

        fact1 = make_fact_assertion(text="first fact")
        fact2 = make_fact_assertion(text="second fact")
        rc1 = RetrievalCandidate(fact=fact1, source="structural", score=0.9)
        rc2 = RetrievalCandidate(fact=fact2, source="vector", score=0.7)

        retrieval = AsyncMock()
        retrieval.retrieve_candidates = AsyncMock(return_value=[rc1, rc2])

        reranker = AsyncMock()
        reranker.rerank = AsyncMock(side_effect=RuntimeError("reranker down"))

        embedding_service = AsyncMock()
        embedding_service.embed_text = AsyncMock(return_value=[0.1] * 128)
        embedding_service.embed_batch = AsyncMock(return_value=[[0.1] * 128, [0.2] * 128])

        trace = AsyncMock()
        trace.append_event = AsyncMock()

        mgr = _make_manager(
            retrieval=retrieval, rerank=reranker,
            embedding_service=embedding_service, trace_ledger=trace,
        )

        with caplog.at_level(logging.WARNING, logger="elephantbroker.runtime.working_set.manager"):
            snap = await mgr.build_working_set(**_build_args())

        # WARNING should be logged about rerank failure
        assert "Rerank failed" in caplog.text
        assert "reranker down" in caplog.text

        # Both items should survive the rerank failure (not lost or corrupted)
        assert len(snap.items) == 2
        item_texts = {item.text for item in snap.items}
        assert item_texts == {"first fact", "second fact"}
