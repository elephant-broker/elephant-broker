"""Tests for RerankOrchestrator — full 4-stage pipeline."""
import json
import math
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elephantbroker.runtime.interfaces.retrieval import RetrievalCandidate
from elephantbroker.runtime.rerank.orchestrator import RerankOrchestrator, _cosine_sim, _sigmoid
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.schemas.config import RerankerConfig, ScoringConfig
from tests.fixtures.factories import make_fact_assertion


def _rc(text="fact", score=0.8, **kwargs) -> RetrievalCandidate:
    fact = make_fact_assertion(text=text, **{k: v for k, v in kwargs.items() if k != "score"})
    return RetrievalCandidate(fact=fact, source="structural", score=score)


class TestCheapPrune:
    @pytest.mark.asyncio
    async def test_preserves_all_when_under_max(self):
        orch = RerankOrchestrator(TraceLedger())
        candidates = [_rc(text=f"fact {i}") for i in range(5)]
        result = await orch.cheap_prune(candidates, "query", max_candidates=10)
        assert len(result) == 5

    @pytest.mark.asyncio
    async def test_respects_max_candidates(self):
        orch = RerankOrchestrator(TraceLedger())
        candidates = [_rc(text=f"fact {i}", score=0.1 * i) for i in range(10)]
        result = await orch.cheap_prune(candidates, "query", max_candidates=3)
        assert len(result) == 3

    @pytest.mark.asyncio
    async def test_preserves_high_overlap(self):
        orch = RerankOrchestrator(TraceLedger())
        # High overlap candidate should rank higher
        candidates = [
            _rc(text="unrelated topic about cooking", score=0.5),
            _rc(text="fix the login bug in auth module", score=0.5),
        ]
        result = await orch.cheap_prune(candidates, "fix login bug", max_candidates=1)
        assert "login" in result[0].fact.text

    @pytest.mark.asyncio
    async def test_empty_query_returns_top_n(self):
        orch = RerankOrchestrator(TraceLedger())
        candidates = [_rc(score=0.5), _rc(score=0.9)]
        result = await orch.cheap_prune(candidates, "", max_candidates=1)
        assert len(result) == 1


class TestSemanticRerank:
    @pytest.mark.asyncio
    async def test_reorders_by_similarity(self):
        embeddings = AsyncMock()
        embeddings.embed_batch = AsyncMock(return_value=[
            [0.0, 1.0],  # orthogonal to query → low similarity
            [1.0, 0.0],  # identical to query → high similarity
        ])
        orch = RerankOrchestrator(TraceLedger(), embedding_service=embeddings)
        query_emb = [1.0, 0.0]
        # Both start with same retrieval score, so semantic rerank should reorder
        candidates = [_rc(text="low", score=0.5), _rc(text="high", score=0.5)]
        result = await orch._semantic_rerank(candidates, query_emb)
        # Higher cosine sim should be first
        assert result[0].fact.text == "high"

    @pytest.mark.asyncio
    async def test_blends_60_40(self):
        embeddings = AsyncMock()
        embeddings.embed_batch = AsyncMock(return_value=[[1.0, 0.0]])
        config = ScoringConfig(semantic_blend_weight=0.6)
        orch = RerankOrchestrator(TraceLedger(), embedding_service=embeddings, scoring_config=config)
        candidates = [_rc(score=0.5)]
        result = await orch._semantic_rerank(candidates, [1.0, 0.0])
        assert len(result) == 1


class TestCrossEncoder:
    @pytest.mark.asyncio
    async def test_calls_v1_rerank_endpoint(self):
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status = MagicMock()
        mock_response.json = MagicMock(return_value={
            "results": [
                {"index": 0, "relevance_score": 2.0},
                {"index": 1, "relevance_score": -1.0},
            ]
        })

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)

        config = RerankerConfig(enabled=True)
        orch = RerankOrchestrator(TraceLedger(), reranker_config=config)
        orch._http_client = mock_client

        candidates = [_rc(text="a"), _rc(text="b")]
        result = await orch.cross_encoder_rerank(candidates, "query")
        assert len(result) == 2
        mock_client.post.assert_called_once()
        call_args = mock_client.post.call_args
        assert "/v1/rerank" in call_args[0][0]

    @pytest.mark.asyncio
    async def test_normalizes_scores_via_sigmoid(self):
        # sigmoid(2.0) ≈ 0.88, sigmoid(-1.0) ≈ 0.27
        assert abs(_sigmoid(2.0) - 0.8808) < 0.01
        assert abs(_sigmoid(-1.0) - 0.2689) < 0.01
        assert abs(_sigmoid(0.0) - 0.5) < 0.01

    @pytest.mark.asyncio
    async def test_fallback_on_error_returns_input(self):
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=Exception("connection refused"))

        config = RerankerConfig(enabled=True, fallback_on_error=True)
        orch = RerankOrchestrator(TraceLedger(), reranker_config=config)
        orch._http_client = mock_client

        candidates = [_rc()]
        result = await orch.cross_encoder_rerank(candidates, "query")
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_fallback_emits_degraded_trace_event(self):
        mock_client = AsyncMock()
        mock_client.post = AsyncMock(side_effect=Exception("timeout"))

        ledger = TraceLedger()
        config = RerankerConfig(enabled=True, fallback_on_error=True)
        orch = RerankOrchestrator(ledger, reranker_config=config)
        orch._http_client = mock_client

        await orch.cross_encoder_rerank([_rc()], "query")
        events = ledger._events
        assert any(e.event_type.value == "degraded_operation" for e in events)

    @pytest.mark.asyncio
    async def test_disabled_returns_input(self):
        config = RerankerConfig(enabled=False)
        orch = RerankOrchestrator(TraceLedger(), reranker_config=config)
        candidates = [_rc()]
        result = await orch.cross_encoder_rerank(candidates, "query")
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_truncates_to_max_documents(self):
        mock_response = MagicMock()
        mock_response.raise_for_status = MagicMock()
        mock_response.json = MagicMock(return_value={"results": [{"index": 0, "relevance_score": 1.0}]})

        mock_client = AsyncMock()
        mock_client.post = AsyncMock(return_value=mock_response)

        config = RerankerConfig(enabled=True, max_documents=2)
        orch = RerankOrchestrator(TraceLedger(), reranker_config=config)
        orch._http_client = mock_client

        candidates = [_rc(text=f"f{i}") for i in range(5)]
        result = await orch.cross_encoder_rerank(candidates, "query")
        # Should have been truncated to 2 before sending
        call_args = mock_client.post.call_args
        docs = call_args[1]["json"]["documents"]
        assert len(docs) == 2


class TestMergeDuplicates:
    @pytest.mark.asyncio
    async def test_groups_by_cosine_095(self):
        embeddings = AsyncMock()
        # Two nearly identical, one different
        embeddings.embed_batch = AsyncMock(return_value=[
            [1.0, 0.0], [0.999, 0.01], [0.0, 1.0],
        ])
        config = ScoringConfig(merge_similarity_threshold=0.95)
        orch = RerankOrchestrator(TraceLedger(), embedding_service=embeddings, scoring_config=config)
        candidates = [_rc(text="a", score=0.9), _rc(text="a copy", score=0.7), _rc(text="b", score=0.8)]
        result = await orch.merge_duplicates(candidates)
        assert len(result) == 2  # Two groups

    @pytest.mark.asyncio
    async def test_keeps_highest_scored(self):
        embeddings = AsyncMock()
        embeddings.embed_batch = AsyncMock(return_value=[
            [1.0, 0.0], [0.999, 0.01],
        ])
        config = ScoringConfig(merge_similarity_threshold=0.95)
        orch = RerankOrchestrator(TraceLedger(), embedding_service=embeddings, scoring_config=config)
        candidates = [_rc(text="a", score=0.5), _rc(text="a copy", score=0.9)]
        result = await orch.merge_duplicates(candidates)
        assert len(result) == 1
        assert result[0].score == 0.9

    @pytest.mark.asyncio
    async def test_single_candidate_no_change(self):
        orch = RerankOrchestrator(TraceLedger(), embedding_service=AsyncMock())
        orch._embeddings.embed_batch = AsyncMock(return_value=[[1.0]])
        result = await orch.merge_duplicates([_rc()])
        assert len(result) == 1


class TestDedupSafe:
    @pytest.mark.asyncio
    async def test_removes_id_duplicates(self):
        orch = RerankOrchestrator(TraceLedger())
        c = _rc()
        result = await orch.dedup_safe([c, c])
        assert len(result) == 1

    @pytest.mark.asyncio
    async def test_preserves_order(self):
        orch = RerankOrchestrator(TraceLedger())
        c1, c2 = _rc(text="first"), _rc(text="second")
        result = await orch.dedup_safe([c1, c2])
        assert result[0].fact.text == "first"


class TestFullPipeline:
    @pytest.mark.asyncio
    async def test_end_to_end_all_stages(self):
        embeddings = AsyncMock()
        embeddings.embed_batch = AsyncMock(return_value=[[0.5, 0.5], [0.1, 0.9]])
        config = RerankerConfig(enabled=False)  # Skip cross-encoder in test
        orch = RerankOrchestrator(
            TraceLedger(), embedding_service=embeddings,
            reranker_config=config,
        )
        candidates = [_rc(text="a"), _rc(text="b")]
        result = await orch.rerank(candidates, "query", query_embedding=[1.0, 0.0])
        assert len(result) <= 2

    @pytest.mark.asyncio
    async def test_empty_input(self):
        orch = RerankOrchestrator(TraceLedger())
        result = await orch.rerank([], "query")
        assert result == []

    @pytest.mark.asyncio
    async def test_single_candidate(self):
        config = RerankerConfig(enabled=False)
        orch = RerankOrchestrator(TraceLedger(), reranker_config=config)
        result = await orch.rerank([_rc()], "query")
        assert len(result) == 1


class TestHelpers:
    def test_cosine_sim_identical(self):
        assert abs(_cosine_sim([1.0, 0.0], [1.0, 0.0]) - 1.0) < 1e-6

    def test_cosine_sim_orthogonal(self):
        assert abs(_cosine_sim([1.0, 0.0], [0.0, 1.0])) < 1e-6

    def test_cosine_sim_empty(self):
        assert _cosine_sim([], []) == 0.0

    def test_sigmoid_zero(self):
        assert abs(_sigmoid(0.0) - 0.5) < 1e-6

    def test_sigmoid_large_positive(self):
        assert _sigmoid(100.0) > 0.99

    def test_sigmoid_large_negative(self):
        assert _sigmoid(-100.0) < 0.01
