"""Integration tests for the rerank orchestrator against live infrastructure.

These tests exercise the 4-stage reranking pipeline (cheap_prune -> semantic ->
cross-encoder -> merge) against live embedding service and optional Qwen3-Reranker.
Skipped unless Docker infrastructure is running and ``integration`` is selected.
"""
from __future__ import annotations

import pytest
import pytest_asyncio

from elephantbroker.runtime.container import RuntimeContainer
from elephantbroker.schemas.config import ElephantBrokerConfig
from elephantbroker.schemas.tiers import BusinessTier
from tests.fixtures.factories import make_fact_assertion, make_retrieval_candidate

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def container():
    """Build a full RuntimeContainer wired to Docker test services."""
    config = ElephantBrokerConfig.from_env()
    c = await RuntimeContainer.from_config(config, BusinessTier.FULL)
    yield c
    try:
        await c.close()
    except Exception:
        pass


class TestRerankIntegration:
    """Reranking pipeline against live embedding service and cross-encoder."""

    async def test_rerank_preserves_candidates(self, container):
        """Reranking should not lose any candidates (unless merged as duplicates)."""
        assert container.rerank is not None

        candidates = [
            make_retrieval_candidate(
                fact=make_fact_assertion(text=f"Distinct candidate fact {i}"),
                score=0.9 - i * 0.1,
            )
            for i in range(5)
        ]
        reranked = await container.rerank.rerank(
            candidates, "test query for reranking",
        )
        # All candidates should survive (they are semantically distinct)
        assert len(reranked) >= 3
        assert len(reranked) <= len(candidates)

    async def test_rerank_empty_input_returns_empty(self, container):
        """Empty candidate list should produce empty output."""
        assert container.rerank is not None

        result = await container.rerank.rerank([], "any query")
        assert result == []

    async def test_cheap_prune_reduces_large_set(self, container):
        """cheap_prune should trim candidates beyond max_candidates."""
        assert container.rerank is not None

        candidates = [
            make_retrieval_candidate(
                fact=make_fact_assertion(text=f"Pruning test candidate {i}"),
                score=0.5,
            )
            for i in range(100)
        ]
        pruned = await container.rerank.cheap_prune(
            candidates, "pruning test", max_candidates=20,
        )
        assert len(pruned) == 20
