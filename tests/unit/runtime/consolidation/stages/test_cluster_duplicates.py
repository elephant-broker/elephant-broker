"""Tests for Stage 1: Cluster Near-Duplicates."""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from elephantbroker.runtime.consolidation.stages.cluster_duplicates import (
    ClusterDuplicatesStage,
    _cosine_sim,
)
from elephantbroker.schemas.consolidation import ConsolidationConfig
from tests.fixtures.factories import make_fact_assertion


def _make_stage(threshold: float = 0.92):
    config = ConsolidationConfig(cluster_similarity_threshold=threshold)
    embeddings = AsyncMock()
    return ClusterDuplicatesStage(embeddings, config), embeddings


class TestClusterDuplicates:
    async def test_empty_facts_returns_empty(self):
        stage, _ = _make_stage()
        result = await stage.run([], "gw-1")
        assert result == []

    async def test_single_fact_returns_empty(self):
        stage, emb = _make_stage()
        facts = [make_fact_assertion(text="hello")]
        emb.embed_batch.return_value = [[0.1, 0.2, 0.3]]
        result = await stage.run(facts, "gw-1")
        assert result == []

    async def test_identical_embeddings_clustered(self):
        stage, emb = _make_stage(threshold=0.9)
        facts = [
            make_fact_assertion(text="A prefers TypeScript", confidence=0.9, session_key="s1"),
            make_fact_assertion(text="A prefers TypeScript", confidence=0.7, session_key="s2"),
        ]
        # Identical embeddings → cosine_sim = 1.0
        emb.embed_batch.return_value = [[1.0, 0.0, 0.0], [1.0, 0.0, 0.0]]
        result = await stage.run(facts, "gw-1")
        assert len(result) == 1
        assert len(result[0].fact_ids) == 2

    async def test_different_embeddings_not_clustered(self):
        stage, emb = _make_stage(threshold=0.95)
        facts = [
            make_fact_assertion(text="apples"),
            make_fact_assertion(text="quantum physics"),
        ]
        # Orthogonal embeddings → cosine_sim ≈ 0
        emb.embed_batch.return_value = [[1.0, 0.0, 0.0], [0.0, 1.0, 0.0]]
        result = await stage.run(facts, "gw-1")
        assert len(result) == 0

    async def test_threshold_configurable(self):
        # Low threshold (0.5 minimum) clusters more aggressively
        stage, emb = _make_stage(threshold=0.5)
        facts = [make_fact_assertion(text="a"), make_fact_assertion(text="b")]
        # cosine_sim([1,0], [0.5,0.5]) ≈ 0.707 > 0.5 threshold → should cluster
        emb.embed_batch.return_value = [[1.0, 0.0], [0.5, 0.5]]
        result = await stage.run(facts, "gw-1")
        assert len(result) == 1

    async def test_canonical_candidate_is_highest_confidence(self):
        stage, emb = _make_stage(threshold=0.5)
        f1 = make_fact_assertion(text="a", confidence=0.3)
        f2 = make_fact_assertion(text="b", confidence=0.9)
        facts = [f1, f2]
        emb.embed_batch.return_value = [[1.0, 0.0], [0.9, 0.1]]
        result = await stage.run(facts, "gw-1")
        assert len(result) == 1
        assert result[0].canonical_candidate_id == str(f2.id)

    async def test_session_keys_tracked(self):
        stage, emb = _make_stage(threshold=0.5)
        facts = [
            make_fact_assertion(text="a", session_key="session-1"),
            make_fact_assertion(text="b", session_key="session-2"),
            make_fact_assertion(text="c", session_key="session-1"),
        ]
        emb.embed_batch.return_value = [[1.0, 0.0], [0.9, 0.1], [0.95, 0.05]]
        result = await stage.run(facts, "gw-1")
        assert len(result) == 1
        assert set(result[0].session_keys) == {"session-1", "session-2"}

    async def test_embedding_failure_returns_empty(self):
        stage, emb = _make_stage()
        emb.embed_batch.side_effect = RuntimeError("API down")
        facts = [make_fact_assertion(text="a"), make_fact_assertion(text="b")]
        result = await stage.run(facts, "gw-1")
        assert result == []


class TestCosineSimUtility:
    def test_identical_vectors(self):
        assert _cosine_sim([1.0, 0.0], [1.0, 0.0]) == pytest.approx(1.0)

    def test_orthogonal_vectors(self):
        assert _cosine_sim([1.0, 0.0], [0.0, 1.0]) == pytest.approx(0.0)

    def test_empty_vectors(self):
        assert _cosine_sim([], []) == 0.0

    def test_zero_vector(self):
        assert _cosine_sim([0.0, 0.0], [1.0, 0.0]) == 0.0
