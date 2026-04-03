"""Tests for Stage 2: Canonicalize Stable Facts (LLM Smart Merge)."""
from __future__ import annotations

import sys
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elephantbroker.schemas.consolidation import (
    CanonicalResult,
    ConsolidationConfig,
    ConsolidationContext,
    DuplicateCluster,
)
from tests.fixtures.factories import make_fact_assertion


@pytest.fixture(autouse=True)
def _mock_cognee_for_canonicalize(monkeypatch):
    """Mock cognee and add_data_points for all canonicalize tests."""
    mock_adp = AsyncMock()
    mock_cognee = MagicMock()
    mock_cognee.add = AsyncMock()
    monkeypatch.setattr("cognee.tasks.storage.add_data_points", mock_adp)
    # Ensure cognee module is available for inline import
    if "cognee" not in sys.modules:
        sys.modules["cognee"] = mock_cognee
    else:
        monkeypatch.setattr("cognee.add", AsyncMock())
    return mock_adp


def _make_stage(llm_text="merged fact", llm_fail=False):
    from elephantbroker.runtime.consolidation.stages.canonicalize import CanonicalizationStage

    graph = AsyncMock()
    vector = AsyncMock()
    embeddings = AsyncMock()
    llm = AsyncMock()
    if llm_fail:
        llm.complete = AsyncMock(side_effect=RuntimeError("LLM down"))
    else:
        llm.complete = AsyncMock(return_value=llm_text)
    config = ConsolidationConfig()
    stage = CanonicalizationStage(graph, vector, llm, embeddings, config)
    return stage, graph, vector, llm


def _make_context(**kw):
    defaults = {"org_id": "org", "gateway_id": "gw", "llm_calls_cap": 50}
    defaults.update(kw)
    return ConsolidationContext(**defaults)


def _make_cluster(facts, avg_sim=0.95):
    return DuplicateCluster(
        fact_ids=[str(f.id) for f in facts],
        canonical_candidate_id=str(facts[0].id),
        avg_similarity=avg_sim,
        session_keys=list({f.session_key for f in facts if f.session_key}),
    )


class TestCanonicalize:
    async def test_creates_new_canonical_fact(self):
        stage, graph, vector, llm = _make_stage(llm_text="User prefers TypeScript for backend projects")
        facts = [
            make_fact_assertion(text="User prefers TypeScript", confidence=0.9, session_key="s1"),
            make_fact_assertion(text="User likes TypeScript for backend", confidence=0.7, session_key="s2"),
        ]
        cluster = _make_cluster(facts)
        ctx = _make_context()
        results = await stage.run([cluster], facts, "gw", ctx)
        assert len(results) == 1
        assert results[0].canonical_text == "User prefers TypeScript for backend projects"
        assert results[0].llm_used is True

    async def test_archives_all_originals(self):
        stage, graph, vector, llm = _make_stage()
        facts = [
            make_fact_assertion(text="fact A", confidence=0.8, session_key="s1"),
            make_fact_assertion(text="fact B", confidence=0.6, session_key="s2"),
        ]
        cluster = _make_cluster(facts)
        ctx = _make_context()
        results = await stage.run([cluster], facts, "gw", ctx)
        assert len(results[0].archived_fact_ids) == 2

    async def test_merges_use_counts(self):
        stage, *_ = _make_stage()
        facts = [
            make_fact_assertion(text="a", use_count=5, successful_use_count=3, session_key="s1"),
            make_fact_assertion(text="b", use_count=3, successful_use_count=2, session_key="s2"),
        ]
        cluster = _make_cluster(facts)
        ctx = _make_context()
        results = await stage.run([cluster], facts, "gw", ctx)
        assert results[0].merged_use_count == 8
        assert results[0].merged_successful_use_count == 5

    async def test_merges_provenance_from_all_versions(self):
        stage, *_ = _make_stage()
        facts = [
            make_fact_assertion(text="a", provenance_refs=["ref1", "ref2"], session_key="s1"),
            make_fact_assertion(text="b", provenance_refs=["ref2", "ref3"], session_key="s2"),
        ]
        cluster = _make_cluster(facts)
        ctx = _make_context()
        results = await stage.run([cluster], facts, "gw", ctx)
        assert set(results[0].merged_provenance) == {"ref1", "ref2", "ref3"}

    async def test_merges_goal_ids(self):
        stage, *_ = _make_stage()
        g1, g2 = uuid.uuid4(), uuid.uuid4()
        facts = [
            make_fact_assertion(text="a", goal_ids=[g1], session_key="s1"),
            make_fact_assertion(text="b", goal_ids=[g2], session_key="s2"),
        ]
        cluster = _make_cluster(facts)
        ctx = _make_context()
        results = await stage.run([cluster], facts, "gw", ctx)
        assert len(results[0].merged_goal_ids) == 2

    async def test_deterministic_merge_for_identical_texts(self):
        stage, _, _, llm = _make_stage()
        facts = [
            make_fact_assertion(text="identical text", confidence=0.9, session_key="s1"),
            make_fact_assertion(text="identical text", confidence=0.7, session_key="s2"),
        ]
        cluster = _make_cluster(facts)
        ctx = _make_context()
        results = await stage.run([cluster], facts, "gw", ctx)
        assert len(results) == 1
        assert results[0].llm_used is False
        llm.complete.assert_not_called()

    async def test_respects_llm_calls_cap(self):
        stage, _, _, llm = _make_stage()
        facts = [
            make_fact_assertion(text="a", session_key="s1"),
            make_fact_assertion(text="b", session_key="s2"),
        ]
        cluster = _make_cluster(facts)
        ctx = _make_context(llm_calls_used=50, llm_calls_cap=50)
        results = await stage.run([cluster], facts, "gw", ctx)
        assert len(results) == 0
        llm.complete.assert_not_called()

    async def test_creates_superseded_by_edges(self):
        stage, graph, vector, _ = _make_stage()
        facts = [
            make_fact_assertion(text="identical", session_key="s1"),
            make_fact_assertion(text="identical", session_key="s2"),
        ]
        cluster = _make_cluster(facts)
        ctx = _make_context()
        await stage.run([cluster], facts, "gw", ctx)
        assert graph.add_relation.call_count == 2
        for call in graph.add_relation.call_args_list:
            assert call[0][2] == "SUPERSEDED_BY"

    async def test_deletes_qdrant_embeddings_on_archive(self):
        stage, graph, vector, _ = _make_stage()
        facts = [
            make_fact_assertion(text="identical", session_key="s1"),
            make_fact_assertion(text="identical", session_key="s2"),
        ]
        cluster = _make_cluster(facts)
        ctx = _make_context()
        await stage.run([cluster], facts, "gw", ctx)
        assert vector.delete_embedding.call_count == 2

    async def test_llm_failure_skips_cluster(self):
        stage, *_ = _make_stage(llm_fail=True)
        facts = [
            make_fact_assertion(text="a", session_key="s1"),
            make_fact_assertion(text="b", session_key="s2"),
        ]
        cluster = _make_cluster(facts)
        ctx = _make_context()
        results = await stage.run([cluster], facts, "gw", ctx)
        assert len(results) == 0

    async def test_uses_broadest_scope(self):
        stage, *_ = _make_stage()
        facts = [
            make_fact_assertion(text="identical", scope="session", session_key="s1"),
            make_fact_assertion(text="identical", scope="actor", session_key="s2"),
        ]
        cluster = _make_cluster(facts)
        ctx = _make_context()
        results = await stage.run([cluster], facts, "gw", ctx)
        assert len(results) == 1

    async def test_empty_clusters_returns_empty(self):
        stage, *_ = _make_stage()
        ctx = _make_context()
        results = await stage.run([], [], "gw", ctx)
        assert results == []
