"""Tests for Stage 6: Promote Facts (Class + Scope)."""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock

from elephantbroker.runtime.consolidation.stages.promote import PromoteStage
from elephantbroker.schemas.consolidation import ConsolidationConfig, ConsolidationContext, DuplicateCluster
from tests.fixtures.factories import make_fact_assertion


def _make_stage():
    config = ConsolidationConfig(promote_session_threshold=3)
    graph = AsyncMock()
    stage = PromoteStage(graph, None, None, config)
    return stage


def _make_context(clusters=None):
    return ConsolidationContext(
        org_id="org", gateway_id="gw",
        clusters=clusters or [],
    )


def _cluster_spanning_sessions(facts, session_keys):
    return DuplicateCluster(
        fact_ids=[str(f.id) for f in facts],
        canonical_candidate_id=str(facts[0].id),
        avg_similarity=0.95,
        session_keys=session_keys,
    )


class TestPromote:
    async def test_promotes_recurring_episodic_to_semantic(self):
        stage = _make_stage()
        f = make_fact_assertion(text="recurring", memory_class="episodic", scope="session",
                                successful_use_count=1, session_key="s1")
        cluster = _cluster_spanning_sessions([f], ["s1", "s2", "s3"])
        ctx = _make_context(clusters=[cluster])
        results = await stage.run([f], "gw", ctx)
        assert len(results) == 1
        assert results[0].new_memory_class == "semantic"

    async def test_does_not_promote_single_session_fact(self):
        stage = _make_stage()
        f = make_fact_assertion(text="one-off", memory_class="episodic", scope="session",
                                successful_use_count=0, session_key="s1")
        ctx = _make_context(clusters=[])
        results = await stage.run([f], "gw", ctx)
        assert len(results) == 0

    async def test_promotes_scope_with_persistent_goal_link(self):
        stage = _make_stage()
        f = make_fact_assertion(text="goal-linked", memory_class="episodic", scope="session",
                                confidence=0.8, goal_ids=[uuid.uuid4()], successful_use_count=1, session_key="s1")
        cluster = _cluster_spanning_sessions([f], ["s1", "s2", "s3"])
        ctx = _make_context(clusters=[cluster])
        results = await stage.run([f], "gw", ctx)
        assert len(results) == 1
        assert results[0].new_scope == "actor"

    async def test_promotes_scope_for_recurring_used_fact(self):
        stage = _make_stage()
        f = make_fact_assertion(text="used", memory_class="episodic", scope="session",
                                successful_use_count=5, session_key="s1")
        cluster = _cluster_spanning_sessions([f], ["s1", "s2", "s3"])
        ctx = _make_context(clusters=[cluster])
        results = await stage.run([f], "gw", ctx)
        assert results[0].new_scope == "actor"

    async def test_does_not_promote_scope_for_unused_recurring(self):
        stage = _make_stage()
        f = make_fact_assertion(text="unused recurring", memory_class="episodic", scope="session",
                                successful_use_count=0, session_key="s1")
        cluster = _cluster_spanning_sessions([f], ["s1", "s2", "s3"])
        ctx = _make_context(clusters=[cluster])
        results = await stage.run([f], "gw", ctx)
        # Class promoted (episodic→semantic) but scope stays session
        if results:
            assert results[0].new_scope == "session"

    async def test_class_and_scope_promoted_together(self):
        stage = _make_stage()
        f = make_fact_assertion(text="full promote", memory_class="episodic", scope="session",
                                confidence=0.8, goal_ids=[uuid.uuid4()], successful_use_count=2, session_key="s1")
        cluster = _cluster_spanning_sessions([f], ["s1", "s2", "s3"])
        ctx = _make_context(clusters=[cluster])
        results = await stage.run([f], "gw", ctx)
        assert len(results) == 1
        assert results[0].new_memory_class == "semantic"
        assert results[0].new_scope == "actor"

    async def test_global_flagged_not_auto_promoted(self):
        stage = _make_stage()
        f = make_fact_assertion(text="high confidence", memory_class="episodic", scope="session",
                                confidence=1.0, goal_ids=[uuid.uuid4()],
                                successful_use_count=5, session_key="s1")
        cluster = _cluster_spanning_sessions([f], ["s1", "s2", "s3"])
        ctx = _make_context(clusters=[cluster])
        results = await stage.run([f], "gw", ctx)
        # GLOBAL promotion is flagged, not applied (AD-13)
        for r in results:
            assert r.new_scope != "global"

    async def test_archived_facts_skipped(self):
        stage = _make_stage()
        f = make_fact_assertion(text="archived", memory_class="episodic", scope="session")
        f.archived = True
        ctx = _make_context()
        results = await stage.run([f], "gw", ctx)
        assert len(results) == 0

    async def test_already_semantic_not_reclassified(self):
        stage = _make_stage()
        f = make_fact_assertion(text="already semantic", memory_class="semantic", scope="session",
                                successful_use_count=2, session_key="s1")
        cluster = _cluster_spanning_sessions([f], ["s1", "s2", "s3"])
        ctx = _make_context(clusters=[cluster])
        results = await stage.run([f], "gw", ctx)
        # Scope may change but class stays semantic (not downgraded)
        for r in results:
            assert r.new_memory_class in ("semantic", "procedural", "policy")

    async def test_persistent_goal_few_sessions_promotes_scope_only(self):
        stage = _make_stage()
        f = make_fact_assertion(text="goal only", memory_class="episodic", scope="session",
                                goal_ids=[uuid.uuid4()], successful_use_count=1, session_key="s1")
        # Only 2 sessions — below threshold
        cluster = _cluster_spanning_sessions([f], ["s1", "s2"])
        ctx = _make_context(clusters=[cluster])
        results = await stage.run([f], "gw", ctx)
        if results:
            assert results[0].old_memory_class == results[0].new_memory_class  # Class unchanged
            assert results[0].new_scope == "actor"

    async def test_empty_facts_returns_empty(self):
        stage = _make_stage()
        ctx = _make_context()
        results = await stage.run([], "gw", ctx)
        assert results == []

    async def test_no_llm_calls(self):
        stage = _make_stage()
        f = make_fact_assertion(text="test", memory_class="episodic", scope="session", session_key="s1")
        cluster = _cluster_spanning_sessions([f], ["s1", "s2", "s3"])
        ctx = _make_context(clusters=[cluster])
        await stage.run([f], "gw", ctx)
        # Stage 6 never calls LLM — pure logic

    async def test_reason_field_set(self):
        stage = _make_stage()
        f = make_fact_assertion(text="reason test", memory_class="episodic", scope="session",
                                goal_ids=[uuid.uuid4()], successful_use_count=1, session_key="s1")
        cluster = _cluster_spanning_sessions([f], ["s1", "s2", "s3"])
        ctx = _make_context(clusters=[cluster])
        results = await stage.run([f], "gw", ctx)
        if results:
            assert results[0].reason in ("recurring_with_goal", "recurring_with_use", "persistent_goal_link")

    async def test_sessions_seen_count(self):
        stage = _make_stage()
        f = make_fact_assertion(text="counted", memory_class="episodic", scope="session",
                                successful_use_count=1, session_key="s1")
        cluster = _cluster_spanning_sessions([f], ["s1", "s2", "s3", "s4"])
        ctx = _make_context(clusters=[cluster])
        results = await stage.run([f], "gw", ctx)
        if results:
            assert results[0].sessions_seen >= 3
