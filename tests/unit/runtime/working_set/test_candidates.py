"""Tests for CandidateGenerator — multi-source candidate collection."""
from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elephantbroker.runtime.interfaces.retrieval import RetrievalCandidate
from elephantbroker.runtime.working_set.candidates import CandidateGenerator
from elephantbroker.schemas.fact import FactAssertion
from elephantbroker.schemas.goal import GoalState, GoalStatus
from elephantbroker.schemas.working_set import WorkingSetItem
from tests.fixtures.factories import (
    make_fact_assertion,
    make_goal_state,
    make_profile_policy,
    make_retrieval_candidate,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_generator(
    *, retrieval=None, goal_manager=None, graph=None, redis=None,
    procedure_engine=None, config=None, embedding_service=None,
    gateway_id="", redis_keys=None,
):
    """Build a CandidateGenerator with AsyncMock dependencies."""
    if retrieval is None:
        retrieval = AsyncMock()
        retrieval.retrieve_candidates = AsyncMock(return_value=[])
    return CandidateGenerator(
        retrieval=retrieval,
        goal_manager=goal_manager,
        procedure_engine=procedure_engine,
        graph=graph,
        redis=redis,
        config=config,
        embedding_service=embedding_service,
        gateway_id=gateway_id,
        redis_keys=redis_keys,
    )


# ---------------------------------------------------------------------------
# C19: CandidateGenerator keys are gateway-scoped via RedisKeyBuilder
# ---------------------------------------------------------------------------


class TestGatewayScopedKeys:
    """After C19, CandidateGenerator always resolves session_goals through
    a RedisKeyBuilder (auto-built from gateway_id if not supplied). The old
    ``f"eb:session_goals:{sk}"`` fallback that bypassed the gateway prefix
    is gone."""

    def test_auto_builds_builder_when_redis_keys_not_supplied(self):
        gen = _make_generator(gateway_id="gw-xyz", redis_keys=None)
        assert gen._keys is not None
        assert gen._keys.session_goals("agent:main:main") == (
            "eb:gw-xyz:session_goals:agent:main:main"
        )

    def test_uses_supplied_builder_unchanged(self):
        from elephantbroker.runtime.redis_keys import RedisKeyBuilder
        external = RedisKeyBuilder("gw-external")
        gen = _make_generator(gateway_id="gw-xyz", redis_keys=external)
        assert gen._keys is external


def _make_rc(text="fact text", source="structural", score=0.8, category="general", **fact_kw):
    """Shortcut to build a RetrievalCandidate."""
    fact = make_fact_assertion(text=text, category=category, **fact_kw)
    return RetrievalCandidate(fact=fact, source=source, score=score)


def _session_args(**overrides):
    """Common kwargs for generate()."""
    defaults = {
        "session_id": uuid.uuid4(),
        "session_key": "agent:main:main",
        "query": "test query",
    }
    defaults.update(overrides)
    return defaults


# ---------------------------------------------------------------------------
# generate() — return shape and retrieval
# ---------------------------------------------------------------------------

class TestGenerateReturnShape:
    async def test_returns_tuple_of_two_lists(self):
        gen = _make_generator()
        result = await gen.generate(**_session_args())
        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], list)
        assert isinstance(result[1], list)

    async def test_empty_sources_return_empty_tuple(self):
        gen = _make_generator()
        retrieval_cands, direct_items = await gen.generate(**_session_args())
        assert retrieval_cands == []
        assert direct_items == []

    async def test_retrieval_candidates_come_from_orchestrator(self):
        rc = _make_rc()
        retrieval = AsyncMock()
        retrieval.retrieve_candidates = AsyncMock(return_value=[rc])
        gen = _make_generator(retrieval=retrieval)

        retrieval_cands, _ = await gen.generate(**_session_args())
        assert len(retrieval_cands) == 1
        assert retrieval_cands[0] is rc

    async def test_retrieval_passes_policy_and_session_key(self):
        retrieval = AsyncMock()
        retrieval.retrieve_candidates = AsyncMock(return_value=[])
        gen = _make_generator(retrieval=retrieval)
        policy = make_profile_policy()

        await gen.generate(**_session_args(profile_policy=policy))
        retrieval.retrieve_candidates.assert_awaited_once()
        call_kw = retrieval.retrieve_candidates.call_args
        assert call_kw.kwargs.get("policy") == policy.retrieval or call_kw[1].get("policy") == policy.retrieval

    async def test_retrieval_exception_returns_empty_list(self):
        retrieval = AsyncMock()
        retrieval.retrieve_candidates = AsyncMock(side_effect=RuntimeError("boom"))
        gen = _make_generator(retrieval=retrieval)

        retrieval_cands, direct_items = await gen.generate(**_session_args())
        assert retrieval_cands == []


# ---------------------------------------------------------------------------
# Session goals from Redis
# ---------------------------------------------------------------------------

class TestSessionGoalsRedis:
    async def test_session_goals_from_redis(self):
        goal = make_goal_state(title="Ship v2", status=GoalStatus.ACTIVE)
        redis = AsyncMock()
        sid = uuid.uuid4()
        key = "eb:session_goals:agent:main:main"
        redis.get = AsyncMock(return_value=json.dumps([goal.model_dump(mode="json")]))

        gen = _make_generator(redis=redis)
        _, direct_items = await gen.generate(
            session_id=sid, session_key="agent:main:main", query="test",
        )
        goal_items = [i for i in direct_items if i.source_type == "goal"]
        assert len(goal_items) == 1
        assert "Ship v2" in goal_items[0].text

    async def test_redis_primary_goal_manager_fallback(self):
        """When Redis returns nothing, fall back to GoalManager."""
        goal = make_goal_state(title="Fallback goal", status=GoalStatus.ACTIVE)
        redis = AsyncMock()
        redis.get = AsyncMock(return_value=None)
        goal_manager = AsyncMock()
        goal_manager.resolve_active_goals = AsyncMock(return_value=[goal])

        gen = _make_generator(redis=redis, goal_manager=goal_manager)
        sid = uuid.uuid4()
        _, direct_items = await gen.generate(
            session_id=sid, session_key="agent:main:main", query="test",
        )
        goal_items = [i for i in direct_items if i.source_type == "goal"]
        assert len(goal_items) == 1
        assert "Fallback goal" in goal_items[0].text
        goal_manager.resolve_active_goals.assert_awaited_once_with(sid)

    async def test_redis_hit_skips_goal_manager(self):
        """When Redis returns goals, GoalManager is never called."""
        goal = make_goal_state(title="From redis", status=GoalStatus.ACTIVE)
        redis = AsyncMock()
        sid = uuid.uuid4()
        redis.get = AsyncMock(
            return_value=json.dumps([goal.model_dump(mode="json")])
        )
        goal_manager = AsyncMock()
        goal_manager.resolve_active_goals = AsyncMock(return_value=[])

        gen = _make_generator(redis=redis, goal_manager=goal_manager)
        await gen.generate(
            session_id=sid, session_key="agent:main:main", query="test",
        )
        goal_manager.resolve_active_goals.assert_not_awaited()

    async def test_completed_goals_filtered_out(self):
        active = make_goal_state(title="Active", status=GoalStatus.ACTIVE)
        completed = make_goal_state(title="Done", status=GoalStatus.COMPLETED)
        redis = AsyncMock()
        sid = uuid.uuid4()
        redis.get = AsyncMock(
            return_value=json.dumps([
                active.model_dump(mode="json"),
                completed.model_dump(mode="json"),
            ])
        )

        gen = _make_generator(redis=redis)
        _, direct_items = await gen.generate(
            session_id=sid, session_key="agent:main:main", query="test",
        )
        goal_items = [i for i in direct_items if i.source_type == "goal"]
        assert len(goal_items) == 1
        assert "Active" in goal_items[0].text

    async def test_proposed_goals_included(self):
        proposed = make_goal_state(title="Maybe", status=GoalStatus.PROPOSED)
        redis = AsyncMock()
        sid = uuid.uuid4()
        redis.get = AsyncMock(
            return_value=json.dumps([proposed.model_dump(mode="json")])
        )

        gen = _make_generator(redis=redis)
        _, direct_items = await gen.generate(
            session_id=sid, session_key="agent:main:main", query="test",
        )
        goal_items = [i for i in direct_items if i.source_type == "goal"]
        assert len(goal_items) == 1


# ---------------------------------------------------------------------------
# must_inject flagging
# ---------------------------------------------------------------------------

class TestMustInjectFlagging:
    async def test_goal_with_blockers_is_must_inject(self):
        goal = make_goal_state(
            title="Blocked", status=GoalStatus.ACTIVE, blockers=["waiting for deploy"],
        )
        redis = AsyncMock()
        sid = uuid.uuid4()
        redis.get = AsyncMock(
            return_value=json.dumps([goal.model_dump(mode="json")])
        )

        gen = _make_generator(redis=redis)
        _, direct_items = await gen.generate(
            session_id=sid, session_key="agent:main:main", query="test",
        )
        goal_items = [i for i in direct_items if i.source_type == "goal"]
        assert len(goal_items) == 1
        assert goal_items[0].must_inject is True

    async def test_goal_without_blockers_not_must_inject(self):
        goal = make_goal_state(title="Free", status=GoalStatus.ACTIVE, blockers=[])
        redis = AsyncMock()
        sid = uuid.uuid4()
        redis.get = AsyncMock(
            return_value=json.dumps([goal.model_dump(mode="json")])
        )

        gen = _make_generator(redis=redis)
        _, direct_items = await gen.generate(
            session_id=sid, session_key="agent:main:main", query="test",
        )
        goal_items = [i for i in direct_items if i.source_type == "goal"]
        assert goal_items[0].must_inject is False

    def test_constraint_fact_is_must_inject(self):
        rc = _make_rc(category="constraint")
        item = CandidateGenerator.retrieval_candidate_to_item(rc)
        assert item.must_inject is True

    def test_non_constraint_fact_not_must_inject(self):
        rc = _make_rc(category="general")
        item = CandidateGenerator.retrieval_candidate_to_item(rc)
        assert item.must_inject is False

    async def test_procedure_with_required_evidence_is_must_inject(self):
        graph = AsyncMock()
        pid = str(uuid.uuid4())
        graph.query_cypher = AsyncMock(return_value=[
            {"props": {"name": "deploy", "eb_id": pid, "required_evidence": "tool_output"}},
        ])
        gen = _make_generator(graph=graph)
        _, direct_items = await gen.generate(**_session_args())
        proc_items = [i for i in direct_items if i.source_type == "procedure"]
        assert len(proc_items) == 1
        assert proc_items[0].must_inject is True

    async def test_procedure_without_required_evidence_not_must_inject(self):
        graph = AsyncMock()
        pid = str(uuid.uuid4())
        graph.query_cypher = AsyncMock(return_value=[
            {"props": {"name": "deploy", "eb_id": pid}},
        ])
        gen = _make_generator(graph=graph)
        _, direct_items = await gen.generate(**_session_args())
        proc_items = [i for i in direct_items if i.source_type == "procedure"]
        assert len(proc_items) == 1
        assert proc_items[0].must_inject is False


# ---------------------------------------------------------------------------
# Persistent goals from Cypher
# ---------------------------------------------------------------------------

class TestPersistentGoals:
    async def test_persistent_goals_from_graph(self):
        gid = str(uuid.uuid4())
        graph = AsyncMock()
        graph.query_cypher = AsyncMock(return_value=[
            {"props": {"title": "Global safety", "description": "Be safe", "eb_id": gid}},
        ])

        gen = _make_generator(graph=graph)
        _, direct_items = await gen.generate(**_session_args())
        goal_items = [i for i in direct_items if i.category == "goal"]
        assert len(goal_items) >= 1
        found = [i for i in goal_items if "Global safety" in i.text]
        assert len(found) == 1
        assert "Be safe" in found[0].text

    async def test_persistent_goals_no_graph_returns_empty(self):
        gen = _make_generator(graph=None)
        _, direct_items = await gen.generate(**_session_args())
        # No persistent goals without graph
        persistent = [i for i in direct_items if "Persistent Goal" in i.text]
        assert persistent == []

    async def test_persistent_goals_graph_error_returns_empty(self):
        graph = AsyncMock()
        graph.query_cypher = AsyncMock(side_effect=RuntimeError("neo4j down"))
        gen = _make_generator(graph=graph)
        _, direct_items = await gen.generate(**_session_args())
        persistent = [i for i in direct_items if "Persistent Goal" in i.text]
        assert persistent == []

    async def test_persistent_goals_with_empty_actor_ids(self):
        """When actor_ids is empty list, still returns all global goals."""
        gid = str(uuid.uuid4())
        graph = AsyncMock()
        graph.query_cypher = AsyncMock(return_value=[
            {"props": {"title": "All actors goal", "eb_id": gid}},
        ])
        gen = _make_generator(graph=graph)
        _, direct_items = await gen.generate(**_session_args(actor_ids=[]))
        persistent = [i for i in direct_items if "Persistent Goal" in i.text]
        assert len(persistent) == 1


# ---------------------------------------------------------------------------
# Procedures from graph
# ---------------------------------------------------------------------------

class TestProcedures:
    async def test_procedures_from_graph(self):
        pid = str(uuid.uuid4())
        graph = AsyncMock()
        graph.query_cypher = AsyncMock(return_value=[
            {"props": {"name": "deploy", "description": "Run deploy script", "eb_id": pid}},
        ])
        gen = _make_generator(graph=graph)
        _, direct_items = await gen.generate(**_session_args())
        proc_items = [i for i in direct_items if i.source_type == "procedure"]
        assert len(proc_items) == 1
        assert "deploy" in proc_items[0].text
        assert "Run deploy script" in proc_items[0].text

    async def test_procedures_are_system_prompt_eligible(self):
        pid = str(uuid.uuid4())
        graph = AsyncMock()
        graph.query_cypher = AsyncMock(return_value=[
            {"props": {"name": "lint", "eb_id": pid}},
        ])
        gen = _make_generator(graph=graph)
        _, direct_items = await gen.generate(**_session_args())
        proc_items = [i for i in direct_items if i.source_type == "procedure"]
        assert proc_items[0].system_prompt_eligible is True

    async def test_no_graph_no_procedures(self):
        gen = _make_generator(graph=None)
        _, direct_items = await gen.generate(**_session_args())
        proc_items = [i for i in direct_items if i.source_type == "procedure"]
        assert proc_items == []


# ---------------------------------------------------------------------------
# retrieval_candidate_to_item
# ---------------------------------------------------------------------------

class TestRetrievalCandidateToItem:
    def test_carries_all_metadata(self):
        goal_id = uuid.uuid4()
        now = datetime.now(UTC)
        fact = make_fact_assertion(
            text="important fact",
            category="preference",
            confidence=0.9,
            use_count=5,
            successful_use_count=3,
            created_at=now,
            updated_at=now,
            last_used_at=now,
            token_size=42,
            goal_ids=[goal_id],
            goal_relevance_tags={"key": "val"},
        )
        rc = RetrievalCandidate(fact=fact, source="vector", score=0.75)
        item = CandidateGenerator.retrieval_candidate_to_item(rc)

        assert item.id == str(fact.id)
        # T-3: rc.source is retrieval-path provenance, split into its own field.
        # source_type now carries the DataPoint-type semantic ("fact" for
        # fact-class items).
        assert item.source_type == "fact"
        assert item.retrieval_source == "vector"
        assert item.source_id == fact.id
        assert item.text == "important fact"
        assert item.token_size == 42
        assert item.confidence == 0.9
        assert item.use_count == 5
        assert item.successful_use_count == 3
        assert item.created_at == now
        assert item.updated_at == now
        assert item.last_used_at == now
        assert item.category == "preference"
        assert goal_id in item.goal_ids
        assert item.goal_relevance_tags == {"key": "val"}

    def test_token_size_fallback_when_none(self):
        fact = make_fact_assertion(text="abcd" * 10, token_size=None)
        rc = RetrievalCandidate(fact=fact, source="graph", score=0.5)
        item = CandidateGenerator.retrieval_candidate_to_item(rc)
        assert item.token_size == len(fact.text) // 4

    def test_constraint_system_prompt_eligible(self):
        rc = _make_rc(category="constraint")
        item = CandidateGenerator.retrieval_candidate_to_item(rc)
        assert item.system_prompt_eligible is True

    def test_procedure_ref_system_prompt_eligible(self):
        rc = _make_rc(category="procedure_ref")
        item = CandidateGenerator.retrieval_candidate_to_item(rc)
        assert item.system_prompt_eligible is True

    def test_general_not_system_prompt_eligible(self):
        rc = _make_rc(category="general")
        item = CandidateGenerator.retrieval_candidate_to_item(rc)
        assert item.system_prompt_eligible is False

    def test_retrieval_candidate_to_item_splits_source_type(self):
        """T-3: ``retrieval_candidate_to_item`` splits ``rc.source`` into two
        orthogonal fields:

        - ``source_type`` carries the DataPoint-type semantic.
          Fact-class items → "fact" (regardless of retrieval path).
          Artifact-class items → "artifact" (Pattern (b1): artifacts aren't
          facts; they're a distinct DataPoint type that happens to flow
          through the fact-retrieval pipeline).

        - ``retrieval_source`` carries the retrieval-path provenance for
          fact-class items only. Artifact items leave it None because
          ``retrieval_source`` describes FACT retrieval paths.

        Locks in the Pattern (b1) split for all 5 retrieval sources the
        orchestrator can emit (structural/keyword/vector/graph/artifact).
        Mirrors the orchestrator's source values 1:1.
        """
        fact = make_fact_assertion(text="candidate text")

        # 4 fact-retrieval paths → source_type="fact" + retrieval_source stamped
        for source in ("structural", "keyword", "vector", "graph"):
            rc = RetrievalCandidate(fact=fact, source=source, score=0.5)
            item = CandidateGenerator.retrieval_candidate_to_item(rc)
            assert item.source_type == "fact", (
                f"Fact-retrieval source={source!r} must produce "
                f"source_type='fact', got {item.source_type!r}"
            )
            assert item.retrieval_source == source, (
                f"Fact-retrieval source={source!r} must stamp "
                f"retrieval_source={source!r}, got {item.retrieval_source!r}"
            )

        # Artifact retrieval → source_type="artifact" + retrieval_source=None
        # (Pattern (b1): artifacts are a distinct DataPoint type; the
        # retrieval_source field describes fact retrieval paths and is
        # intentionally None for non-fact DataPoints.)
        rc_art = RetrievalCandidate(fact=fact, source="artifact", score=0.5)
        item_art = CandidateGenerator.retrieval_candidate_to_item(rc_art)
        assert item_art.source_type == "artifact"
        assert item_art.retrieval_source is None, (
            "Artifact items must leave retrieval_source=None — the field "
            "describes fact retrieval paths, and artifacts are not facts."
        )

    def test_retrieval_candidate_to_item_rejects_unknown_source(self):
        """TODO-6-303 (Round 1 Blind Spot Reviewer, LOW):
        ``retrieval_candidate_to_item`` must reject any ``rc.source`` value
        outside the ``{structural, keyword, vector, graph, artifact}`` set
        at the conversion boundary, failing loud-and-early rather than
        silently constructing a ``WorkingSetItem`` whose ``retrieval_source``
        fails deep Pydantic Literal validation.

        Pins the guard against two real-world vectors:

        1. ``/rerank`` producer (``source="api"``) accidentally piped into
           the working-set converter — the exact scenario the reviewer
           flagged as a latent landmine.
        2. A typo or new retrieval path slipping into the orchestrator
           without updating the Literal or the converter guard.
        """
        import pytest
        fact = make_fact_assertion(text="candidate text")

        # /rerank-style producer value — must be rejected.
        rc_api = RetrievalCandidate(fact=fact, source="api", score=0.5)
        with pytest.raises(ValueError, match="unknown RetrievalCandidate.source"):
            CandidateGenerator.retrieval_candidate_to_item(rc_api)

        # Generic typo — must be rejected.
        rc_typo = RetrievalCandidate(fact=fact, source="vectoor", score=0.5)
        with pytest.raises(ValueError, match="expected one of"):
            CandidateGenerator.retrieval_candidate_to_item(rc_typo)


# ---------------------------------------------------------------------------
# Goal rendering
# ---------------------------------------------------------------------------

class TestGoalRendering:
    def _gen(self):
        return CandidateGenerator(retrieval=AsyncMock())

    def test_render_title_only(self):
        goal = make_goal_state(title="My Goal", description="", success_criteria=[], blockers=[])
        text = self._gen()._render_goal(goal)
        assert text == "Goal: My Goal"

    def test_render_with_description(self):
        goal = make_goal_state(title="G", description="Build it")
        text = self._gen()._render_goal(goal)
        assert "Description: Build it" in text

    def test_render_with_criteria(self):
        goal = make_goal_state(title="G", success_criteria=["tests pass", "deployed"])
        text = self._gen()._render_goal(goal)
        assert "Criteria: tests pass, deployed" in text

    def test_render_with_blockers(self):
        goal = make_goal_state(title="G", blockers=["CI red"])
        text = self._gen()._render_goal(goal)
        assert "BLOCKED BY: CI red" in text

    def test_render_full(self):
        goal = make_goal_state(
            title="Ship", description="Ship v2",
            success_criteria=["tests"], blockers=["infra"],
        )
        text = self._gen()._render_goal(goal)
        parts = text.split(" | ")
        assert parts[0] == "Goal: Ship"
        assert parts[1] == "Description: Ship v2"
        assert parts[2] == "Criteria: tests"
        assert parts[3] == "BLOCKED BY: infra"


# ---------------------------------------------------------------------------
# Exception isolation in gather (#491)
# ---------------------------------------------------------------------------

class TestExceptionIsolation:
    async def test_exception_isolation_gather(self):
        """If one source raises, the others still return results."""
        rc = _make_rc(text="surviving fact")
        retrieval = AsyncMock()
        retrieval.retrieve_candidates = AsyncMock(return_value=[rc])

        # Procedures will raise (graph blows up on second call)
        graph = AsyncMock()
        call_count = {"n": 0}
        async def _cypher_side_effect(query, params=None):
            call_count["n"] += 1
            # Persistent goals query succeeds, procedure query raises
            if "ProcedureDataPoint" in query:
                raise RuntimeError("procedure source exploded")
            return [{"props": {"title": "Global goal", "eb_id": str(uuid.uuid4())}}]
        graph.query_cypher = AsyncMock(side_effect=_cypher_side_effect)

        goal = make_goal_state(title="Session goal", status=GoalStatus.ACTIVE)
        redis = AsyncMock()
        sid = uuid.uuid4()
        redis.get = AsyncMock(
            return_value=json.dumps([goal.model_dump(mode="json")])
        )

        gen = _make_generator(retrieval=retrieval, graph=graph, redis=redis)
        retrieval_cands, direct_items = await gen.generate(
            session_id=sid, session_key="agent:main:main", query="test",
        )
        # Retrieval survived
        assert len(retrieval_cands) == 1
        assert retrieval_cands[0].fact.text == "surviving fact"
        # Session goals survived
        goal_items = [i for i in direct_items if "Session goal" in i.text]
        assert len(goal_items) == 1
        # Procedure items are empty (source raised)
        proc_items = [i for i in direct_items if i.source_type == "procedure"]
        assert proc_items == []


# ---------------------------------------------------------------------------
# Persistent goal with no owners (#487)
# ---------------------------------------------------------------------------

class TestPersistentGoalNoOwner:
    async def test_persistent_goal_no_owner_visible_to_all(self):
        """Goals with no OWNS_GOAL edges are visible to any actor (Phase 5 fallback)."""
        gid = str(uuid.uuid4())
        graph = AsyncMock()
        # The Phase 5 fallback Cypher uses OPTIONAL MATCH + WHERE size(owner_ids) = 0
        # So a goal with no owners should appear for any actor_id
        graph.query_cypher = AsyncMock(return_value=[
            {"props": {"title": "Ownerless goal", "eb_id": gid}},
        ])
        gen = _make_generator(graph=graph)
        random_actor = uuid.uuid4()
        _, direct_items = await gen.generate(
            **_session_args(actor_ids=[random_actor]),
        )
        persistent = [i for i in direct_items if "Ownerless goal" in i.text]
        assert len(persistent) == 1


# ---------------------------------------------------------------------------
# Procedure relevance filter embedding failure (#489)
# ---------------------------------------------------------------------------

class TestProcedureRelevanceFilterFallback:
    async def test_procedure_relevance_filter_embedding_failure_returns_all(self):
        """When embedding service raises, all procedures are returned unfiltered."""
        from elephantbroker.schemas.config import ProcedureCandidateConfig

        pid1 = str(uuid.uuid4())
        pid2 = str(uuid.uuid4())
        graph = AsyncMock()
        graph.query_cypher = AsyncMock(return_value=[
            {"props": {"name": "proc-alpha", "eb_id": pid1}},
            {"props": {"name": "proc-beta", "eb_id": pid2}},
        ])
        embedding_service = AsyncMock()
        embedding_service.embed_text = AsyncMock(side_effect=RuntimeError("embedding down"))

        pcc = ProcedureCandidateConfig(
            enabled=True, filter_by_relevance=True, relevance_threshold=0.3, top_k=5,
        )
        gen = _make_generator(graph=graph, embedding_service=embedding_service)
        gen._procedure_candidate_config = pcc

        _, direct_items = await gen.generate(**_session_args())
        proc_items = [i for i in direct_items if i.source_type == "procedure"]
        assert len(proc_items) == 2
        names = {i.text for i in proc_items}
        assert any("proc-alpha" in n for n in names)
        assert any("proc-beta" in n for n in names)


# ---------------------------------------------------------------------------
# Token size estimation (#492)
# ---------------------------------------------------------------------------

class TestTokenSizeEstimation:
    def test_token_size_estimation_len_div_4(self):
        """Token estimation uses len(text) // 4 character-based approximation."""
        text_40_chars = "A" * 40  # 40 chars => 10 tokens
        fact = make_fact_assertion(text=text_40_chars, token_size=None)
        rc = RetrievalCandidate(fact=fact, source="structural", score=0.5)
        item = CandidateGenerator.retrieval_candidate_to_item(rc)
        assert item.token_size == 10  # 40 // 4

    async def test_token_size_estimation_goal_item(self):
        """Goal WorkingSetItems also use len(text) // 4."""
        goal = make_goal_state(title="A" * 24, description="", status=GoalStatus.ACTIVE,
                               success_criteria=[], blockers=[])
        redis = AsyncMock()
        sid = uuid.uuid4()
        redis.get = AsyncMock(
            return_value=json.dumps([goal.model_dump(mode="json")])
        )
        gen = _make_generator(redis=redis)
        _, direct_items = await gen.generate(
            session_id=sid, session_key="agent:main:main", query="test",
        )
        goal_item = [i for i in direct_items if i.source_type == "goal"][0]
        # Text is "Goal: " + "A"*24 = 30 chars => 7 tokens
        assert goal_item.token_size == len(goal_item.text) // 4

    async def test_token_size_estimation_procedure_item(self):
        """Procedure WorkingSetItems also use len(text) // 4."""
        pid = str(uuid.uuid4())
        graph = AsyncMock()
        graph.query_cypher = AsyncMock(return_value=[
            {"props": {"name": "B" * 20, "eb_id": pid}},
        ])
        gen = _make_generator(graph=graph)
        _, direct_items = await gen.generate(**_session_args())
        proc_item = [i for i in direct_items if i.source_type == "procedure"][0]
        # Text is "Procedure: " + "B"*20 = 31 chars => 7 tokens
        assert proc_item.token_size == len(proc_item.text) // 4


# ---------------------------------------------------------------------------
# Sub-goal blocker propagation (#490)
# ---------------------------------------------------------------------------

class TestSubgoalBlockerPropagation:
    async def test_subgoal_blocker_propagation(self):
        """Sub-goal blockers are rendered in the parent goal's candidate text."""
        parent_id = uuid.uuid4()
        parent = make_goal_state(
            id=parent_id, title="Parent task", status=GoalStatus.ACTIVE, blockers=[],
        )
        child = make_goal_state(
            title="Child task", status=GoalStatus.ACTIVE,
            parent_goal_id=parent_id, blockers=["waiting for API key"],
        )
        redis = AsyncMock()
        sid = uuid.uuid4()
        redis.get = AsyncMock(
            return_value=json.dumps([
                parent.model_dump(mode="json"),
                child.model_dump(mode="json"),
            ])
        )
        gen = _make_generator(redis=redis)
        _, direct_items = await gen.generate(
            session_id=sid, session_key="agent:main:main", query="test",
        )
        parent_item = [i for i in direct_items if "Parent task" in i.text][0]
        assert "waiting for API key" in parent_item.text
        assert "Sub-task blockers" in parent_item.text


# ---------------------------------------------------------------------------
# Random UUID on missing eb_id (#1327)
# ---------------------------------------------------------------------------

class TestMissingEbId:
    async def test_random_uuid_on_missing_eb_id(self):
        """When a graph record has no eb_id, a random UUID is assigned."""
        graph = AsyncMock()
        graph.query_cypher = AsyncMock(return_value=[
            {"props": {"name": "no-id procedure"}},  # no eb_id key
        ])
        gen = _make_generator(graph=graph)
        _, direct_items = await gen.generate(**_session_args())
        proc_items = [i for i in direct_items if i.source_type == "procedure"]
        assert len(proc_items) == 1
        # Must have a valid UUID (not empty, not crash)
        parsed = uuid.UUID(proc_items[0].id)
        assert parsed.version == 4

    async def test_random_uuid_on_empty_eb_id_persistent_goal(self):
        """Persistent goal with empty eb_id gets a random UUID."""
        graph = AsyncMock()
        graph.query_cypher = AsyncMock(return_value=[
            {"props": {"title": "Goal without ID"}},  # no eb_id key
        ])
        gen = _make_generator(graph=graph)
        _, direct_items = await gen.generate(**_session_args())
        goal_items = [i for i in direct_items if "Goal without ID" in i.text]
        assert len(goal_items) == 1
        parsed = uuid.UUID(goal_items[0].id)
        assert parsed.version == 4
