"""Tests for WorkingSetManager — full scoring pipeline."""
import json
import uuid
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest

from elephantbroker.runtime.interfaces.retrieval import RetrievalCandidate
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.runtime.working_set.candidates import CandidateGenerator
from elephantbroker.runtime.working_set.manager import WorkingSetManager
from elephantbroker.schemas.config import ElephantBrokerConfig
from elephantbroker.schemas.goal import GoalState
from elephantbroker.schemas.working_set import ScoringWeights, WorkingSetItem, WorkingSetSnapshot
from tests.fixtures.factories import make_fact_assertion, make_goal_state, make_profile_policy


def _make_rc(text="fact", score=0.8, token_size=100, **kwargs) -> RetrievalCandidate:
    fact = make_fact_assertion(text=text, token_size=token_size)
    return RetrievalCandidate(fact=fact, source="structural", score=score)


def _make_manager(**overrides):
    retrieval = AsyncMock()
    retrieval.retrieve_candidates = AsyncMock(return_value=[])
    ledger = TraceLedger()
    defaults = dict(
        retrieval=retrieval,
        trace_ledger=ledger,
        config=ElephantBrokerConfig(),
    )
    defaults.update(overrides)
    return WorkingSetManager(**defaults), retrieval, ledger


class TestBuildWorkingSet:
    @pytest.mark.asyncio
    async def test_build_empty_returns_snapshot(self):
        mgr, retrieval, _ = _make_manager()
        snapshot = await mgr.build_working_set(
            session_id=uuid.uuid4(), session_key="agent:main:main",
            profile_name="coding", query="test",
        )
        assert isinstance(snapshot, WorkingSetSnapshot)
        assert len(snapshot.items) == 0

    @pytest.mark.asyncio
    async def test_build_with_candidates(self):
        mgr, retrieval, _ = _make_manager()
        rc = _make_rc(text="relevant fact", token_size=100)
        retrieval.retrieve_candidates = AsyncMock(return_value=[rc])
        snapshot = await mgr.build_working_set(
            session_id=uuid.uuid4(), session_key="agent:main:main",
            profile_name="coding", query="relevant",
        )
        assert len(snapshot.items) >= 1

    @pytest.mark.asyncio
    async def test_build_resolves_profile_from_registry(self):
        profile_reg = AsyncMock()
        profile = make_profile_policy()
        profile_reg.get_effective_policy = AsyncMock(return_value=profile)
        mgr, _, _ = _make_manager(profile_registry=profile_reg)
        await mgr.build_working_set(
            session_id=uuid.uuid4(), session_key="agent:main:main",
            profile_name="coding", query="test",
        )
        profile_reg.get_effective_policy.assert_called_once_with("coding", org_id=None)

    @pytest.mark.asyncio
    async def test_build_calls_candidate_generator(self):
        mgr, retrieval, _ = _make_manager()
        rc = _make_rc(token_size=50)
        retrieval.retrieve_candidates = AsyncMock(return_value=[rc])
        snapshot = await mgr.build_working_set(
            session_id=uuid.uuid4(), session_key="agent:main:main",
            profile_name="coding", query="test",
        )
        retrieval.retrieve_candidates.assert_called_once()

    @pytest.mark.asyncio
    async def test_build_reranks_when_available(self):
        rerank = AsyncMock()
        rerank.rerank = AsyncMock(side_effect=lambda cs, q, **kw: cs)
        mgr, retrieval, _ = _make_manager(rerank=rerank)
        rc = _make_rc(token_size=50)
        retrieval.retrieve_candidates = AsyncMock(return_value=[rc])
        await mgr.build_working_set(
            session_id=uuid.uuid4(), session_key="agent:main:main",
            profile_name="coding", query="test",
        )
        rerank.rerank.assert_called_once()

    @pytest.mark.asyncio
    async def test_build_skips_rerank_when_unavailable(self):
        mgr, retrieval, _ = _make_manager(rerank=None)
        rc = _make_rc(token_size=50)
        retrieval.retrieve_candidates = AsyncMock(return_value=[rc])
        snapshot = await mgr.build_working_set(
            session_id=uuid.uuid4(), session_key="agent:main:main",
            profile_name="coding", query="test",
        )
        assert len(snapshot.items) >= 1


class TestGetWorkingSet:
    @pytest.mark.asyncio
    async def test_returns_none_when_not_built(self):
        mgr, _, _ = _make_manager()
        result = await mgr.get_working_set(uuid.uuid4())
        assert result is None

    @pytest.mark.asyncio
    async def test_returns_cached_snapshot(self):
        mgr, retrieval, _ = _make_manager()
        retrieval.retrieve_candidates = AsyncMock(return_value=[])
        sid = uuid.uuid4()
        await mgr.build_working_set(
            session_id=sid, session_key="agent:main:main",
            profile_name="coding", query="test",
        )
        result = await mgr.get_working_set(sid)
        assert result is not None
        assert result.session_id == sid


class TestPreComputation:
    @pytest.mark.asyncio
    async def test_build_with_embeddings(self):
        embeddings = AsyncMock()
        embeddings.embed_text = AsyncMock(return_value=[0.1] * 10)
        embeddings.embed_batch = AsyncMock(return_value=[[0.1] * 10])
        mgr, retrieval, _ = _make_manager(embedding_service=embeddings)
        rc = _make_rc(token_size=50)
        retrieval.retrieve_candidates = AsyncMock(return_value=[rc])
        snapshot = await mgr.build_working_set(
            session_id=uuid.uuid4(), session_key="agent:main:main",
            profile_name="coding", query="test",
        )
        embeddings.embed_text.assert_called()

    @pytest.mark.asyncio
    async def test_build_with_graph_precomputation(self):
        graph = AsyncMock()
        graph.query_cypher = AsyncMock(return_value=[])
        mgr, retrieval, _ = _make_manager(graph=graph)
        await mgr.build_working_set(
            session_id=uuid.uuid4(), session_key="agent:main:main",
            profile_name="coding", query="test",
        )
        # Should have been called for evidence, verification, conflicts
        assert graph.query_cypher.call_count >= 3


class TestCachingAndTrace:
    @pytest.mark.asyncio
    async def test_build_caches_in_redis(self):
        redis = AsyncMock()
        redis.get = AsyncMock(return_value=None)
        redis.setex = AsyncMock()
        redis.mget = AsyncMock(return_value=[])
        mgr, retrieval, _ = _make_manager(redis=redis)
        await mgr.build_working_set(
            session_id=uuid.uuid4(), session_key="agent:main:main",
            profile_name="coding", query="test",
        )
        redis.setex.assert_called()

    @pytest.mark.asyncio
    async def test_build_emits_scoring_completed(self):
        mgr, _, ledger = _make_manager()
        await mgr.build_working_set(
            session_id=uuid.uuid4(), session_key="agent:main:main",
            profile_name="coding", query="test",
        )
        events = ledger._events
        assert any(e.event_type.value == "scoring_completed" for e in events)


class TestBudgetSelection:
    @pytest.mark.asyncio
    async def test_budget_limits_selection(self):
        mgr, retrieval, _ = _make_manager()
        # 3 candidates at 5000 tokens each, 8000 budget → only 1 fits
        rcs = [_make_rc(text=f"fact {i}", token_size=5000, score=0.9) for i in range(3)]
        retrieval.retrieve_candidates = AsyncMock(return_value=rcs)
        snapshot = await mgr.build_working_set(
            session_id=uuid.uuid4(), session_key="agent:main:main",
            profile_name="coding", query="test",
        )
        assert len(snapshot.items) <= 2
        assert snapshot.tokens_used <= 8000

    @pytest.mark.asyncio
    async def test_build_with_no_redis_all_fallbacks(self):
        mgr, retrieval, _ = _make_manager(redis=None, graph=None, embedding_service=None)
        rc = _make_rc(token_size=50)
        retrieval.retrieve_candidates = AsyncMock(return_value=[rc])
        snapshot = await mgr.build_working_set(
            session_id=uuid.uuid4(), session_key="agent:main:main",
            profile_name="coding", query="test",
        )
        assert len(snapshot.items) >= 1


class TestGraphPreComputationDetails:
    """Verify that each graph pre-computation fires a distinct Cypher query."""

    @pytest.mark.asyncio
    async def test_build_pre_computes_evidence_index_via_cypher(self):
        """Evidence index pre-computation fires a Cypher query containing SUPPORTS."""
        graph = AsyncMock()
        graph.query_cypher = AsyncMock(return_value=[])
        mgr, _, _ = _make_manager(graph=graph)
        await mgr.build_working_set(
            session_id=uuid.uuid4(), session_key="agent:main:main",
            profile_name="coding", query="test",
        )
        cypher_calls = [
            str(call.args[0]) for call in graph.query_cypher.call_args_list
        ]
        assert any("EvidenceDataPoint" in c and "SUPPORTS" in c for c in cypher_calls)

    @pytest.mark.asyncio
    async def test_build_pre_computes_verification_index(self):
        """Verification index pre-computation fires a Cypher query on ClaimDataPoint status."""
        graph = AsyncMock()
        graph.query_cypher = AsyncMock(return_value=[])
        mgr, _, _ = _make_manager(graph=graph)
        await mgr.build_working_set(
            session_id=uuid.uuid4(), session_key="agent:main:main",
            profile_name="coding", query="test",
        )
        cypher_calls = [
            str(call.args[0]) for call in graph.query_cypher.call_args_list
        ]
        assert any("ClaimDataPoint" in c and "status" in c for c in cypher_calls)

    @pytest.mark.asyncio
    async def test_build_pre_computes_conflict_pairs(self):
        """Conflict pairs pre-computation fires a Cypher query with SUPERSEDES|CONTRADICTS."""
        graph = AsyncMock()
        graph.query_cypher = AsyncMock(return_value=[])
        mgr, _, _ = _make_manager(graph=graph)
        await mgr.build_working_set(
            session_id=uuid.uuid4(), session_key="agent:main:main",
            profile_name="coding", query="test",
        )
        cypher_calls = [
            str(call.args[0]) for call in graph.query_cypher.call_args_list
        ]
        assert any("SUPERSEDES" in c and "CONTRADICTS" in c for c in cypher_calls)


class TestCandidateConversion:
    @pytest.mark.asyncio
    async def test_build_converts_reranked_candidates_to_items(self):
        """Verify retrieval_candidate_to_item is used to convert candidates."""
        mgr, retrieval, _ = _make_manager()
        rc = _make_rc(text="converted fact", token_size=50)
        retrieval.retrieve_candidates = AsyncMock(return_value=[rc])
        snapshot = await mgr.build_working_set(
            session_id=uuid.uuid4(), session_key="agent:main:main",
            profile_name="coding", query="test",
        )
        # The snapshot items should contain items with source_type from the RC
        assert len(snapshot.items) >= 1
        converted = snapshot.items[0]
        assert isinstance(converted, WorkingSetItem)
        assert converted.text == "converted fact"


class TestScoringPipeline:
    @pytest.mark.asyncio
    async def test_build_scores_all_candidates_pass1(self):
        """ScoringEngine.score_independent runs on every candidate item."""
        mgr, retrieval, _ = _make_manager()
        rcs = [_make_rc(text=f"fact {i}", token_size=50) for i in range(3)]
        retrieval.retrieve_candidates = AsyncMock(return_value=rcs)
        snapshot = await mgr.build_working_set(
            session_id=uuid.uuid4(), session_key="agent:main:main",
            profile_name="coding", query="test",
        )
        # Every item in the snapshot must have a non-default scores object
        # (score_independent sets at least recency and successful_use_prior)
        for item in snapshot.items:
            assert item.scores is not None
            # final was computed (weighted_sum was called)
            assert isinstance(item.scores.final, float)

    @pytest.mark.asyncio
    async def test_build_selects_via_budget_selector_pass2(self):
        """BudgetSelector is used: oversized candidates get excluded."""
        mgr, retrieval, _ = _make_manager()
        # 5 candidates at 3000 tokens each, 8000 budget -> at most 2 fit
        rcs = [_make_rc(text=f"fact {i}", token_size=3000, score=0.9) for i in range(5)]
        retrieval.retrieve_candidates = AsyncMock(return_value=rcs)
        snapshot = await mgr.build_working_set(
            session_id=uuid.uuid4(), session_key="agent:main:main",
            profile_name="coding", query="test",
        )
        assert len(snapshot.items) <= 2
        assert snapshot.tokens_used <= 8000


class TestProfileWeights:
    @pytest.mark.asyncio
    async def test_coding_profile_favors_turn_relevance(self):
        """The coding profile sets turn_relevance=1.5 (higher than default 1.0)."""
        from elephantbroker.runtime.profiles.registry import ProfileRegistry

        registry = ProfileRegistry(TraceLedger())
        profile = await registry.get_effective_policy("coding")
        assert profile.scoring_weights.turn_relevance == 1.5


class TestEmptyRetrieval:
    @pytest.mark.asyncio
    async def test_build_with_empty_retrieval_only_goals_returned(self):
        """When retrieval returns empty but goals exist, snapshot contains goal items."""
        redis = AsyncMock()
        goal = make_goal_state(title="Active goal")
        goals_json = json.dumps([goal.model_dump(mode="json")])
        redis.get = AsyncMock(return_value=goals_json)
        redis.setex = AsyncMock()
        redis.mget = AsyncMock(return_value=[])

        mgr, retrieval, _ = _make_manager(redis=redis)
        retrieval.retrieve_candidates = AsyncMock(return_value=[])

        snapshot = await mgr.build_working_set(
            session_id=uuid.uuid4(), session_key="agent:main:main",
            profile_name="coding", query="test",
        )
        # Goal items should be present even though retrieval was empty
        goal_items = [it for it in snapshot.items if it.source_type == "goal"]
        assert len(goal_items) >= 1


class TestGoalEmbeddings:
    @pytest.mark.asyncio
    async def test_build_goal_embeddings_computed(self):
        """When session goals are present, goal embeddings are computed via embed_batch."""
        embeddings = AsyncMock()
        embeddings.embed_text = AsyncMock(return_value=[0.1] * 10)
        embeddings.embed_batch = AsyncMock(return_value=[[0.2] * 10])

        redis = AsyncMock()
        goal = make_goal_state(title="Build feature X")
        goals_json = json.dumps([goal.model_dump(mode="json")])
        redis.get = AsyncMock(return_value=goals_json)
        redis.setex = AsyncMock()
        redis.mget = AsyncMock(return_value=[])

        mgr, _, _ = _make_manager(embedding_service=embeddings, redis=redis)
        await mgr.build_working_set(
            session_id=uuid.uuid4(), session_key="agent:main:main",
            profile_name="coding", query="test",
        )
        # embed_batch should have been called at least once for goal text embeddings
        embeddings.embed_batch.assert_called()


class TestSessionGoalLoading:
    @pytest.mark.asyncio
    async def test_session_goals_loaded_from_redis_primary(self):
        """Session goals are loaded from Redis via redis.get with the correct
        key pattern. Since PR #5 C19 the key routes unconditionally through
        RedisKeyBuilder, so the gateway prefix is present even without an
        explicit builder — the substring check stays broad enough to cover
        both ``eb:{gw}:session_goals:`` and the empty-gateway form."""
        redis = AsyncMock()
        redis.get = AsyncMock(return_value=None)
        redis.setex = AsyncMock()
        redis.mget = AsyncMock(return_value=[])

        sid = uuid.uuid4()
        mgr, _, _ = _make_manager(redis=redis)
        await mgr.build_working_set(
            session_id=sid, session_key="agent:main:main",
            profile_name="coding", query="test",
        )
        # redis.get should have been called with a session_goals key pattern
        get_calls = [str(call.args[0]) for call in redis.get.call_args_list]
        assert any(":session_goals:" in c for c in get_calls)


class TestProfileRegistryFailure:
    @pytest.mark.asyncio
    async def test_build_with_profile_registry_failure_uses_defaults(self):
        """When profile_registry.get_effective_policy raises, default weights are used."""
        profile_reg = AsyncMock()
        profile_reg.get_effective_policy = AsyncMock(side_effect=RuntimeError("boom"))
        mgr, retrieval, _ = _make_manager(profile_registry=profile_reg)
        rc = _make_rc(token_size=50)
        retrieval.retrieve_candidates = AsyncMock(return_value=[rc])
        snapshot = await mgr.build_working_set(
            session_id=uuid.uuid4(), session_key="agent:main:main",
            profile_name="coding", query="test",
        )
        # Should succeed with default weights (turn_relevance=1.0, not coding's 1.5)
        assert isinstance(snapshot, WorkingSetSnapshot)
        assert snapshot.weights_used.turn_relevance == 1.0
