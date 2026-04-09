"""Integration tests for the working set pipeline against real infrastructure.

These tests exercise the full scoring pipeline (candidates -> rerank -> score -> select)
against live Neo4j, Qdrant, and Redis.  They are skipped unless Docker infrastructure
is running and the ``integration`` marker is selected.
"""
from __future__ import annotations

import uuid

import pytest
import pytest_asyncio

from elephantbroker.runtime.container import RuntimeContainer
from elephantbroker.schemas.config import ElephantBrokerConfig
from elephantbroker.schemas.tiers import BusinessTier
from tests.fixtures.factories import make_fact_assertion, make_goal_state

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def container(monkeypatch):
    """Build a full RuntimeContainer wired to Docker test services.

    TODO-3-510: EB_GATEWAY_ID is explicitly set to a non-empty distinctive
    value here so that test_end_to_end_build_working_set below is NOT
    tautologically satisfied. Without this monkeypatch, the integration
    fixture runs with no EB_GATEWAY_ID env override, which means
    ElephantBrokerConfig.load() returns the post-Bucket-A sentinel default
    of "" (empty string) and the WorkingSetManager instantiated inside the
    container carries `_gateway_id == ""`. Under that state, the assertion
      assert snapshot.gateway_id == container.working_set_manager._gateway_id
    collapses to `"" == ""` and passes regardless of whether the stamping
    contract is actually honored. Seeding a real value at fixture entry
    turns that assertion into a meaningful contract check.

    The value "test-ws-gateway" is deliberately distinct from the guards
    subpackage's `GATEWAY_ID = "test-gw"` hardcoded constant (see
    tests/integration/runtime/guards/conftest.py) and from the Batch 4
    explicit values ("gw-alpha", "gw-beta", "gw-owner", etc.) so a mismatch
    during cross-module refactoring fails loudly instead of accidentally
    aligning.
    """
    monkeypatch.setenv("EB_GATEWAY_ID", "test-ws-gateway")
    config = ElephantBrokerConfig.load()
    c = await RuntimeContainer.from_config(config, BusinessTier.FULL)
    yield c
    try:
        await c.close()
    except Exception:
        pass


class TestWorkingSetIntegration:
    """End-to-end working set pipeline tests."""

    async def test_store_facts_then_build_working_set(self, container):
        """Store facts via facade, build working set, verify snapshot shape."""
        assert container.memory_store is not None
        assert container.working_set_manager is not None

        for i in range(5):
            fact = make_fact_assertion(text=f"Integration test fact {i}")
            await container.memory_store.store(fact, dedup_threshold=1.0)

        session_id = uuid.uuid4()
        snapshot = await container.working_set_manager.build_working_set(
            session_id=session_id,
            session_key="test:integration",
            profile_name="coding",
            query="integration test",
        )
        assert snapshot is not None
        assert snapshot.session_id == session_id
        assert snapshot.tokens_used <= snapshot.token_budget

    async def test_empty_store_returns_empty_snapshot(self, container):
        """Building a working set against empty stores produces a valid snapshot."""
        assert container.working_set_manager is not None

        snapshot = await container.working_set_manager.build_working_set(
            session_id=uuid.uuid4(),
            session_key="test:empty",
            profile_name="coding",
            query="no facts stored yet",
        )
        assert snapshot is not None
        assert snapshot.tokens_used == 0
        assert len(snapshot.items) == 0

    async def test_snapshot_respects_token_budget(self, container):
        """Items selected must not exceed the profile's token budget."""
        assert container.memory_store is not None
        assert container.working_set_manager is not None

        # Store many facts so the selector has to compete
        for i in range(20):
            fact = make_fact_assertion(
                text=f"Budget test fact number {i} with moderate length text to consume tokens",
            )
            await container.memory_store.store(fact, dedup_threshold=1.0)

        snapshot = await container.working_set_manager.build_working_set(
            session_id=uuid.uuid4(),
            session_key="test:budget",
            profile_name="coding",
            query="budget test",
        )
        assert snapshot.tokens_used <= snapshot.token_budget

    async def test_snapshot_weights_match_profile(self, container):
        """The snapshot's weights_used should reflect the resolved profile."""
        assert container.working_set_manager is not None

        snapshot = await container.working_set_manager.build_working_set(
            session_id=uuid.uuid4(),
            session_key="test:weights",
            profile_name="coding",
            query="weights check",
        )
        # coding profile has turn_relevance=1.5 and recency=1.2
        assert snapshot.weights_used.turn_relevance == pytest.approx(1.5, abs=0.01)
        assert snapshot.weights_used.recency == pytest.approx(1.2, abs=0.01)

    async def test_get_working_set_after_build(self, container):
        """get_working_set should return the cached snapshot by session_id."""
        assert container.working_set_manager is not None

        session_id = uuid.uuid4()
        original = await container.working_set_manager.build_working_set(
            session_id=session_id,
            session_key="test:cache",
            profile_name="coding",
            query="cache roundtrip",
        )
        retrieved = await container.working_set_manager.get_working_set(session_id)
        assert retrieved is not None
        assert retrieved.snapshot_id == original.snapshot_id

    async def test_get_working_set_missing_returns_none(self, container):
        """get_working_set for an unknown session returns None."""
        assert container.working_set_manager is not None

        result = await container.working_set_manager.get_working_set(uuid.uuid4())
        assert result is None

    async def test_scoring_trace_event_emitted(self, container):
        """build_working_set emits a SCORING_COMPLETED trace event."""
        from elephantbroker.schemas.trace import TraceEventType, TraceQuery

        assert container.working_set_manager is not None
        assert container.trace_ledger is not None

        await container.working_set_manager.build_working_set(
            session_id=uuid.uuid4(),
            session_key="test:trace",
            profile_name="coding",
            query="trace event check",
        )
        events = await container.trace_ledger.query_trace(TraceQuery())
        scoring_events = [
            e for e in events
            if e.event_type == TraceEventType.SCORING_COMPLETED
        ]
        assert len(scoring_events) >= 1

    async def test_end_to_end_build_working_set(self, container):
        """End-to-end: store facts + create session goals, build working set,
        verify both fact and goal items appear in snapshot with valid scores."""
        assert container.memory_store is not None
        assert container.working_set_manager is not None

        # Store facts
        for i in range(3):
            fact = make_fact_assertion(text=f"E2E pipeline fact {i}")
            await container.memory_store.store(fact, dedup_threshold=1.0)

        # Create session goal
        session_id = uuid.uuid4()
        if container.session_goal_store:
            goal = make_goal_state(title="E2E test goal")
            await container.session_goal_store.add_goal(
                "test:e2e", session_id, goal,
            )

        snapshot = await container.working_set_manager.build_working_set(
            session_id=session_id,
            session_key="test:e2e",
            profile_name="coding",
            query="E2E pipeline test",
        )
        assert snapshot is not None
        assert snapshot.session_id == session_id
        assert snapshot.tokens_used <= snapshot.token_budget
        # TODO-3-510: Verify gateway_id is stamped to match the manager's
        # configured value. The `container` fixture above explicitly sets
        # EB_GATEWAY_ID="test-ws-gateway" via monkeypatch so both sides of
        # this equality carry a real distinctive value instead of the
        # post-Bucket-A "" sentinel that ElephantBrokerConfig.load() would
        # otherwise produce under no env override (see TD-41 in
        # TECHNICAL-DEBT.md for the sentinel rationale). Pre-510 the
        # assertion was tautologically satisfied — both sides evaluated to
        # "" regardless of whether the WorkingSetManager actually did the
        # stamping, so the contract went unchecked. Post-510 the assertion
        # fails loudly if working_set/manager.py:195's
        # `snapshot.gateway_id or self._gateway_id` coalescing ever gets
        # replaced with a hardcoded value or silently dropped. The contract
        # is "snapshot carries the manager's gateway_id byte-identically"
        # and it now holds under a real gateway_id the way it needs to in
        # any multi-tenant (e.g. "hub-01") deployment.
        assert snapshot.gateway_id == container.working_set_manager._gateway_id
        # Must contain items (facts and/or goals)
        assert len(snapshot.items) > 0
        # All items should have scores
        for item in snapshot.items:
            assert item.scores is not None

    async def test_different_profiles_produce_different_weights(self, container):
        """Building with different profile names should yield different weight vectors."""
        assert container.working_set_manager is not None

        snap_coding = await container.working_set_manager.build_working_set(
            session_id=uuid.uuid4(),
            session_key="test:profile:coding",
            profile_name="coding",
            query="profile comparison",
        )
        snap_research = await container.working_set_manager.build_working_set(
            session_id=uuid.uuid4(),
            session_key="test:profile:research",
            profile_name="research",
            query="profile comparison",
        )
        # coding and research profiles have different weight vectors
        assert snap_coding.weights_used != snap_research.weights_used
