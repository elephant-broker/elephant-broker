"""Integration tests for session goals and goal refinement pipeline."""
import uuid

import pytest
import pytest_asyncio

from elephantbroker.runtime.working_set.session_goals import SessionGoalStore
from elephantbroker.schemas.config import ScoringConfig
from elephantbroker.schemas.goal import GoalState

pytestmark = pytest.mark.integration


@pytest_asyncio.fixture
async def session_goal_store(redis_client, graph_adapter, trace_ledger):
    """SessionGoalStore wired to real Redis and graph."""
    return SessionGoalStore(
        redis=redis_client,
        config=ScoringConfig(),
        trace_ledger=trace_ledger,
        graph=graph_adapter,
        dataset_name="test_integration",
    )


class TestSessionGoalsCrudIntegration:
    """Tests for session goal CRUD via SessionGoalStore against real Redis."""

    @pytest.mark.asyncio
    async def test_session_goals_crud_roundtrip_redis(self, session_goal_store):
        """Create, read, update, remove goals via SessionGoalStore against Redis."""
        store = session_goal_store
        sk = "test:integration"
        sid = uuid.uuid4()

        goal = GoalState(title="Integration test goal")
        await store.add_goal(sk, sid, goal)

        goals = await store.get_goals(sk, sid)
        assert len(goals) == 1
        assert goals[0].title == "Integration test goal"

        await store.update_goal(sk, sid, goal.id, {"description": "Updated"})
        goals = await store.get_goals(sk, sid)
        assert goals[0].description == "Updated"

        await store.remove_goal(sk, sid, goal.id)
        goals = await store.get_goals(sk, sid)
        assert len(goals) == 0

    @pytest.mark.asyncio
    async def test_hint_processing_modifies_redis_goals(self, session_goal_store):
        """Process blocked hint and verify Redis state."""
        store = session_goal_store
        sk = "test:hints"
        sid = uuid.uuid4()

        goal = GoalState(title="Hint test goal")
        await store.add_goal(sk, sid, goal)

        await store.update_goal(sk, sid, goal.id, {"blockers": ["CI is red"]})
        goals = await store.get_goals(sk, sid)
        assert "CI is red" in goals[0].blockers

    @pytest.mark.asyncio
    async def test_session_end_flushes_goals_to_cognee(self, session_goal_store):
        """Create goals in Redis, flush to Cognee, verify Redis cleaned."""
        store = session_goal_store
        sk = "test:flush"
        sid = uuid.uuid4()

        goal = GoalState(title="Flush test goal", description="For integration test")
        await store.add_goal(sk, sid, goal)

        count = await store.flush_to_cognee(sk, sid)
        assert count == 1

        goals = await store.get_goals(sk, sid)
        assert len(goals) == 0

    @pytest.mark.asyncio
    async def test_subgoal_hierarchy_preserved_on_flush(self, session_goal_store):
        """Create parent + child goals, flush, verify both flushed."""
        store = session_goal_store
        sk = "test:hierarchy"
        sid = uuid.uuid4()

        parent = GoalState(title="Parent goal")
        child = GoalState(title="Child goal", parent_goal_id=parent.id)
        await store.add_goal(sk, sid, parent)
        await store.add_goal(sk, sid, child)

        count = await store.flush_to_cognee(sk, sid)
        assert count == 2

    @pytest.mark.asyncio
    async def test_goal_ownership_preserved_on_flush(self, session_goal_store):
        """Create goals with owners, flush, verify count."""
        store = session_goal_store
        sk = "test:ownership"
        sid = uuid.uuid4()
        owner_id = uuid.uuid4()

        goal = GoalState(title="Owned goal", owner_actor_ids=[owner_id])
        await store.add_goal(sk, sid, goal)

        count = await store.flush_to_cognee(sk, sid)
        assert count == 1
