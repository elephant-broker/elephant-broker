"""Tests for SessionGoalStore — Redis-backed CRUD with Cognee flush."""
from __future__ import annotations

import json
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elephantbroker.runtime.working_set.session_goals import SessionGoalStore
from elephantbroker.schemas.config import ScoringConfig
from elephantbroker.schemas.goal import GoalState, GoalStatus
from elephantbroker.schemas.trace import TraceEventType
from tests.fixtures.factories import make_goal_state


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_store(
    redis=None,
    config: ScoringConfig | None = None,
    trace_ledger=None,
    graph=None,
    dataset_name: str = "test_ds",
) -> tuple[SessionGoalStore, AsyncMock]:
    """Build a SessionGoalStore with an AsyncMock redis and return both."""
    redis = redis or AsyncMock()
    redis.get = redis.get if hasattr(redis.get, "return_value") else AsyncMock(return_value=None)
    redis.setex = redis.setex if hasattr(redis.setex, "return_value") else AsyncMock()
    redis.delete = redis.delete if hasattr(redis.delete, "return_value") else AsyncMock()
    store = SessionGoalStore(
        redis=redis,
        config=config,
        trace_ledger=trace_ledger,
        graph=graph,
        dataset_name=dataset_name,
    )
    return store, redis


SESSION_KEY = "agent:main:main"
SESSION_ID = uuid.uuid4()


# ---------------------------------------------------------------------------
# Basic CRUD
# ---------------------------------------------------------------------------

class TestGetGoals:
    async def test_get_goals_empty(self):
        """Returns empty list when Redis has no data for the key."""
        store, redis = _make_store()
        redis.get = AsyncMock(return_value=None)
        goals = await store.get_goals(SESSION_KEY, SESSION_ID)
        assert goals == []

    async def test_get_goals_returns_deserialized(self):
        """Goals stored as JSON are deserialized back to GoalState list."""
        goal = make_goal_state(title="Alpha")
        data = json.dumps([goal.model_dump(mode="json")])
        store, redis = _make_store()
        redis.get = AsyncMock(return_value=data)
        goals = await store.get_goals(SESSION_KEY, SESSION_ID)
        assert len(goals) == 1
        assert goals[0].title == "Alpha"


class TestSetGoals:
    async def test_set_goals_roundtrip(self):
        """set_goals serializes to JSON, get_goals deserializes back."""
        store, redis = _make_store()

        g1 = make_goal_state(title="One")
        g2 = make_goal_state(title="Two")

        # Capture what setex stores
        stored = {}

        async def fake_setex(key, ttl, value):
            stored["key"] = key
            stored["ttl"] = ttl
            stored["value"] = value

        redis.setex = fake_setex

        await store.set_goals(SESSION_KEY, SESSION_ID, [g1, g2])

        # Now simulate get by returning the stored value
        redis.get = AsyncMock(return_value=stored["value"])
        goals = await store.get_goals(SESSION_KEY, SESSION_ID)
        assert len(goals) == 2
        assert {g.title for g in goals} == {"One", "Two"}

    async def test_set_goals_uses_ttl(self):
        """setex is called with the configured TTL."""
        config = ScoringConfig(session_goals_ttl_seconds=7200)
        store, redis = _make_store(config=config)

        await store.set_goals(SESSION_KEY, SESSION_ID, [make_goal_state()])
        call_args = redis.setex.call_args
        assert call_args[0][1] == 7200  # TTL parameter


class TestAddGoal:
    async def test_add_goal_appends(self):
        """add_goal appends to existing goals and persists."""
        existing = make_goal_state(title="Existing")
        new_goal = make_goal_state(title="New")
        data = json.dumps([existing.model_dump(mode="json")])

        store, redis = _make_store()
        redis.get = AsyncMock(return_value=data)

        stored_value = None

        async def capture_setex(key, ttl, value):
            nonlocal stored_value
            stored_value = value

        redis.setex = capture_setex

        result = await store.add_goal(SESSION_KEY, SESSION_ID, new_goal)
        assert result.title == "New"

        # Verify both goals are stored
        parsed = json.loads(stored_value)
        assert len(parsed) == 2
        titles = {g["title"] for g in parsed}
        assert titles == {"Existing", "New"}


class TestUpdateGoal:
    async def test_update_goal_modifies(self):
        """update_goal changes specified fields on the matching goal."""
        goal = make_goal_state(title="Original")
        data = json.dumps([goal.model_dump(mode="json")])

        store, redis = _make_store()
        redis.get = AsyncMock(return_value=data)

        stored_value = None

        async def capture_setex(key, ttl, value):
            nonlocal stored_value
            stored_value = value

        redis.setex = capture_setex

        updated = await store.update_goal(
            SESSION_KEY, SESSION_ID, goal.id,
            {"title": "Updated", "status": GoalStatus.COMPLETED},
        )
        assert updated is not None
        assert updated.title == "Updated"
        assert updated.status == GoalStatus.COMPLETED

        # Verify persisted
        parsed = json.loads(stored_value)
        assert parsed[0]["title"] == "Updated"

    async def test_update_goal_not_found(self):
        """update_goal returns None when goal_id does not exist."""
        store, redis = _make_store()
        redis.get = AsyncMock(return_value="[]")
        result = await store.update_goal(
            SESSION_KEY, SESSION_ID, uuid.uuid4(), {"title": "Nope"},
        )
        assert result is None


class TestRemoveGoal:
    async def test_remove_goal(self):
        """remove_goal deletes the matching goal and returns True."""
        goal = make_goal_state(title="Doomed")
        data = json.dumps([goal.model_dump(mode="json")])

        store, redis = _make_store()
        redis.get = AsyncMock(return_value=data)

        stored_value = None

        async def capture_setex(key, ttl, value):
            nonlocal stored_value
            stored_value = value

        redis.setex = capture_setex

        removed = await store.remove_goal(SESSION_KEY, SESSION_ID, goal.id)
        assert removed is True
        parsed = json.loads(stored_value)
        assert len(parsed) == 0

    async def test_remove_goal_not_found(self):
        """remove_goal returns False when the goal_id is not present."""
        store, redis = _make_store()
        redis.get = AsyncMock(return_value="[]")
        removed = await store.remove_goal(SESSION_KEY, SESSION_ID, uuid.uuid4())
        assert removed is False


# ---------------------------------------------------------------------------
# Key pattern
# ---------------------------------------------------------------------------

class TestKeyPattern:
    def test_goals_use_session_key_key_pattern(self):
        """Redis key follows eb:session_goals:{session_key} pattern (session_id excluded)."""
        store, _ = _make_store()
        sid = uuid.UUID("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee")
        key = store._key("agent:main:main", sid)
        assert key == "eb:session_goals:agent:main:main"


# ---------------------------------------------------------------------------
# Flush to Cognee
# ---------------------------------------------------------------------------

class TestFlushToCognee:
    async def test_flush_empty_returns_zero(self):
        """flush_to_cognee returns 0 when no goals exist."""
        store, redis = _make_store()
        redis.get = AsyncMock(return_value=None)
        count = await store.flush_to_cognee(SESSION_KEY, SESSION_ID)
        assert count == 0

    @patch("elephantbroker.runtime.working_set.session_goals.add_data_points", new_callable=AsyncMock)
    @patch("elephantbroker.runtime.working_set.session_goals.cognee")
    async def test_flush_stores_in_cognee(self, mock_cognee, mock_add_dp):
        """flush_to_cognee calls add_data_points and cognee.add for each goal."""
        mock_cognee.add = AsyncMock()
        goal = make_goal_state(title="Ship MVP", description="Release v1")
        data = json.dumps([goal.model_dump(mode="json")])

        store, redis = _make_store()
        redis.get = AsyncMock(return_value=data)

        count = await store.flush_to_cognee(SESSION_KEY, SESSION_ID)
        assert count == 1
        mock_add_dp.assert_called_once()
        mock_cognee.add.assert_called_once()

        # Verify cognee.add text includes title and description
        text = mock_cognee.add.call_args[0][0]
        assert "Ship MVP" in text
        assert "Release v1" in text

    @patch("elephantbroker.runtime.working_set.session_goals.add_data_points", new_callable=AsyncMock)
    @patch("elephantbroker.runtime.working_set.session_goals.cognee")
    async def test_flush_creates_child_of_edges(self, mock_cognee, mock_add_dp):
        """flush_to_cognee creates CHILD_OF edge when parent_goal_id is set."""
        mock_cognee.add = AsyncMock()
        parent_id = uuid.uuid4()
        child = make_goal_state(title="Sub-task", parent_goal_id=parent_id)
        data = json.dumps([child.model_dump(mode="json")])

        graph = AsyncMock()
        store, redis = _make_store(graph=graph)
        redis.get = AsyncMock(return_value=data)

        await store.flush_to_cognee(SESSION_KEY, SESSION_ID)
        graph.add_relation.assert_any_call(
            str(child.id), str(parent_id), "CHILD_OF",
        )

    @patch("elephantbroker.runtime.working_set.session_goals.add_data_points", new_callable=AsyncMock)
    @patch("elephantbroker.runtime.working_set.session_goals.cognee")
    async def test_flush_creates_owns_goal_edges(self, mock_cognee, mock_add_dp):
        """flush_to_cognee creates OWNS_GOAL edges for each owner_actor_id."""
        mock_cognee.add = AsyncMock()
        owner1 = uuid.uuid4()
        owner2 = uuid.uuid4()
        goal = make_goal_state(title="Shared goal", owner_actor_ids=[owner1, owner2])
        data = json.dumps([goal.model_dump(mode="json")])

        graph = AsyncMock()
        store, redis = _make_store(graph=graph)
        redis.get = AsyncMock(return_value=data)

        await store.flush_to_cognee(SESSION_KEY, SESSION_ID)

        # Should have 2 OWNS_GOAL calls
        owns_calls = [
            c for c in graph.add_relation.call_args_list
            if c[0][2] == "OWNS_GOAL"
        ]
        assert len(owns_calls) == 2
        owner_ids_called = {c[0][0] for c in owns_calls}
        assert owner_ids_called == {str(owner1), str(owner2)}

    @patch("elephantbroker.runtime.working_set.session_goals.add_data_points", new_callable=AsyncMock)
    @patch("elephantbroker.runtime.working_set.session_goals.cognee")
    async def test_flush_cleans_redis(self, mock_cognee, mock_add_dp):
        """flush_to_cognee deletes the Redis key after flushing."""
        mock_cognee.add = AsyncMock()
        goal = make_goal_state()
        data = json.dumps([goal.model_dump(mode="json")])

        store, redis = _make_store()
        redis.get = AsyncMock(return_value=data)

        await store.flush_to_cognee(SESSION_KEY, SESSION_ID)
        expected_key = store._key(SESSION_KEY, SESSION_ID)
        redis.delete.assert_called_once_with(expected_key)

    @patch("elephantbroker.runtime.working_set.session_goals.add_data_points", new_callable=AsyncMock)
    @patch("elephantbroker.runtime.working_set.session_goals.cognee")
    async def test_flush_emits_trace_event(self, mock_cognee, mock_add_dp):
        """flush_to_cognee emits a SESSION_BOUNDARY trace event."""
        mock_cognee.add = AsyncMock()
        goal = make_goal_state()
        data = json.dumps([goal.model_dump(mode="json")])

        trace = AsyncMock()
        store, redis = _make_store(trace_ledger=trace)
        redis.get = AsyncMock(return_value=data)

        await store.flush_to_cognee(SESSION_KEY, SESSION_ID)

        trace.append_event.assert_called_once()
        event = trace.append_event.call_args[0][0]
        assert event.event_type == TraceEventType.SESSION_BOUNDARY
        assert event.payload["action"] == "goals_flushed"
        assert event.payload["goals_flushed"] == 1
        assert event.payload["session_key"] == SESSION_KEY

    @patch("elephantbroker.runtime.working_set.session_goals.add_data_points", new_callable=AsyncMock)
    @patch("elephantbroker.runtime.working_set.session_goals.cognee")
    async def test_flush_includes_success_criteria_in_text(self, mock_cognee, mock_add_dp):
        """cognee.add text includes success criteria when present."""
        mock_cognee.add = AsyncMock()
        goal = make_goal_state(
            title="Deploy",
            success_criteria=["tests pass", "no regressions"],
        )
        data = json.dumps([goal.model_dump(mode="json")])

        store, redis = _make_store()
        redis.get = AsyncMock(return_value=data)

        await store.flush_to_cognee(SESSION_KEY, SESSION_ID)
        text = mock_cognee.add.call_args[0][0]
        assert "tests pass" in text
        assert "no regressions" in text


# ---------------------------------------------------------------------------
# Immutable fields on update (#506)
# ---------------------------------------------------------------------------

class TestImmutableFields:
    async def test_immutable_fields_skipped_on_update(self):
        """id and created_at in updates dict are silently ignored."""
        goal = make_goal_state(title="Original")
        original_id = goal.id
        original_created_at = goal.created_at
        data = json.dumps([goal.model_dump(mode="json")])

        store, redis = _make_store()
        redis.get = AsyncMock(return_value=data)
        redis.setex = AsyncMock()

        new_id = uuid.uuid4()
        updated = await store.update_goal(
            SESSION_KEY, SESSION_ID, goal.id,
            {"id": new_id, "created_at": "2000-01-01T00:00:00Z", "title": "Changed"},
        )
        assert updated is not None
        assert updated.title == "Changed"
        assert updated.id == original_id  # id unchanged
        assert updated.created_at == original_created_at  # created_at unchanged


# ---------------------------------------------------------------------------
# Auto-goal completion blocked (#507)
# ---------------------------------------------------------------------------

class TestAutoGoalCompletionBlocked:
    async def test_auto_goal_completion_blocked(self):
        """Agent cannot complete auto-goals — ValueError raised."""
        goal = make_goal_state(
            title="Auto task", status=GoalStatus.ACTIVE,
            metadata={"source_type": "auto"},
        )
        data = json.dumps([goal.model_dump(mode="json")])

        store, redis = _make_store()
        redis.get = AsyncMock(return_value=data)

        with pytest.raises(ValueError, match="managed by the runtime"):
            await store.update_goal(
                SESSION_KEY, SESSION_ID, goal.id,
                {"status": GoalStatus.COMPLETED},
            )

    async def test_auto_goal_abandonment_blocked(self):
        """Agent cannot abandon auto-goals — ValueError raised."""
        goal = make_goal_state(
            title="Auto task", status=GoalStatus.ACTIVE,
            metadata={"source_type": "auto"},
        )
        data = json.dumps([goal.model_dump(mode="json")])

        store, redis = _make_store()
        redis.get = AsyncMock(return_value=data)

        with pytest.raises(ValueError, match="managed by the runtime"):
            await store.update_goal(
                SESSION_KEY, SESSION_ID, goal.id,
                {"status": GoalStatus.ABANDONED},
            )


# ---------------------------------------------------------------------------
# Flush continues on single goal failure (#509)
# ---------------------------------------------------------------------------

class TestFlushPartialFailure:
    @patch("elephantbroker.runtime.working_set.session_goals.cognee")
    async def test_flush_continues_on_single_goal_failure(self, mock_cognee):
        """If add_data_points raises on first goal, second goal still flushes."""
        mock_cognee.add = AsyncMock()
        g1 = make_goal_state(title="Fail goal")
        g2 = make_goal_state(title="Success goal")
        data = json.dumps([
            g1.model_dump(mode="json"),
            g2.model_dump(mode="json"),
        ])

        store, redis = _make_store()
        redis.get = AsyncMock(return_value=data)

        call_count = {"n": 0}

        async def _add_dp_side_effect(dps):
            call_count["n"] += 1
            if call_count["n"] == 1:
                raise RuntimeError("storage failure on first goal")

        with patch(
            "elephantbroker.runtime.working_set.session_goals.add_data_points",
            new_callable=AsyncMock, side_effect=_add_dp_side_effect,
        ):
            count = await store.flush_to_cognee(SESSION_KEY, SESSION_ID)

        # Only second goal succeeded
        assert count == 1
        # cognee.add should have been called once (for the successful goal only)
        assert mock_cognee.add.call_count == 1
        text = mock_cognee.add.call_args[0][0]
        assert "Success goal" in text


# ---------------------------------------------------------------------------
# Flush annotation priority (#511)
# ---------------------------------------------------------------------------

class TestFlushAnnotationPriority:
    @patch("elephantbroker.runtime.working_set.session_goals.add_data_points", new_callable=AsyncMock)
    @patch("elephantbroker.runtime.working_set.session_goals.cognee")
    async def test_flush_annotation_priority(self, mock_cognee, mock_add_dp):
        """Flush text annotations follow status-based priority ordering."""
        mock_cognee.add = AsyncMock()
        completed = make_goal_state(
            title="Done", status=GoalStatus.COMPLETED,
            success_criteria=["shipped"],
        )
        abandoned = make_goal_state(title="Dropped", status=GoalStatus.ABANDONED)
        blocked = make_goal_state(
            title="Stuck", status=GoalStatus.ACTIVE,
            blockers=["infra down"],
        )
        data = json.dumps([
            completed.model_dump(mode="json"),
            abandoned.model_dump(mode="json"),
            blocked.model_dump(mode="json"),
        ])

        store, redis = _make_store()
        redis.get = AsyncMock(return_value=data)

        await store.flush_to_cognee(SESSION_KEY, SESSION_ID)

        texts = [call[0][0] for call in mock_cognee.add.call_args_list]
        completed_text = [t for t in texts if "Done" in t][0]
        abandoned_text = [t for t in texts if "Dropped" in t][0]
        blocked_text = [t for t in texts if "Stuck" in t][0]

        assert "[COMPLETED: shipped]" in completed_text
        assert "[ABANDONED]" in abandoned_text
        assert "[BLOCKED: infra down]" in blocked_text


# ---------------------------------------------------------------------------
# Active goal with criteria annotation (#1216)
# ---------------------------------------------------------------------------

class TestActiveGoalCriteriaAnnotation:
    @patch("elephantbroker.runtime.working_set.session_goals.add_data_points", new_callable=AsyncMock)
    @patch("elephantbroker.runtime.working_set.session_goals.cognee")
    async def test_active_goal_with_criteria_annotation(self, mock_cognee, mock_add_dp):
        """ACTIVE goal with success_criteria gets 'criteria:' annotation (fourth branch)."""
        mock_cognee.add = AsyncMock()
        goal = make_goal_state(
            title="In progress", status=GoalStatus.ACTIVE,
            success_criteria=["all tests green", "docs updated"],
            blockers=[],  # no blockers — so the elif criteria branch fires
        )
        data = json.dumps([goal.model_dump(mode="json")])

        store, redis = _make_store()
        redis.get = AsyncMock(return_value=data)

        await store.flush_to_cognee(SESSION_KEY, SESSION_ID)
        text = mock_cognee.add.call_args[0][0]
        assert "criteria:" in text
        assert "all tests green" in text
        assert "docs updated" in text


# ---------------------------------------------------------------------------
# Gateway ID stamped on DataPoint (#512)
# ---------------------------------------------------------------------------

class TestGatewayIdStamped:
    @patch("elephantbroker.runtime.working_set.session_goals.add_data_points", new_callable=AsyncMock)
    @patch("elephantbroker.runtime.working_set.session_goals.cognee")
    async def test_gateway_id_stamped_before_datapoint(self, mock_cognee, mock_add_dp):
        """flush_to_cognee stamps gateway_id on the GoalDataPoint before storage."""
        mock_cognee.add = AsyncMock()
        goal = make_goal_state(title="Gated goal")
        data = json.dumps([goal.model_dump(mode="json")])

        store, redis = _make_store()
        store._gateway_id = "gw-test-42"
        redis.get = AsyncMock(return_value=data)

        await store.flush_to_cognee(SESSION_KEY, SESSION_ID)

        mock_add_dp.assert_called_once()
        dp = mock_add_dp.call_args[0][0][0]  # first arg, first item in list
        assert dp.gateway_id == "gw-test-42"
