"""Tests for ApprovalQueue (Phase 7 — §7.19)."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from elephantbroker.runtime.guards.approval_queue import ApprovalQueue, _action_hash
from elephantbroker.runtime.redis_keys import RedisKeyBuilder
from elephantbroker.schemas.config import HitlConfig
from elephantbroker.schemas.goal import GoalStatus
from elephantbroker.schemas.guards import ApprovalRequest, ApprovalStatus, AutonomyLevel


def _make_queue():
    redis = AsyncMock()
    redis.setex = AsyncMock()
    redis.get = AsyncMock(return_value=None)
    redis.sadd = AsyncMock()
    redis.expire = AsyncMock()
    redis.smembers = AsyncMock(return_value=set())
    redis.ttl = AsyncMock(return_value=300)
    keys = RedisKeyBuilder("test")
    config = HitlConfig(approval_default_timeout_seconds=300)
    return ApprovalQueue(redis, keys, config), redis


def _make_request(**overrides) -> ApprovalRequest:
    defaults = {
        "guard_event_id": uuid.uuid4(),
        "session_id": uuid.uuid4(),
        "action_summary": "test action",
        "decision_domain": "general",
    }
    return ApprovalRequest(**(defaults | overrides))


class TestApprovalQueue:
    @pytest.mark.asyncio
    async def test_create_stores_in_redis(self):
        queue, redis = _make_queue()
        req = _make_request()
        result = await queue.create(req, "agent1")
        redis.setex.assert_called_once()

    @pytest.mark.asyncio
    async def test_create_sets_timeout_at(self):
        queue, _ = _make_queue()
        req = _make_request()
        result = await queue.create(req, "agent1")
        assert result.timeout_at is not None
        assert result.timeout_at > result.created_at

    @pytest.mark.asyncio
    async def test_create_adds_to_session_index(self):
        queue, redis = _make_queue()
        req = _make_request()
        await queue.create(req, "agent1")
        redis.sadd.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_returns_request(self):
        queue, redis = _make_queue()
        req = _make_request()
        redis.get = AsyncMock(return_value=req.model_dump_json())
        result = await queue.get(req.id, "agent1")
        assert result is not None
        assert result.id == req.id

    @pytest.mark.asyncio
    async def test_get_nonexistent_returns_none(self):
        queue, redis = _make_queue()
        redis.get = AsyncMock(return_value=None)
        result = await queue.get(uuid.uuid4(), "agent1")
        assert result is None

    @pytest.mark.asyncio
    async def test_approve_updates_status(self):
        queue, redis = _make_queue()
        req = _make_request()
        redis.get = AsyncMock(return_value=req.model_dump_json())
        result = await queue.approve(req.id, "agent1", message="looks good")
        assert result is not None
        assert result.status == ApprovalStatus.APPROVED
        assert result.approval_message == "looks good"

    @pytest.mark.asyncio
    async def test_reject_updates_status(self):
        queue, redis = _make_queue()
        req = _make_request()
        redis.get = AsyncMock(return_value=req.model_dump_json())
        result = await queue.reject(req.id, "agent1", reason="too risky")
        assert result is not None
        assert result.status == ApprovalStatus.REJECTED
        assert result.rejection_reason == "too risky"

    @pytest.mark.asyncio
    async def test_check_timeout_marks_timed_out(self):
        queue, redis = _make_queue()
        req = _make_request()
        req.timeout_at = datetime.now(UTC) - timedelta(seconds=10)  # Already expired
        redis.get = AsyncMock(return_value=req.model_dump_json())
        result = await queue.check_timeout(req.id, "agent1")
        assert result is not None
        assert result.status == ApprovalStatus.TIMED_OUT

    @pytest.mark.asyncio
    async def test_check_timeout_not_expired(self):
        queue, redis = _make_queue()
        req = _make_request()
        req.timeout_at = datetime.now(UTC) + timedelta(seconds=300)
        redis.get = AsyncMock(return_value=req.model_dump_json())
        result = await queue.check_timeout(req.id, "agent1")
        assert result is not None
        assert result.status == ApprovalStatus.PENDING

    @pytest.mark.asyncio
    async def test_find_matching_same_action(self):
        queue, redis = _make_queue()
        req = _make_request(action_summary="deploy to prod")
        req.timeout_at = datetime.now(UTC) + timedelta(seconds=300)
        redis.smembers = AsyncMock(return_value={str(req.id)})
        redis.get = AsyncMock(return_value=req.model_dump_json())
        result = await queue.find_matching(req.session_id, "deploy to prod", "agent1")
        assert result is not None

    @pytest.mark.asyncio
    async def test_find_matching_different_action(self):
        queue, redis = _make_queue()
        req = _make_request(action_summary="deploy to prod")
        redis.smembers = AsyncMock(return_value={str(req.id)})
        redis.get = AsyncMock(return_value=req.model_dump_json())
        result = await queue.find_matching(req.session_id, "something completely different", "agent1")
        assert result is None

    @pytest.mark.asyncio
    async def test_create_auto_goal_created(self):
        queue, redis = _make_queue()
        goal_store = AsyncMock()
        goal_store.add_goal = AsyncMock()
        req = _make_request()
        sid = req.session_id
        await queue.create(req, "agent1", session_goal_store=goal_store,
                          session_key="agent:main:main", session_id=sid)
        goal_store.add_goal.assert_called_once()

    # --- Amendment 7.2 additional tests ---

    @pytest.mark.asyncio
    async def test_create_timeout_at_calculation_correctness(self):
        """timeout_at = created_at + approval_default_timeout_seconds."""
        queue, _ = _make_queue()
        req = _make_request()
        result = await queue.create(req, "agent1")
        expected_timeout = result.created_at + timedelta(seconds=300)
        assert result.timeout_at == expected_timeout

    @pytest.mark.asyncio
    async def test_create_ttl_is_timeout_plus_60(self):
        """Redis TTL = approval_default_timeout_seconds + 60."""
        queue, redis = _make_queue()
        req = _make_request()
        await queue.create(req, "agent1")
        # setex called with TTL = 300 + 60 = 360
        call_args = redis.setex.call_args
        assert call_args[0][1] == 360

    @pytest.mark.asyncio
    async def test_approve_nonexistent_returns_none(self):
        queue, redis = _make_queue()
        redis.get = AsyncMock(return_value=None)
        result = await queue.approve(uuid.uuid4(), "agent1", message="ok")
        assert result is None

    @pytest.mark.asyncio
    async def test_approve_sets_resolved_at(self):
        queue, redis = _make_queue()
        req = _make_request()
        redis.get = AsyncMock(return_value=req.model_dump_json())
        before = datetime.now(UTC)
        result = await queue.approve(req.id, "agent1", message="approved")
        assert result is not None
        assert result.resolved_at is not None
        assert result.resolved_at >= before

    @pytest.mark.asyncio
    async def test_reject_nonexistent_returns_none(self):
        queue, redis = _make_queue()
        redis.get = AsyncMock(return_value=None)
        result = await queue.reject(uuid.uuid4(), "agent1", reason="no")
        assert result is None

    @pytest.mark.asyncio
    async def test_reject_sets_resolved_at(self):
        queue, redis = _make_queue()
        req = _make_request()
        redis.get = AsyncMock(return_value=req.model_dump_json())
        before = datetime.now(UTC)
        result = await queue.reject(req.id, "agent1", reason="dangerous")
        assert result is not None
        assert result.resolved_at is not None
        assert result.resolved_at >= before

    @pytest.mark.asyncio
    async def test_check_timeout_autonomous_auto_approves(self):
        """AUTONOMOUS timeout_action → auto-approve (silence = consent)."""
        queue, redis = _make_queue()
        req = _make_request()
        req.timeout_at = datetime.now(UTC) - timedelta(seconds=10)
        redis.get = AsyncMock(return_value=req.model_dump_json())
        result = await queue.check_timeout(
            req.id, "agent1", timeout_action=AutonomyLevel.AUTONOMOUS,
        )
        assert result is not None
        assert result.status == ApprovalStatus.APPROVED
        assert "auto-approved" in (result.approval_message or "").lower()

    @pytest.mark.asyncio
    async def test_check_timeout_non_pending_ignored(self):
        """Already-resolved requests are not re-processed by check_timeout."""
        queue, redis = _make_queue()
        req = _make_request()
        req.status = ApprovalStatus.APPROVED
        req.timeout_at = datetime.now(UTC) - timedelta(seconds=10)
        redis.get = AsyncMock(return_value=req.model_dump_json())
        result = await queue.check_timeout(req.id, "agent1")
        assert result is not None
        assert result.status == ApprovalStatus.APPROVED  # Unchanged

    @pytest.mark.asyncio
    async def test_find_matching_no_requests_returns_none(self):
        queue, redis = _make_queue()
        redis.smembers = AsyncMock(return_value=set())
        result = await queue.find_matching(uuid.uuid4(), "some action", "agent1")
        assert result is None

    @pytest.mark.asyncio
    async def test_cancel_sets_cancelled_status(self):
        queue, redis = _make_queue()
        req = _make_request()
        redis.get = AsyncMock(return_value=req.model_dump_json())
        result = await queue.cancel(req.id, "agent1", reason="cancelled by user")
        assert result is not None
        assert result.status == ApprovalStatus.CANCELLED

    @pytest.mark.asyncio
    async def test_cancel_resolves_auto_goal_as_abandoned(self):
        queue, redis = _make_queue()
        req = _make_request()
        redis.get = AsyncMock(return_value=req.model_dump_json())
        goal_store = AsyncMock()
        goal_store.get_goals = AsyncMock(return_value=[])
        result = await queue.cancel(
            req.id, "agent1", reason="session ended",
            session_goal_store=goal_store, session_key="agent:main:main",
        )
        assert result is not None
        assert result.status == ApprovalStatus.CANCELLED
        # resolve_approval_goal was called (get_goals invoked)
        goal_store.get_goals.assert_called_once()

    @pytest.mark.asyncio
    async def test_resolve_goal_finds_and_updates(self):
        """resolve_approval_goal finds the matching auto-goal and updates its status."""
        queue, redis = _make_queue()
        req = _make_request()
        # Create a mock goal that matches the approval request
        goal_id = uuid.uuid4()
        mock_goal = AsyncMock()
        mock_goal.id = goal_id
        mock_goal.metadata = {
            "source_type": "auto",
            "source_system": "guard_approval",
            "approval_request_id": str(req.id),
        }
        mock_goal.status = GoalStatus.ACTIVE
        mock_goal.blockers = []
        goal_store = AsyncMock()
        goal_store.get_goals = AsyncMock(return_value=[mock_goal])
        goal_store.update_goal = AsyncMock()
        await queue.resolve_approval_goal(req, goal_store, "agent:main:main", GoalStatus.COMPLETED)
        goal_store.update_goal.assert_called_once()
        call_args = goal_store.update_goal.call_args
        assert call_args[0][2] == goal_id  # goal_id passed correctly
        updates = call_args[0][3]
        assert updates["status"] == GoalStatus.COMPLETED
        assert updates["metadata"]["resolved_by_runtime"] == "true"

    @pytest.mark.asyncio
    async def test_resolve_goal_no_matching_goal(self):
        """resolve_approval_goal does nothing when no matching goal exists."""
        queue, redis = _make_queue()
        req = _make_request()
        mock_goal = AsyncMock()
        mock_goal.metadata = {"source_type": "manual"}
        goal_store = AsyncMock()
        goal_store.get_goals = AsyncMock(return_value=[mock_goal])
        goal_store.update_goal = AsyncMock()
        await queue.resolve_approval_goal(req, goal_store, "agent:main:main", GoalStatus.COMPLETED)
        goal_store.update_goal.assert_not_called()

    @pytest.mark.asyncio
    async def test_resolve_goal_store_error_handled(self):
        """resolve_approval_goal handles store errors gracefully."""
        queue, redis = _make_queue()
        req = _make_request()
        goal_store = AsyncMock()
        goal_store.get_goals = AsyncMock(side_effect=Exception("DB unavailable"))
        # Should not raise
        await queue.resolve_approval_goal(req, goal_store, "agent:main:main", GoalStatus.COMPLETED)

    def test_action_hash_deterministic(self):
        """Same input always produces same hash."""
        h1 = _action_hash("deploy to production")
        h2 = _action_hash("deploy to production")
        assert h1 == h2

    def test_action_hash_case_normalization(self):
        """Action hash normalizes case."""
        h1 = _action_hash("Deploy To Production")
        h2 = _action_hash("deploy to production")
        assert h1 == h2

    @pytest.mark.asyncio
    async def test_approve_resolves_auto_goal_as_completed(self):
        """Amendment 7.2 C2: approve resolves auto-goal as COMPLETED."""
        queue, redis = _make_queue()
        req = _make_request()
        redis.get = AsyncMock(return_value=req.model_dump_json())
        goal_id = uuid.uuid4()
        mock_goal = AsyncMock()
        mock_goal.id = goal_id
        mock_goal.metadata = {
            "source_type": "auto",
            "source_system": "guard_approval",
            "approval_request_id": str(req.id),
        }
        mock_goal.status = GoalStatus.ACTIVE
        mock_goal.blockers = []
        goal_store = AsyncMock()
        goal_store.get_goals = AsyncMock(return_value=[mock_goal])
        goal_store.update_goal = AsyncMock()
        result = await queue.approve(
            req.id, "agent1", message="ok",
            session_goal_store=goal_store, session_key="agent:main:main",
        )
        assert result is not None
        assert result.status == ApprovalStatus.APPROVED
        goal_store.update_goal.assert_called_once()
        updates = goal_store.update_goal.call_args[0][3]
        assert updates["status"] == GoalStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_reject_resolves_auto_goal_as_abandoned(self):
        """Amendment 7.2 C2: reject resolves auto-goal as ABANDONED."""
        queue, redis = _make_queue()
        req = _make_request()
        redis.get = AsyncMock(return_value=req.model_dump_json())
        goal_id = uuid.uuid4()
        mock_goal = AsyncMock()
        mock_goal.id = goal_id
        mock_goal.metadata = {
            "source_type": "auto",
            "source_system": "guard_approval",
            "approval_request_id": str(req.id),
        }
        mock_goal.status = GoalStatus.ACTIVE
        mock_goal.blockers = []
        goal_store = AsyncMock()
        goal_store.get_goals = AsyncMock(return_value=[mock_goal])
        goal_store.update_goal = AsyncMock()
        result = await queue.reject(
            req.id, "agent1", reason="too risky",
            session_goal_store=goal_store, session_key="agent:main:main",
        )
        assert result is not None
        assert result.status == ApprovalStatus.REJECTED
        goal_store.update_goal.assert_called_once()
        updates = goal_store.update_goal.call_args[0][3]
        assert updates["status"] == GoalStatus.ABANDONED
        assert "Rejected: too risky" in updates["blockers"]

    @pytest.mark.asyncio
    async def test_create_without_session_goal_store_skips_auto_goal(self):
        """EC-1: create() with session_goal_store=None creates approval but skips auto-goal."""
        queue, redis = _make_queue()
        req = _make_request()
        result = await queue.create(
            req, "agent1",
            session_goal_store=None,
            session_key="agent:main:main",
            session_id=req.session_id,
        )
        # Approval request created successfully
        assert result.id == req.id
        redis.setex.assert_called()
        redis.sadd.assert_called()
        # No auto-goal creation attempted (no goal store calls to verify against)
