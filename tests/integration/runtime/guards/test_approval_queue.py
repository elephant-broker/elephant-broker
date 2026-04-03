"""Integration tests for ApprovalQueue with mocked Redis (8 tests).

Tests create/get/approve/reject/timeout flows using an ApprovalQueue
backed by an AsyncMock Redis client.
"""
from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime, timedelta

import pytest
from unittest.mock import AsyncMock, MagicMock, call

from elephantbroker.runtime.guards.approval_queue import ApprovalQueue
from elephantbroker.runtime.redis_keys import RedisKeyBuilder
from elephantbroker.schemas.config import HitlConfig
from elephantbroker.schemas.guards import (
    ApprovalRequest,
    ApprovalStatus,
    AutonomyLevel,
)

GATEWAY_ID = "test-gw"
AGENT_ID = "agent-1"


def _fresh_redis() -> AsyncMock:
    r = AsyncMock()
    r.lpush = AsyncMock()
    r.ltrim = AsyncMock()
    r.expire = AsyncMock()
    r.lrange = AsyncMock(return_value=[])
    r.get = AsyncMock(return_value=None)
    r.set = AsyncMock()
    r.setex = AsyncMock()
    r.delete = AsyncMock()
    r.smembers = AsyncMock(return_value=set())
    r.sadd = AsyncMock()
    r.ttl = AsyncMock(return_value=300)
    return r


def _make_queue(redis=None) -> tuple[ApprovalQueue, AsyncMock]:
    r = redis or _fresh_redis()
    keys = RedisKeyBuilder(GATEWAY_ID)
    config = HitlConfig(approval_default_timeout_seconds=300)
    queue = ApprovalQueue(redis=r, redis_keys=keys, config=config)
    return queue, r


@pytest.mark.asyncio
async def test_create_stores():
    """create() calls setex to store the serialized request in Redis."""
    queue, redis = _make_queue()
    req = ApprovalRequest(
        session_id=uuid.uuid4(),
        action_summary="deploy to prod",
        decision_domain="code_change",
    )
    result = await queue.create(req, AGENT_ID)

    assert result.id == req.id
    # setex should be called with the approval key, ttl, and JSON
    redis.setex.assert_called()
    call_args = redis.setex.call_args_list[0]
    key_arg = call_args[0][0]
    assert "approval" in key_arg
    json_arg = call_args[0][2]
    parsed = json.loads(json_arg)
    assert parsed["action_summary"] == "deploy to prod"


@pytest.mark.asyncio
async def test_get_returns():
    """get() returns deserialized request when Redis has the data."""
    queue, redis = _make_queue()
    req = ApprovalRequest(
        session_id=uuid.uuid4(),
        action_summary="run tests",
    )
    # Simulate Redis returning the serialized request
    redis.get = AsyncMock(return_value=req.model_dump_json())

    result = await queue.get(req.id, AGENT_ID)
    assert result is not None
    assert result.id == req.id
    assert result.action_summary == "run tests"


@pytest.mark.asyncio
async def test_get_for_session():
    """get_for_session() returns all requests for a given session."""
    queue, redis = _make_queue()

    sid = uuid.uuid4()
    req1 = ApprovalRequest(session_id=sid, action_summary="task 1")
    req2 = ApprovalRequest(session_id=sid, action_summary="task 2")

    # smembers returns the request IDs
    redis.smembers = AsyncMock(return_value={str(req1.id), str(req2.id)})
    # get returns the request for each ID
    redis.get = AsyncMock(side_effect=lambda key: (
        req1.model_dump_json() if str(req1.id) in key
        else req2.model_dump_json() if str(req2.id) in key
        else None
    ))

    results = await queue.get_for_session(sid, AGENT_ID)
    assert len(results) == 2
    summaries = {r.action_summary for r in results}
    assert summaries == {"task 1", "task 2"}


@pytest.mark.asyncio
async def test_approve_updates():
    """approve() transitions status to APPROVED and stores updated JSON."""
    queue, redis = _make_queue()
    req = ApprovalRequest(
        session_id=uuid.uuid4(),
        action_summary="deploy staging",
        status=ApprovalStatus.PENDING,
    )

    # Mock get to return the pending request
    redis.get = AsyncMock(return_value=req.model_dump_json())
    redis.ttl = AsyncMock(return_value=250)

    result = await queue.approve(req.id, AGENT_ID, message="approved by lead")

    assert result is not None
    assert result.status == ApprovalStatus.APPROVED
    assert result.approval_message == "approved by lead"
    # Verify the updated JSON was stored back
    assert redis.setex.call_count >= 1
    last_call = redis.setex.call_args_list[-1]
    stored_json = json.loads(last_call[0][2])
    assert stored_json["status"] == "approved"


@pytest.mark.asyncio
async def test_reject_stores_reason():
    """reject() transitions to REJECTED and stores the rejection reason."""
    queue, redis = _make_queue()
    req = ApprovalRequest(
        session_id=uuid.uuid4(),
        action_summary="delete database",
        status=ApprovalStatus.PENDING,
    )

    redis.get = AsyncMock(return_value=req.model_dump_json())
    redis.ttl = AsyncMock(return_value=200)

    result = await queue.reject(req.id, AGENT_ID, reason="too dangerous")

    assert result is not None
    assert result.status == ApprovalStatus.REJECTED
    assert result.rejection_reason == "too dangerous"
    last_call = redis.setex.call_args_list[-1]
    stored_json = json.loads(last_call[0][2])
    assert stored_json["rejection_reason"] == "too dangerous"


@pytest.mark.asyncio
async def test_timeout_hard_stop():
    """check_timeout with HARD_STOP -> TIMED_OUT status."""
    queue, redis = _make_queue()
    req = ApprovalRequest(
        session_id=uuid.uuid4(),
        action_summary="scale cluster",
        status=ApprovalStatus.PENDING,
        # Set timeout_at in the past
        timeout_at=datetime.now(UTC) - timedelta(seconds=60),
    )

    redis.get = AsyncMock(return_value=req.model_dump_json())

    result = await queue.check_timeout(
        req.id, AGENT_ID,
        timeout_action=AutonomyLevel.HARD_STOP,
    )

    assert result is not None
    assert result.status == ApprovalStatus.TIMED_OUT


@pytest.mark.asyncio
async def test_timeout_autonomous():
    """check_timeout with AUTONOMOUS -> APPROVED status (silence = consent)."""
    queue, redis = _make_queue()
    req = ApprovalRequest(
        session_id=uuid.uuid4(),
        action_summary="log analysis",
        status=ApprovalStatus.PENDING,
        timeout_at=datetime.now(UTC) - timedelta(seconds=60),
    )

    redis.get = AsyncMock(return_value=req.model_dump_json())

    result = await queue.check_timeout(
        req.id, AGENT_ID,
        timeout_action=AutonomyLevel.AUTONOMOUS,
    )

    assert result is not None
    assert result.status == ApprovalStatus.APPROVED
    assert "auto-approved" in (result.approval_message or "").lower()


@pytest.mark.asyncio
async def test_find_matching_dedup():
    """find_matching returns existing request for the same action summary."""
    queue, redis = _make_queue()

    sid = uuid.uuid4()
    existing = ApprovalRequest(
        session_id=sid,
        action_summary="deploy to staging",
        status=ApprovalStatus.PENDING,
        # Timeout in the future so it stays PENDING
        timeout_at=datetime.now(UTC) + timedelta(seconds=600),
    )

    # smembers returns the existing request ID
    redis.smembers = AsyncMock(return_value={str(existing.id)})
    # get returns the existing request
    redis.get = AsyncMock(return_value=existing.model_dump_json())

    result = await queue.find_matching(sid, "deploy to staging", AGENT_ID)

    assert result is not None
    assert result.id == existing.id
    assert result.status == ApprovalStatus.PENDING
