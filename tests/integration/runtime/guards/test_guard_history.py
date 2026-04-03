"""Integration tests for guard event history in Redis (5 tests).

Verifies that preflight_check stores guard events via lpush, caps them with
ltrim, sets TTL via expire, and isolates events per session.
"""
from __future__ import annotations

import asyncio
import json
import uuid

import pytest
from unittest.mock import AsyncMock, call

from elephantbroker.schemas.context import AgentMessage
from elephantbroker.schemas.guards import GuardOutcome, GuardEvent


def _sid() -> uuid.UUID:
    return uuid.uuid4()


def _msg(content: str, role: str = "assistant", **meta) -> AgentMessage:
    return AgentMessage(role=role, content=content, metadata=meta)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_history_stored(guard_engine, mock_redis):
    """After preflight_check, a guard event is pushed to Redis via lpush."""
    sid = _sid()
    await guard_engine.load_session_rules(sid, "coding", session_key="agent:main:main")

    messages = [_msg("hello world")]
    await guard_engine.preflight_check(sid, messages)

    # Give the fire-and-forget asyncio.create_task a chance to run
    await asyncio.sleep(0.05)

    mock_redis.lpush.assert_called()
    # Find the guard_history lpush call
    lpush_calls = mock_redis.lpush.call_args_list
    guard_history_calls = [c for c in lpush_calls if "guard_history" in str(c)]
    assert len(guard_history_calls) >= 1

    # Verify the stored data is valid JSON / GuardEvent
    json_data = guard_history_calls[0][0][1]
    event = GuardEvent.model_validate_json(json_data)
    assert event.session_id == sid


@pytest.mark.asyncio
async def test_history_capped(guard_engine, mock_redis):
    """After storing, ltrim is called to cap the list at 50 entries."""
    sid = _sid()
    await guard_engine.load_session_rules(sid, "coding", session_key="agent:main:main")

    messages = [_msg("test message")]
    await guard_engine.preflight_check(sid, messages)
    await asyncio.sleep(0.05)

    mock_redis.ltrim.assert_called()
    ltrim_calls = mock_redis.ltrim.call_args_list
    guard_ltrim_calls = [c for c in ltrim_calls if "guard_history" in str(c)]
    assert len(guard_ltrim_calls) >= 1
    # Check it trims to 0..49
    _, end = guard_ltrim_calls[0][0][1], guard_ltrim_calls[0][0][2]
    assert end == 49


@pytest.mark.asyncio
async def test_history_ttl(guard_engine, mock_redis):
    """After storing, expire is called on the guard_history key."""
    sid = _sid()
    await guard_engine.load_session_rules(sid, "coding", session_key="agent:main:main")

    messages = [_msg("test message")]
    await guard_engine.preflight_check(sid, messages)
    await asyncio.sleep(0.05)

    expire_calls = mock_redis.expire.call_args_list
    guard_expire_calls = [c for c in expire_calls if "guard_history" in str(c)]
    assert len(guard_expire_calls) >= 1


@pytest.mark.asyncio
async def test_near_miss_count(guard_engine, mock_redis):
    """get_guard_history returns events from Redis lrange."""
    sid = _sid()
    await guard_engine.load_session_rules(sid, "coding", session_key="agent:main:main")

    # Create 3 WARN events to simulate near-misses
    warn_events = []
    for i in range(3):
        ev = GuardEvent(
            session_id=sid,
            outcome=GuardOutcome.WARN,
            matched_rules=[f"rule_{i}"],
            explanation=f"Near miss {i}",
        )
        warn_events.append(ev.model_dump_json())

    mock_redis.lrange = AsyncMock(return_value=warn_events)

    history = await guard_engine.get_guard_history(sid)
    assert len(history) == 3
    warn_count = sum(1 for e in history if e.outcome == GuardOutcome.WARN)
    assert warn_count == 3


@pytest.mark.asyncio
async def test_session_isolation(guard_engine, mock_redis):
    """Two different session IDs produce separate lpush keys."""
    sid1 = _sid()
    sid2 = _sid()
    await guard_engine.load_session_rules(sid1, "coding", session_key="agent:a:main")
    await guard_engine.load_session_rules(sid2, "coding", session_key="agent:b:main")

    messages = [_msg("hello")]
    await guard_engine.preflight_check(sid1, messages)
    await guard_engine.preflight_check(sid2, messages)
    await asyncio.sleep(0.05)

    lpush_calls = mock_redis.lpush.call_args_list
    guard_keys = [str(c[0][0]) for c in lpush_calls if "guard_history" in str(c)]

    # Both sessions should have distinct keys
    unique_keys = set(guard_keys)
    assert len(unique_keys) >= 2, f"Expected 2 distinct guard_history keys, got: {unique_keys}"
