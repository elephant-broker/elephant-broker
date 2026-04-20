"""Tests for Tier 3: Decision Domain Auto-Discovery."""
from __future__ import annotations

import json
from unittest.mock import AsyncMock, MagicMock

from elephantbroker.runtime.consolidation.stages.domain_discovery import DomainDiscoveryTask
from elephantbroker.runtime.redis_keys import RedisKeyBuilder


def _make_task(guard_events=None, embed_result=None):
    embeddings = AsyncMock()
    if embed_result:
        embeddings.embed_batch = AsyncMock(return_value=embed_result)
    else:
        # Default: unique embeddings per input
        async def default_embed(texts):
            return [[float(i) / max(len(texts), 1)] * 10 for i in range(len(texts))]
        embeddings.embed_batch = default_embed

    redis = AsyncMock()
    keys_list = []
    events_by_key = {}
    if guard_events:
        for i, ev in enumerate(guard_events):
            key = f"eb:gw-1:guard_history:sk{i}:sid{i}"
            keys_list.append(key)
            events_by_key[key] = [json.dumps(ev)]

    async def mock_scan(cursor, match=None, count=100):
        if cursor == 0:
            return (0, keys_list)
        return (0, [])

    async def mock_lrange(key, start, end):
        return events_by_key.get(key, [])

    redis.scan = mock_scan
    redis.lrange = mock_lrange

    redis_keys = RedisKeyBuilder("gw-1")
    return DomainDiscoveryTask(embeddings, redis, redis_keys)


class TestDomainDiscovery:
    async def test_frequency_analysis_finds_candidates(self):
        events = [
            {"action_target": "custom_tool", "decision_domain": "uncategorized"}
            for _ in range(7)
        ]
        task = _make_task(guard_events=events)
        suggestions = await task.run("gw-1")
        assert len(suggestions) >= 1
        assert suggestions[0].action_target == "custom_tool"
        assert suggestions[0].occurrences == 7

    async def test_low_frequency_skipped(self):
        events = [
            {"action_target": "rare_tool", "decision_domain": "uncategorized"}
            for _ in range(2)  # Below threshold of 5
        ]
        task = _make_task(guard_events=events)
        suggestions = await task.run("gw-1")
        assert len(suggestions) == 0

    async def test_non_uncategorized_ignored(self):
        events = [
            {"action_target": "known_tool", "decision_domain": "financial"}
            for _ in range(10)
        ]
        task = _make_task(guard_events=events)
        suggestions = await task.run("gw-1")
        assert len(suggestions) == 0

    async def test_empty_history_returns_empty(self):
        task = _make_task(guard_events=[])
        suggestions = await task.run("gw-1")
        assert suggestions == []

    async def test_scan_pattern_scoped_to_gateway(self):
        # The SCAN pattern should be eb:{gateway_id}:guard_history:*
        events = [{"action_target": "tool_x", "decision_domain": "uncategorized"} for _ in range(6)]
        task = _make_task(guard_events=events)
        suggestions = await task.run("gw-1")
        assert all(s.gateway_id == "gw-1" for s in suggestions)

    async def test_similarity_to_existing_domain(self):
        events = [{"action_target": "payment_processor", "decision_domain": "uncategorized"} for _ in range(8)]
        task = _make_task(guard_events=events)
        suggestions = await task.run("gw-1")
        if suggestions:
            assert 0.0 <= suggestions[0].similarity_to_existing <= 1.0

    async def test_no_redis_returns_empty(self):
        task = DomainDiscoveryTask(AsyncMock(), None, RedisKeyBuilder("gw"))
        suggestions = await task.run("gw")
        assert suggestions == []


class TestGatewayScopedScanPattern:
    """C19b: scan pattern must route through RedisKeyBuilder, not a hardcoded
    ``f"eb:{gateway_id}:guard_history:*"`` literal."""

    async def test_scan_pattern_uses_builder(self):
        captured = {}

        async def capturing_scan(cursor, match=None, count=100):
            captured["match"] = match
            return (0, [])

        redis = AsyncMock()
        redis.scan = capturing_scan
        redis.lrange = AsyncMock(return_value=[])

        embeddings = AsyncMock()
        embeddings.embed_batch = AsyncMock(return_value=[])

        keys = RedisKeyBuilder("gw-alpha")
        task = DomainDiscoveryTask(embeddings, redis, keys)
        await task.run("gw-alpha")

        assert captured["match"] == "eb:gw-alpha:guard_history:*"
        assert captured["match"] == keys.guard_history_scan_pattern()

    async def test_scan_pattern_distinct_per_gateway(self):
        k1 = RedisKeyBuilder("gw-one")
        k2 = RedisKeyBuilder("gw-two")
        assert k1.guard_history_scan_pattern() == "eb:gw-one:guard_history:*"
        assert k2.guard_history_scan_pattern() == "eb:gw-two:guard_history:*"
        assert k1.guard_history_scan_pattern() != k2.guard_history_scan_pattern()
