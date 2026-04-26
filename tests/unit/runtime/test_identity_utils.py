"""Tests for identity_utils — assert_same_gateway_batch (M6) + skip-path coverage (M10)."""
from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from elephantbroker.runtime.identity_utils import (
    assert_same_gateway,
    assert_same_gateway_batch,
)


# ── assert_same_gateway_batch (M6) ──────────────────────────────────


class TestAssertSameGatewayBatch:

    @pytest.mark.asyncio
    async def test_batch_all_match(self):
        graph = AsyncMock()
        graph.query_cypher = AsyncMock(return_value=[])
        await assert_same_gateway_batch(graph, ["a", "b", "c"], "gw-a")
        graph.query_cypher.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_batch_one_violation(self):
        graph = AsyncMock()
        graph.query_cypher = AsyncMock(
            return_value=[{"id": "bad-node", "gw": "gw-b"}],
        )
        with pytest.raises(PermissionError) as exc:
            await assert_same_gateway_batch(graph, ["ok", "bad-node"], "gw-a")
        assert "bad-node" in str(exc.value)
        assert "gw-b" in str(exc.value)
        assert "gw-a" in str(exc.value)

    @pytest.mark.asyncio
    async def test_batch_empty_list(self):
        graph = AsyncMock()
        graph.query_cypher = AsyncMock()
        await assert_same_gateway_batch(graph, [], "gw-a")
        graph.query_cypher.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_batch_expected_gw_empty(self):
        graph = AsyncMock()
        graph.query_cypher = AsyncMock()
        await assert_same_gateway_batch(graph, ["a"], "")
        graph.query_cypher.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_batch_no_graph(self):
        await assert_same_gateway_batch(None, ["a", "b"], "gw-a")


# ── assert_same_gateway skip-path coverage (M10) ────────────────────


class TestAssertSameGatewaySkipPaths:

    @pytest.mark.asyncio
    async def test_graph_none_skips(self):
        await assert_same_gateway(None, "target-1", "gw-a")

    @pytest.mark.asyncio
    async def test_entity_none_skips(self):
        graph = AsyncMock()
        graph.get_entity = AsyncMock(return_value=None)
        await assert_same_gateway(graph, "target-1", "gw-a")

    @pytest.mark.asyncio
    async def test_target_gw_empty_skips(self):
        graph = AsyncMock()
        graph.get_entity = AsyncMock(return_value={"gateway_id": "", "eb_id": "t1"})
        await assert_same_gateway(graph, "t1", "gw-a")

    @pytest.mark.asyncio
    async def test_expected_gw_empty_skips(self):
        graph = AsyncMock()
        graph.get_entity = AsyncMock(return_value={"gateway_id": "gw-b", "eb_id": "t1"})
        await assert_same_gateway(graph, "t1", "")

    @pytest.mark.asyncio
    async def test_matching_gateways_passes(self):
        graph = AsyncMock()
        graph.get_entity = AsyncMock(return_value={"gateway_id": "gw-a", "eb_id": "t1"})
        await assert_same_gateway(graph, "t1", "gw-a")
