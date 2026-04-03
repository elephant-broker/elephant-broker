"""Unit tests for GraphAdapter with mocked Neo4j driver."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from elephantbroker.runtime.adapters.cognee.graph import GraphAdapter
from elephantbroker.schemas.config import CogneeConfig


def _make_adapter() -> GraphAdapter:
    return GraphAdapter(CogneeConfig())


def _mock_driver_with_result(records):
    """Create a mock driver that returns the given records from session.run().

    Neo4j's driver.session() returns a sync context-manager-like object
    that supports ``async with``.
    """
    mock_result = AsyncMock()
    mock_result.single = AsyncMock(return_value=records[0] if records else None)
    mock_result.data = AsyncMock(return_value=records)

    mock_session = AsyncMock()
    mock_session.run = AsyncMock(return_value=mock_result)

    # driver.session() is a sync call returning an async context manager
    ctx = MagicMock()
    ctx.__aenter__ = AsyncMock(return_value=mock_session)
    ctx.__aexit__ = AsyncMock(return_value=False)

    mock_driver = MagicMock()
    mock_driver.session.return_value = ctx
    mock_driver.close = AsyncMock()

    return mock_driver, mock_session


class TestGraphAdapter:
    async def test_add_relation_creates_relationship(self):
        adapter = _make_adapter()
        driver, session = _mock_driver_with_result([])
        adapter._driver = driver

        await adapter.add_relation("a", "b", "DELEGATES_TO", {"weight": 1.0})
        cypher = session.run.call_args[0][0]
        assert "DELEGATES_TO" in cypher
        assert "MERGE" in cypher

    async def test_get_entity_returns_props(self):
        adapter = _make_adapter()
        record = {"props": {"eb_id": "x", "text": "hi"}, "labels": ["FactDataPoint"]}
        driver, session = _mock_driver_with_result([record])
        adapter._driver = driver

        result = await adapter.get_entity("x")
        assert result is not None
        assert result["text"] == "hi"
        assert result["_labels"] == ["FactDataPoint"]

    async def test_get_entity_returns_none_when_missing(self):
        adapter = _make_adapter()
        driver, session = _mock_driver_with_result([])
        adapter._driver = driver

        result = await adapter.get_entity("nonexistent")
        assert result is None

    async def test_get_neighbors_uses_depth(self):
        adapter = _make_adapter()
        driver, session = _mock_driver_with_result([])
        adapter._driver = driver

        await adapter.get_neighbors("start", depth=3)
        cypher = session.run.call_args[0][0]
        assert "*1..3" in cypher

    async def test_delete_entity_uses_detach_delete(self):
        adapter = _make_adapter()
        driver, session = _mock_driver_with_result([])
        adapter._driver = driver

        await adapter.delete_entity("del-me")
        cypher = session.run.call_args[0][0]
        assert "DETACH DELETE" in cypher

    async def test_query_cypher_returns_records(self):
        adapter = _make_adapter()
        records = [{"count": 42}]
        driver, session = _mock_driver_with_result(records)
        adapter._driver = driver

        results = await adapter.query_cypher("RETURN 42 AS count")
        assert len(results) == 1
        assert results[0]["count"] == 42

    async def test_close_shuts_down_driver(self):
        adapter = _make_adapter()
        mock_driver = MagicMock()
        mock_driver.close = AsyncMock()
        adapter._driver = mock_driver
        await adapter.close()
        mock_driver.close.assert_awaited_once()
        assert adapter._driver is None

    async def test_query_subgraph_returns_nodes_and_edges(self):
        adapter = _make_adapter()
        record = {
            "nodes": [{"id": "n1", "type": "Fact", "properties": {"text": "hi"}}],
            "edges": [{"source": "n1", "target": "n2", "relation_type": "RELATED", "properties": {}}],
        }
        driver, session = _mock_driver_with_result([record])
        adapter._driver = driver

        result = await adapter.query_subgraph("n1")
        assert len(result.nodes) == 1
        assert len(result.edges) == 1

    async def test_query_subgraph_with_relation_types(self):
        adapter = _make_adapter()
        driver, session = _mock_driver_with_result([])
        adapter._driver = driver

        await adapter.query_subgraph("start", relation_types=["SUPERVISES"])
        cypher = session.run.call_args[0][0]
        assert ":SUPERVISES" in cypher

    async def test_query_subgraph_without_relation_types(self):
        adapter = _make_adapter()
        driver, session = _mock_driver_with_result([])
        adapter._driver = driver

        await adapter.query_subgraph("start", relation_types=None)
        cypher = session.run.call_args[0][0]
        assert "[r*1.." in cypher

    async def test_query_subgraph_empty_result(self):
        adapter = _make_adapter()
        driver, session = _mock_driver_with_result([])
        adapter._driver = driver

        result = await adapter.query_subgraph("nonexistent")
        assert result.nodes == []
        assert result.edges == []
