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
        # G5: add_relation MATCHes by eb_id on both source and target (not by graph node id)
        assert "eb_id: $source_id" in cypher
        assert "eb_id: $target_id" in cypher

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

    # ------------------------------------------------------------------
    # TF-FN-007 additions
    # ------------------------------------------------------------------

    async def test_get_entity_with_gateway_id_includes_filter(self):
        """G1: get_entity(entity_id, gateway_id=...) injects gateway_id into Cypher + params.

        This is the only GraphAdapter primitive that accepts a gateway_id kwarg; all other
        primitives are intentionally gateway-agnostic (see CLAUDE.md Gateway Identity note
        and G8-G11 PROD-risk pins below). Callers that need gateway filtering on reads
        should prefer this entry point.
        """
        adapter = _make_adapter()
        driver, session = _mock_driver_with_result([])
        adapter._driver = driver

        await adapter.get_entity("x", gateway_id="gw-a")
        cypher = session.run.call_args[0][0]
        params = session.run.call_args.kwargs
        assert "gateway_id" in cypher
        assert params["gateway_id"] == "gw-a"
        assert params["entity_id"] == "x"

    async def test_get_neighbors_filters_by_relation_types(self):
        """G2: get_neighbors relation_types list becomes a pipe-joined Cypher label filter."""
        adapter = _make_adapter()
        driver, session = _mock_driver_with_result([])
        adapter._driver = driver

        await adapter.get_neighbors("x", relation_types=["REPORTS_TO", "DELEGATES_TO"])
        cypher = session.run.call_args[0][0]
        assert ":REPORTS_TO|DELEGATES_TO" in cypher

    async def test_add_relation_sanitizes_lowercase_and_spaces(self):
        """G3: add_relation normalizes label to UPPER_SNAKE via .upper().replace(' ', '_')."""
        adapter = _make_adapter()
        driver, session = _mock_driver_with_result([])
        adapter._driver = driver

        await adapter.add_relation("a", "b", "has child", {})
        cypher = session.run.call_args[0][0]
        assert "HAS_CHILD" in cypher
        assert "has child" not in cypher

    async def test_add_relation_uses_merge_not_create(self):
        """G4: add_relation uses MERGE (idempotent) not CREATE -- repeated calls do not duplicate edges."""
        adapter = _make_adapter()
        driver, session = _mock_driver_with_result([])
        adapter._driver = driver

        await adapter.add_relation("a", "b", "TRUSTS", {})
        cypher = session.run.call_args[0][0]
        assert "MERGE" in cypher
        assert "CREATE" not in cypher

    async def test_delete_relation_uses_directed_delete_cypher(self):
        """G6: delete_relation is directed (source -> target via [r:TYPE]) and deletes only r."""
        adapter = _make_adapter()
        driver, session = _mock_driver_with_result([])
        adapter._driver = driver

        await adapter.delete_relation("a", "b", "TRUSTS")
        cypher = session.run.call_args[0][0]
        assert "-[r:TRUSTS]->" in cypher
        assert "DELETE r" in cypher

    async def test_delete_relation_silent_when_missing(self):
        """G6: delete_relation on a nonexistent edge is a no-op -- does not raise."""
        adapter = _make_adapter()
        driver, session = _mock_driver_with_result([])
        adapter._driver = driver

        # Must not raise even though no matching relationship exists
        await adapter.delete_relation("nonexistent", "also-missing", "TRUSTS")

    async def test_lazy_init_no_driver_until_first_op(self):
        """G7: GraphAdapter constructor does NOT instantiate a Neo4j driver.

        Driver is lazy-initialized on first op via _get_driver(). Enables tests to
        construct adapters without live Neo4j, and defers socket cost until first use.
        """
        adapter = GraphAdapter(CogneeConfig())
        assert adapter._driver is None

    async def test_get_neighbors_has_no_gateway_filter_documented_prod_risk(self):
        """Pins documented PROD risk #1498 (TF-FN-007 step 16).

        If a future change adds gateway filtering to get_neighbors, update this test,
        the TF-FN-007 plan, and file a TD entry.
        """
        adapter = _make_adapter()
        driver, session = _mock_driver_with_result([])
        adapter._driver = driver

        await adapter.get_neighbors("x", depth=1)
        cypher = session.run.call_args[0][0]
        assert "gateway_id" not in cypher

    async def test_query_subgraph_has_no_gateway_filter_documented_prod_risk(self):
        """Pins documented PROD risk #1499 (TF-FN-007 step 17).

        If a future change adds gateway filtering to query_subgraph, update this test,
        the TF-FN-007 plan, and file a TD entry.
        """
        adapter = _make_adapter()
        driver, session = _mock_driver_with_result([])
        adapter._driver = driver

        await adapter.query_subgraph("x")
        cypher = session.run.call_args[0][0]
        assert "gateway_id" not in cypher

    async def test_add_relation_has_no_gateway_filter_documented_prod_risk(self):
        """Pins documented PROD risk #1497 (TF-FN-007 step 18).

        If a future change adds gateway filtering to add_relation, update this test,
        the TF-FN-007 plan, and file a TD entry.
        """
        adapter = _make_adapter()
        driver, session = _mock_driver_with_result([])
        adapter._driver = driver

        await adapter.add_relation("a", "b", "TRUSTS", {})
        cypher = session.run.call_args[0][0]
        assert "gateway_id" not in cypher

    async def test_delete_entity_has_no_gateway_filter_documented_prod_risk(self):
        """Pins documented PROD risk #1158 (TF-FN-007 step 19).

        If a future change adds gateway filtering to delete_entity, update this test,
        the TF-FN-007 plan, and file a TD entry.
        """
        adapter = _make_adapter()
        driver, session = _mock_driver_with_result([])
        adapter._driver = driver

        await adapter.delete_entity("x")
        cypher = session.run.call_args[0][0]
        assert "gateway_id" not in cypher

    async def test_delete_relation_has_no_gateway_filter_documented_prod_risk(self):
        """Pins documented PROD risk #1495 (TF-FN-007 step 19).

        If a future change adds gateway filtering to delete_relation, update this test,
        the TF-FN-007 plan, and file a TD entry.
        """
        adapter = _make_adapter()
        driver, session = _mock_driver_with_result([])
        adapter._driver = driver

        await adapter.delete_relation("a", "b", "TRUSTS")
        cypher = session.run.call_args[0][0]
        assert "gateway_id" not in cypher

    async def test_add_relation_sanitizes_hyphens_post_R2P7_fix(self):
        """G20 FLIPPED (#1165 RESOLVED — R2-P7): ``add_relation`` now
        applies strict charset sanitization (``[^A-Za-z0-9_]`` →
        ``_``) so hyphens are replaced with underscores instead of
        passing through to the Cypher literal.

        Pre-fix: ``relation_type.upper().replace(" ", "_")`` only
        stripped spaces. Cypher 5 rejected the resulting
        ``[r:HAS-CHILD]`` clause at runtime — callers had to know to
        pre-sanitize, which the contract didn't enforce.

        Post-fix: the new ``_sanitize_rel_type()`` helper handles
        hyphens, dots, and any other non-identifier character. The
        legacy ``OWNS_GOAL`` / ``CREATED_BY`` shapes (already
        alphanumeric+underscore) are unchanged — sanitization is
        idempotent on clean inputs.
        """
        adapter = _make_adapter()
        driver, session = _mock_driver_with_result([])
        adapter._driver = driver

        await adapter.add_relation("a", "b", "has-child", {})
        cypher = session.run.call_args[0][0]
        # Post-fix: hyphen → underscore in the relationship-type literal.
        assert "HAS_CHILD" in cypher
        assert "HAS-CHILD" not in cypher
