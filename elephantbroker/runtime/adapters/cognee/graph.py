"""Neo4j graph adapter — wraps the Neo4j async driver directly."""
from __future__ import annotations

import logging  # [DIAG-50-D]
from typing import Any

from neo4j import AsyncDriver, AsyncGraphDatabase
from pydantic import BaseModel

from elephantbroker.schemas.config import CogneeConfig

logger = logging.getLogger("elephantbroker.graph")  # [DIAG-50-D]


class SubgraphResult(BaseModel):
    """Result of a subgraph query."""
    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []


class GraphAdapter:
    """Read-only graph adapter for structural queries and custom edges.

    ALL DataPoint storage goes through ``add_data_points()`` (from cognee.tasks.storage).
    This adapter handles:
    - Structural reads: get_entity(), query_cypher(), get_neighbors(), query_subgraph()
    - Custom typed edges: add_relation() for CHILD_OF, SUPPORTS, CREATED_BY, etc.
    - Deletes: delete_entity() for GDPR compliance (Cognee lacks node-level delete)

    DO NOT add write methods here. Use add_data_points() for all DataPoint storage.
    """

    def __init__(self, config: CogneeConfig) -> None:
        self._uri = config.neo4j_uri
        self._auth = (config.neo4j_user, config.neo4j_password)
        self._driver: AsyncDriver | None = None

    async def _get_driver(self) -> AsyncDriver:
        if self._driver is None:
            self._driver = AsyncGraphDatabase.driver(self._uri, auth=self._auth)
        return self._driver

    async def add_relation(
        self,
        source_id: str,
        target_id: str,
        relation_type: str,
        properties: dict[str, Any] | None = None,
    ) -> None:
        """Create a relationship between two nodes identified by ``eb_id``."""
        driver = await self._get_driver()
        props = properties or {}
        # Sanitize relation_type for Cypher label
        safe_type = relation_type.upper().replace(" ", "_")

        cypher = (
            f"MATCH (a {{eb_id: $source_id}}), (b {{eb_id: $target_id}}) "
            f"MERGE (a)-[r:{safe_type}]->(b) "
            f"SET r += $props"
        )

        async with driver.session() as session:
            await session.run(cypher, source_id=source_id, target_id=target_id, props=props)

    async def get_entity(self, entity_id: str, *, gateway_id: str | None = None) -> dict[str, Any] | None:
        """Retrieve a node by ``eb_id``. Returns properties dict or None.

        When ``gateway_id`` is provided, the query includes a gateway_id filter
        per CLAUDE.md GW-ID rules. When None, queries without gateway scoping
        (backward compat for callers that don't need filtering).
        """
        driver = await self._get_driver()
        if gateway_id is not None:
            cypher = "MATCH (n {eb_id: $entity_id, gateway_id: $gateway_id}) RETURN properties(n) AS props, labels(n) AS labels"
            params = {"entity_id": entity_id, "gateway_id": gateway_id}
        else:
            cypher = "MATCH (n {eb_id: $entity_id}) RETURN properties(n) AS props, labels(n) AS labels"
            params = {"entity_id": entity_id}

        async with driver.session() as session:
            result = await session.run(cypher, **params)
            record = await result.single()
            if record is None:
                return None
            props = dict(record["props"])
            props["_labels"] = list(record["labels"])
            return props

    async def get_neighbors(
        self,
        entity_id: str,
        depth: int = 1,
        relation_types: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Return neighbor nodes within ``depth`` hops."""
        driver = await self._get_driver()

        if relation_types:
            rel_filter = "|".join(rt.upper().replace(" ", "_") for rt in relation_types)
            rel_pattern = f"[:{rel_filter}*1..{depth}]"
        else:
            rel_pattern = f"[*1..{depth}]"

        cypher = (
            f"MATCH (start {{eb_id: $entity_id}})-{rel_pattern}-(neighbor) "
            f"WHERE neighbor.eb_id <> $entity_id "
            f"RETURN DISTINCT properties(neighbor) AS props, labels(neighbor) AS labels"
        )

        async with driver.session() as session:
            result = await session.run(cypher, entity_id=entity_id)
            records = await result.data("props", "labels")
            neighbors = []
            for rec in records:
                props = dict(rec["props"])
                props["_labels"] = list(rec["labels"])
                neighbors.append(props)
            return neighbors

    async def query_subgraph(
        self,
        start_id: str,
        relation_types: list[str] | None = None,
        max_depth: int = 2,
    ) -> SubgraphResult:
        """Return nodes and edges reachable from ``start_id``."""
        driver = await self._get_driver()

        if relation_types:
            rel_filter = "|".join(rt.upper().replace(" ", "_") for rt in relation_types)
            rel_pattern = f"[r:{rel_filter}*1..{max_depth}]"
        else:
            rel_pattern = f"[r*1..{max_depth}]"

        cypher = (
            f"MATCH path = (start {{eb_id: $start_id}})-{rel_pattern}-(end) "
            f"UNWIND nodes(path) AS n "
            f"WITH COLLECT(DISTINCT n) AS all_nodes, COLLECT(relationships(path)) AS all_rels_nested "
            f"UNWIND all_nodes AS node "
            f"WITH COLLECT(DISTINCT {{id: node.eb_id, type: labels(node)[0], properties: properties(node)}}) AS nodes, "
            f"all_rels_nested "
            f"UNWIND all_rels_nested AS rels "
            f"UNWIND rels AS rel "
            f"WITH nodes, COLLECT(DISTINCT {{"
            f"source: startNode(rel).eb_id, "
            f"target: endNode(rel).eb_id, "
            f"relation_type: type(rel), "
            f"properties: properties(rel)"
            f"}}) AS edges "
            f"RETURN nodes, edges"
        )

        async with driver.session() as session:
            result = await session.run(cypher, start_id=start_id)
            record = await result.single()
            if record is None:
                return SubgraphResult()
            return SubgraphResult(
                nodes=[dict(n) for n in record["nodes"]],
                edges=[dict(e) for e in record["edges"]],
            )

    async def delete_relation(
        self,
        source_id: str,
        target_id: str,
        relation_type: str,
    ) -> None:
        """Delete a specific relationship between two nodes."""
        driver = await self._get_driver()
        safe_type = relation_type.upper().replace(" ", "_")
        cypher = f"MATCH (a {{eb_id: $source_id}})-[r:{safe_type}]->(b {{eb_id: $target_id}}) DELETE r"
        async with driver.session() as session:
            await session.run(cypher, source_id=source_id, target_id=target_id)

    async def delete_entity(self, entity_id: str) -> None:
        """Delete a node and all its relationships (DETACH DELETE)."""
        driver = await self._get_driver()
        cypher = "MATCH (n {eb_id: $entity_id}) DETACH DELETE n"

        async with driver.session() as session:
            result = await session.run(cypher, entity_id=entity_id)
            try:
                summary = await result.consume()
                counters = summary.counters
                logger.info(
                    "[DIAG-50-D] graph_delete_entity id=%s nodes_deleted=%d rels_deleted=%d",
                    entity_id,
                    getattr(counters, "nodes_deleted", -1),
                    getattr(counters, "relationships_deleted", -1),
                )
            except Exception as diag_exc:
                logger.info("[DIAG-50-D] graph_delete_entity id=%s summary_failed err=%r",
                            entity_id, diag_exc)

    async def query_cypher(
        self,
        cypher: str,
        params: dict[str, Any] | None = None,
    ) -> list[dict[str, Any]]:
        """Execute arbitrary Cypher and return result records as dicts."""
        driver = await self._get_driver()

        async with driver.session() as session:
            result = await session.run(cypher, **(params or {}))
            return await result.data()

    async def close(self) -> None:
        """Close the Neo4j driver."""
        if self._driver is not None:
            await self._driver.close()
            self._driver = None
