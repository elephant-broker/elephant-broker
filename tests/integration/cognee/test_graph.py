"""Integration tests for GraphAdapter against a live Neo4j instance.

Storage is done via add_data_points() (Cognee-first). GraphAdapter is read-only
for structural queries, custom edges, and deletes.
"""
from __future__ import annotations

from cognee.tasks.storage import add_data_points

from elephantbroker.runtime.adapters.cognee.datapoints import ActorDataPoint, FactDataPoint
from elephantbroker.schemas.actor import ActorRef, ActorType
from elephantbroker.schemas.fact import FactAssertion, FactCategory


class TestGraphAdapterIntegration:
    async def test_add_and_retrieve_entity(self, graph_adapter):
        fact = FactAssertion(text="Neo4j is a graph database", category=FactCategory.SYSTEM)
        dp = FactDataPoint.from_schema(fact)
        await add_data_points([dp])

        retrieved = await graph_adapter.get_entity(dp.eb_id)
        assert retrieved is not None
        assert retrieved["text"] == "Neo4j is a graph database"
        assert retrieved["category"] == "system"

    async def test_add_relation_between_entities(self, graph_adapter):
        actor_a = ActorRef(type=ActorType.WORKER_AGENT, display_name="Agent A")
        actor_b = ActorRef(type=ActorType.WORKER_AGENT, display_name="Agent B")
        dp_a = ActorDataPoint.from_schema(actor_a)
        dp_b = ActorDataPoint.from_schema(actor_b)

        await add_data_points([dp_a])
        await add_data_points([dp_b])

        await graph_adapter.add_relation(dp_a.eb_id, dp_b.eb_id, "DELEGATES_TO", {"weight": 1.0})

        neighbors = await graph_adapter.get_neighbors(dp_a.eb_id)
        assert len(neighbors) == 1
        assert neighbors[0]["display_name"] == "Agent B"

    async def test_get_neighbors_depth_1(self, graph_adapter):
        a = ActorDataPoint.from_schema(ActorRef(type=ActorType.MANAGER_AGENT, display_name="Manager"))
        b = ActorDataPoint.from_schema(ActorRef(type=ActorType.WORKER_AGENT, display_name="Worker"))
        c = ActorDataPoint.from_schema(ActorRef(type=ActorType.REVIEWER_AGENT, display_name="Reviewer"))

        await add_data_points([a])
        await add_data_points([b])
        await add_data_points([c])

        await graph_adapter.add_relation(a.eb_id, b.eb_id, "SUPERVISES")
        await graph_adapter.add_relation(b.eb_id, c.eb_id, "COLLABORATES_WITH")

        # Depth 1 from manager should only find worker
        neighbors = await graph_adapter.get_neighbors(a.eb_id, depth=1)
        names = {n["display_name"] for n in neighbors}
        assert "Worker" in names
        assert "Reviewer" not in names

    async def test_get_neighbors_depth_2(self, graph_adapter):
        a = ActorDataPoint.from_schema(ActorRef(type=ActorType.MANAGER_AGENT, display_name="Manager"))
        b = ActorDataPoint.from_schema(ActorRef(type=ActorType.WORKER_AGENT, display_name="Worker"))
        c = ActorDataPoint.from_schema(ActorRef(type=ActorType.REVIEWER_AGENT, display_name="Reviewer"))

        await add_data_points([a])
        await add_data_points([b])
        await add_data_points([c])

        await graph_adapter.add_relation(a.eb_id, b.eb_id, "SUPERVISES")
        await graph_adapter.add_relation(b.eb_id, c.eb_id, "COLLABORATES_WITH")

        # Depth 2 from manager should find both worker and reviewer
        neighbors = await graph_adapter.get_neighbors(a.eb_id, depth=2)
        names = {n["display_name"] for n in neighbors}
        assert "Worker" in names
        assert "Reviewer" in names

    async def test_query_subgraph_by_relation_type(self, graph_adapter):
        a = ActorDataPoint.from_schema(ActorRef(type=ActorType.MANAGER_AGENT, display_name="Boss"))
        b = ActorDataPoint.from_schema(ActorRef(type=ActorType.WORKER_AGENT, display_name="Dev"))
        c = ActorDataPoint.from_schema(ActorRef(type=ActorType.REVIEWER_AGENT, display_name="QA"))

        await add_data_points([a])
        await add_data_points([b])
        await add_data_points([c])

        await graph_adapter.add_relation(a.eb_id, b.eb_id, "SUPERVISES")
        await graph_adapter.add_relation(a.eb_id, c.eb_id, "DELEGATES_TO")

        sub = await graph_adapter.query_subgraph(a.eb_id, relation_types=["SUPERVISES"])
        edge_types = {e["relation_type"] for e in sub.edges}
        assert "SUPERVISES" in edge_types

    async def test_delete_entity_removes_relations(self, graph_adapter):
        a = ActorDataPoint.from_schema(ActorRef(type=ActorType.WORKER_AGENT, display_name="Temp"))
        b = ActorDataPoint.from_schema(ActorRef(type=ActorType.WORKER_AGENT, display_name="Perm"))

        await add_data_points([a])
        await add_data_points([b])
        await graph_adapter.add_relation(a.eb_id, b.eb_id, "TRUSTS")

        await graph_adapter.delete_entity(a.eb_id)

        assert await graph_adapter.get_entity(a.eb_id) is None
        neighbors = await graph_adapter.get_neighbors(b.eb_id)
        assert len(neighbors) == 0
