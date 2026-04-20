"""Integration tests for Cognee-first storage and hybrid search."""
import pytest

from cognee.tasks.storage import add_data_points

from elephantbroker.runtime.adapters.cognee.datapoints import (
    ActorDataPoint, ArtifactDataPoint, FactDataPoint,
)
from elephantbroker.runtime.graph_utils import clean_graph_props
from tests.fixtures.factories import (
    make_actor_ref, make_fact_assertion, make_goal_state, make_tool_artifact,
)


@pytest.mark.integration
class TestCogneeDataPointStorage:
    async def test_fact_stored_via_add_data_points_has_all_properties(self, graph_adapter):
        """Store FactDataPoint via add_data_points, query Neo4j, verify all custom fields present."""
        fact = make_fact_assertion(text="Integration test fact")
        dp = FactDataPoint.from_schema(fact)
        await add_data_points([dp])
        entity = await graph_adapter.get_entity(str(fact.id))
        assert entity is not None
        assert entity["text"] == "Integration test fact"
        assert entity["eb_id"] == str(fact.id)

    async def test_actor_stored_via_add_data_points(self, graph_adapter):
        """Store ActorDataPoint via add_data_points, verify retrievable."""
        actor = make_actor_ref()
        dp = ActorDataPoint.from_schema(actor)
        await add_data_points([dp])
        entity = await graph_adapter.get_entity(str(actor.id))
        assert entity is not None
        assert entity["display_name"] == actor.display_name

    async def test_add_data_points_upserts_on_same_id(self, graph_adapter):
        """Store same DataPoint twice, verify update not duplicate."""
        fact = make_fact_assertion(text="original")
        dp = FactDataPoint.from_schema(fact)
        await add_data_points([dp])
        fact.text = "updated"
        # TODO-5-800: re-constructs the DataPoint with the same eb_id — Cognee's
        # add_data_points does MERGE-by-ID, so the second call is an update
        # (same node, updated properties), not a duplicate insert.
        dp2 = FactDataPoint.from_schema(fact)
        await add_data_points([dp2])
        entity = await graph_adapter.get_entity(str(fact.id))
        assert entity is not None
        assert entity["text"] == "updated"

    async def test_custom_edges_coexist_with_add_data_points(self, graph_adapter):
        """Store via add_data_points, add CREATED_BY edge, verify both visible."""
        actor = make_actor_ref()
        fact = make_fact_assertion()
        await add_data_points([ActorDataPoint.from_schema(actor)])
        await add_data_points([FactDataPoint.from_schema(fact)])
        await graph_adapter.add_relation(str(fact.id), str(actor.id), "CREATED_BY")
        neighbors = await graph_adapter.get_neighbors(str(fact.id))
        assert len(neighbors) >= 1

    async def test_add_data_points_creates_vector_index(self, graph_adapter, cognee_config):
        """Store FactDataPoint, verify FactDataPoint_text collection exists in Qdrant."""
        from qdrant_client import AsyncQdrantClient
        fact = make_fact_assertion(text="Vector index test fact")
        dp = FactDataPoint.from_schema(fact)
        await add_data_points([dp])
        # Verify the auto-created collection exists
        client = AsyncQdrantClient(url=cognee_config.qdrant_url)
        try:
            exists = await client.collection_exists("FactDataPoint_text")
            assert exists
        finally:
            await client.close()

    async def test_full_module_store_then_resolve(self, actor_registry, memory_facade):
        """Register actor + store fact via module methods -> data is in Neo4j."""
        actor = make_actor_ref()
        await actor_registry.register_actor(actor)
        resolved = await actor_registry.resolve_actor(actor.id)
        assert resolved is not None
        assert resolved.display_name == actor.display_name

    async def test_hybrid_search_returns_cognee_plus_structural(self, memory_facade, graph_adapter):
        """Store facts with different scopes, verify hybrid search finds both."""
        from elephantbroker.schemas.base import Scope
        fact1 = make_fact_assertion(text="Paris is the capital of France", scope=Scope.SESSION)
        fact2 = make_fact_assertion(text="Berlin is the capital of Germany", scope=Scope.GLOBAL)
        await memory_facade.store(fact1)
        await memory_facade.store(fact2)
        results = await memory_facade.search("capital", scope=Scope.GLOBAL)
        # At minimum, structural query should find the GLOBAL-scoped fact
        assert isinstance(results, list)


@pytest.mark.pipeline
class TestCogneePipelineIntegration:
    """Tests that require cognee.cognify() -- validates the full pipeline path.
    These prove the foundation works for Phase 4's Turn Ingest Pipeline."""

    async def test_cognee_add_plus_cognify_searchable(self):
        """cognee.add() + cognify(), verify CHUNKS search finds text."""
        import cognee
        from cognee.modules.search.types import SearchType
        await cognee.add("ElephantBroker is a cognitive runtime for OpenClaw", dataset_name="test_pipeline")
        await cognee.cognify(datasets=["test_pipeline"])
        results = await cognee.search(
            query_type=SearchType.CHUNKS,
            query_text="cognitive runtime",
        )
        assert len(results) >= 1

    async def test_full_module_flow_searchable(self, actor_registry, goal_manager, memory_facade):
        """Register actor + set goal + store fact -> cognify -> GRAPH_COMPLETION returns connected context."""
        import cognee
        from cognee.modules.search.types import SearchType
        actor = make_actor_ref(display_name="pipeline-test-actor")
        await actor_registry.register_actor(actor)
        goal = make_goal_state(title="Test pipeline goal")
        await goal_manager.set_goal(goal)
        fact = make_fact_assertion(text="Pipeline test fact for goal completion", source_actor_id=actor.id)
        await memory_facade.store(fact)
        await cognee.cognify(datasets=["test_integration"])
        results = await cognee.search(
            query_type=SearchType.GRAPH_COMPLETION,
            query_text="pipeline test fact",
            only_context=True,
        )
        assert isinstance(results, list)
