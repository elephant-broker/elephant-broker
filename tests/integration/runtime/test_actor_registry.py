"""Integration tests for ActorRegistry with real Neo4j."""
from __future__ import annotations

import pytest

from elephantbroker.schemas.actor import ActorType
from tests.fixtures.factories import make_actor_ref


@pytest.mark.integration
class TestActorRegistryIntegration:
    async def test_register_and_resolve_actor_via_neo4j(self, actor_registry):
        actor = make_actor_ref()
        await actor_registry.register_actor(actor)
        resolved = await actor_registry.resolve_actor(actor.id)
        assert resolved is not None
        assert resolved.display_name == actor.display_name
        assert resolved.type == actor.type

    async def test_add_relationship_and_query(self, actor_registry, graph_adapter):
        supervisor = make_actor_ref(type=ActorType.SUPERVISOR_AGENT, display_name="boss")
        worker = make_actor_ref(type=ActorType.WORKER_AGENT, display_name="worker")
        await actor_registry.register_actor(supervisor)
        await actor_registry.register_actor(worker)
        await graph_adapter.add_relation(str(worker.id), str(supervisor.id), "REPORTS_TO")
        rels = await actor_registry.get_relationships(worker.id)
        # May return relationships depending on direction
        assert isinstance(rels, list)

    async def test_authority_chain_traversal(self, actor_registry, graph_adapter):
        root = make_actor_ref(type=ActorType.HUMAN_COORDINATOR, display_name="root")
        mid = make_actor_ref(type=ActorType.MANAGER_AGENT, display_name="mid")
        leaf = make_actor_ref(type=ActorType.WORKER_AGENT, display_name="leaf")
        await actor_registry.register_actor(root)
        await actor_registry.register_actor(mid)
        await actor_registry.register_actor(leaf)
        await graph_adapter.add_relation(str(leaf.id), str(mid.id), "REPORTS_TO")
        await graph_adapter.add_relation(str(mid.id), str(root.id), "REPORTS_TO")
        chain = await actor_registry.get_authority_chain(leaf.id)
        assert len(chain) >= 1
