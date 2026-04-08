"""Tests for actor routes."""
import uuid
from unittest.mock import AsyncMock

from elephantbroker.schemas.actor import ActorRef


class TestActorRoutes:
    async def test_create_actor(self, client):
        body = {"type": "worker_agent", "display_name": "test-bot"}
        r = await client.post("/actors/", json=body)
        assert r.status_code == 200

    async def test_get_actor_not_found(self, client):
        r = await client.get(f"/actors/{uuid.uuid4()}")
        assert r.status_code == 404

    async def test_get_relationships(self, client):
        r = await client.get(f"/actors/{uuid.uuid4()}/relationships")
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    async def test_get_authority_chain(self, client):
        r = await client.get(f"/actors/{uuid.uuid4()}/authority-chain")
        assert r.status_code == 200
        assert isinstance(r.json(), list)

    async def test_create_actor_missing_body_422(self, client):
        r = await client.post("/actors/", json={})
        assert r.status_code == 422

    async def test_create_actor_invalid_type_422(self, client):
        r = await client.post("/actors/", json={"type": "invalid", "display_name": "x"})
        assert r.status_code == 422

    async def test_create_actor_when_registry_disabled(self, client, container):
        container.actor_registry = None
        body = {"type": "worker_agent", "display_name": "test"}
        r = await client.post("/actors/", json=body)
        assert r.status_code == 500


class TestActorGatewayIsolation:
    """Gateway-ID enforcement tests for actor routes."""

    async def test_create_actor_stamps_gateway_id(self, client, container):
        """POST /actors/ stamps actor.gateway_id from the X-EB-Gateway-ID header."""
        captured_actors: list[ActorRef] = []

        async def capture_register(actor):
            captured_actors.append(actor)
            return actor

        container.actor_registry.register_actor = AsyncMock(side_effect=capture_register)

        body = {"type": "worker_agent", "display_name": "tenant-bot"}
        r = await client.post(
            "/actors/",
            json=body,
            headers={"X-EB-Gateway-ID": "tenant-33"},
        )
        assert r.status_code == 200
        assert len(captured_actors) == 1
        assert captured_actors[0].gateway_id == "tenant-33"

    async def test_create_actor_default_gateway(self, client, container):
        """Without X-EB-Gateway-ID header, middleware falls back to the container's
        configured gateway_id. Post-Bucket-A the default is "" (empty string) — the
        app factory wires container.config.gateway.gateway_id through to the
        middleware so write and read paths stay byte-identical."""
        captured_actors: list[ActorRef] = []

        async def capture_register(actor):
            captured_actors.append(actor)
            return actor

        container.actor_registry.register_actor = AsyncMock(side_effect=capture_register)

        body = {"type": "worker_agent", "display_name": "local-bot"}
        r = await client.post("/actors/", json=body)
        assert r.status_code == 200
        assert len(captured_actors) == 1
        assert captured_actors[0].gateway_id == ""
