"""Tests for context routes."""
import uuid
from unittest.mock import AsyncMock

from elephantbroker.schemas.context import (
    AssembleResult,
    BootstrapResult,
    CompactResult,
    IngestBatchResult,
    IngestResult,
    SubagentSpawnResult,
    SystemPromptOverlay,
)


class TestContextRoutes:
    async def test_bootstrap(self, client):
        body = {"session_key": "agent:main:main", "session_id": str(uuid.uuid4()), "profile_name": "coding"}
        r = await client.post("/context/bootstrap", json=body)
        assert r.status_code == 200
        assert r.json()["bootstrapped"] is True

    async def test_ingest(self, client):
        body = {
            "session_id": str(uuid.uuid4()),
            "session_key": "agent:main:main",
            "message": {"role": "user", "content": "hello"},
        }
        r = await client.post("/context/ingest", json=body)
        assert r.status_code == 200

    async def test_ingest_batch(self, client):
        body = {
            "session_id": str(uuid.uuid4()),
            "session_key": "agent:main:main",
            "messages": [{"role": "user", "content": "hello"}],
        }
        r = await client.post("/context/ingest-batch", json=body)
        assert r.status_code == 200

    async def test_ingest_batch_response_includes_facts_stored(self, client, container):
        """POST /context/ingest-batch response must include facts_stored field."""
        from elephantbroker.schemas.context import IngestBatchResult
        container.context_lifecycle.ingest_batch = AsyncMock(
            return_value=IngestBatchResult(ingested_count=2, facts_stored=1),
        )
        body = {
            "session_id": str(uuid.uuid4()),
            "session_key": "agent:main:main",
            "messages": [{"role": "user", "content": "hello"}, {"role": "assistant", "content": "hi"}],
        }
        r = await client.post("/context/ingest-batch", json=body)
        assert r.status_code == 200
        data = r.json()
        assert data["facts_stored"] == 1
        assert data["ingested_count"] == 2

    async def test_assemble(self, client):
        body = {
            "session_id": str(uuid.uuid4()),
            "session_key": "agent:main:main",
            "messages": [{"role": "user", "content": "hello"}],
            "token_budget": 8000,
        }
        r = await client.post("/context/assemble", json=body)
        assert r.status_code == 200

    async def test_bootstrap_missing_session_key_422(self, client):
        r = await client.post("/context/bootstrap", json={"session_id": str(uuid.uuid4())})
        assert r.status_code == 422

    async def test_ingest_missing_message_422(self, client):
        r = await client.post("/context/ingest", json={})
        assert r.status_code == 422

    async def test_bootstrap_with_unknown_profile(self, client):
        body = {"session_key": "agent:main:main", "session_id": str(uuid.uuid4()), "profile_name": "nonexistent"}
        r = await client.post("/context/bootstrap", json=body)
        assert r.status_code == 200

    async def test_compact(self, client):
        body = {"session_key": "agent:main:main", "session_id": str(uuid.uuid4())}
        r = await client.post("/context/compact", json=body)
        assert r.status_code == 200

    async def test_after_turn(self, client):
        body = {"session_key": "agent:main:main", "session_id": str(uuid.uuid4())}
        r = await client.post("/context/after-turn", json=body)
        assert r.status_code == 200

    async def test_subagent_spawn(self, client):
        body = {"parent_session_key": "parent", "child_session_key": "child"}
        r = await client.post("/context/subagent/spawn", json=body)
        assert r.status_code == 200

    async def test_subagent_ended(self, client):
        body = {"child_session_key": "child"}
        r = await client.post("/context/subagent/ended", json=body)
        assert r.status_code == 200

    async def test_build_overlay(self, client):
        body = {"session_key": "sk", "session_id": "sid"}
        r = await client.post("/context/build-overlay", json=body)
        assert r.status_code == 200

    async def test_dispose(self, client):
        body = {"session_key": "sk", "session_id": "sid"}
        r = await client.post("/context/dispose", json=body)
        assert r.status_code == 200

    async def test_get_config(self, client):
        r = await client.get("/context/config")
        assert r.status_code == 200

    async def test_config_returns_ingest_batch_size(self, client, container):
        """GET /context/config includes ingest_batch_size and ingest_batch_timeout_ms from LLMConfig."""
        from elephantbroker.schemas.config import ElephantBrokerConfig
        container.config = ElephantBrokerConfig()
        r = await client.get("/context/config")
        assert r.status_code == 200
        data = r.json()
        assert data["ingest_batch_size"] == 6
        assert data["ingest_batch_timeout_ms"] == 60000

    async def test_subagent_rollback(self, client):
        body = {"parent_session_key": "p", "child_session_key": "c", "rollback_key": "k"}
        r = await client.post("/context/subagent/rollback", json=body)
        assert r.status_code == 200


class TestContextGatewayIsolation:
    """Gateway-ID enforcement tests for context routes."""

    async def test_bootstrap_stamps_gateway_id(self, client, container):
        """POST /context/bootstrap stamps gateway_id from the X-EB-Gateway-ID header
        onto the BootstrapParams before passing to the lifecycle."""
        from elephantbroker.schemas.context import BootstrapParams, BootstrapResult

        captured_params: list[BootstrapParams] = []

        async def capture_bootstrap(params):
            captured_params.append(params)
            return BootstrapResult(bootstrapped=True)

        container.context_lifecycle.bootstrap = AsyncMock(side_effect=capture_bootstrap)

        body = {
            "session_key": "agent:main:main",
            "session_id": str(uuid.uuid4()),
            "profile_name": "coding",
        }
        r = await client.post(
            "/context/bootstrap",
            json=body,
            headers={"X-EB-Gateway-ID": "tenant-55"},
        )
        assert r.status_code == 200
        assert len(captured_params) == 1
        assert captured_params[0].gateway_id == "tenant-55"

    async def test_bootstrap_default_gateway(self, client, container):
        """Without X-EB-Gateway-ID header, _stamp_gateway uses the middleware
        default ('local')."""
        from elephantbroker.schemas.context import BootstrapParams, BootstrapResult

        captured_params: list[BootstrapParams] = []

        async def capture_bootstrap(params):
            captured_params.append(params)
            return BootstrapResult(bootstrapped=True)

        container.context_lifecycle.bootstrap = AsyncMock(side_effect=capture_bootstrap)

        body = {
            "session_key": "agent:main:main",
            "session_id": str(uuid.uuid4()),
            "profile_name": "coding",
        }
        r = await client.post("/context/bootstrap", json=body)
        assert r.status_code == 200
        assert len(captured_params) == 1
        assert captured_params[0].gateway_id == "local"
