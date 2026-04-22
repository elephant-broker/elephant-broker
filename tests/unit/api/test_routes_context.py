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

    async def test_get_config_returns_profile_resolved_ingest_batch_size_when_profile_param_provided(
        self, client, container,
    ):
        """P6: GET /context/config?profile=X returns the profile-level
        ingest_batch_size override; omitting the param returns the global value.

        Exercises ProfileRegistry.effective_ingest_batch_size via the real
        registry on the container (not mocked), with a monkey-patched
        resolve_profile so the test doesn't depend on preset contents.
        """
        from unittest.mock import AsyncMock

        from elephantbroker.schemas.config import ElephantBrokerConfig
        from elephantbroker.schemas.profile import ProfilePolicy

        container.config = ElephantBrokerConfig()  # global ingest_batch_size = 6
        override_policy = ProfilePolicy(id="coding", name="Coding", ingest_batch_size=4)
        container.profile_registry.resolve_profile = AsyncMock(return_value=override_policy)

        # With ?profile=coding — profile override wins.
        r = await client.get("/context/config?profile=coding")
        assert r.status_code == 200
        assert r.json()["ingest_batch_size"] == 4

        # Without profile param — global LLMConfig default.
        r2 = await client.get("/context/config")
        assert r2.status_code == 200
        assert r2.json()["ingest_batch_size"] == 6

    async def test_get_config_unknown_profile_returns_404(
        self, client, container,
    ):
        """TODO-6-702: ProfileRegistry.resolve_profile raises KeyError on
        unknown profile names; this must surface as HTTP 404 with a
        diagnosable ``detail`` so operator typos don't silently fall back
        to matching-default values."""
        from unittest.mock import AsyncMock

        from elephantbroker.schemas.config import ElephantBrokerConfig

        container.config = ElephantBrokerConfig()
        container.profile_registry.resolve_profile = AsyncMock(
            side_effect=KeyError("Unknown profile: codnig"),
        )

        r = await client.get("/context/config?profile=codnig")
        assert r.status_code == 404
        assert r.json()["detail"] == "Unknown profile: codnig"

    async def test_get_config_transient_exception_warns_and_falls_back(
        self, client, container, caplog,
    ):
        """TODO-6-202 / TODO-6-304: non-KeyError resolver exceptions (transient
        registry/DB faults) must (a) NOT 500 — endpoint stays up with global
        fallback, (b) emit a WARNING so the silent fallback is observable in
        logs. KeyError-specific branch is covered by the 404 test above."""
        import logging
        from unittest.mock import AsyncMock

        from elephantbroker.schemas.config import ElephantBrokerConfig

        container.config = ElephantBrokerConfig()
        container.profile_registry.resolve_profile = AsyncMock(
            side_effect=RuntimeError("transient-db-hiccup"),
        )

        with caplog.at_level(logging.WARNING, logger="elephantbroker.api.routes.context"):
            r = await client.get("/context/config?profile=coding")

        assert r.status_code == 200
        assert r.json()["ingest_batch_size"] == 6  # global default

        warning_records = [
            rec for rec in caplog.records
            if rec.levelno == logging.WARNING
            and rec.name == "elephantbroker.api.routes.context"
            and "profile resolution failed" in rec.getMessage()
        ]
        assert len(warning_records) == 1, (
            f"expected exactly one WARNING on transient-fallback branch, got {len(warning_records)}"
        )
        msg = warning_records[0].getMessage()
        assert "'coding'" in msg
        assert "transient-db-hiccup" in msg

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
        fallback, which the app factory wires to container.config.gateway.gateway_id.
        Post-Bucket-A the default is "" (empty string) — write and read paths stay
        byte-identical because both resolve through the same config value."""
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
        assert captured_params[0].gateway_id == ""
