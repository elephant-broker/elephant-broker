"""Tests for context routes."""
import logging
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

    # ------------------------------------------------------------------
    # TODO-6-407 (Round 1 Architecture Reviewer, LOW): pin the 3-state
    # pre_prompt_message_count wire contract through the FastAPI hop.
    # Python-side branches (plugin/derived/empty) are covered by
    # TestAfterTurnP4 in tests/unit/runtime/context/test_lifecycle.py;
    # what this trio locks is that the HTTP JSON body correctly
    # materializes into AfterTurnParams.pre_prompt_message_count with
    # the None / 0 / N distinction preserved across the hop. A
    # regression that collapsed 0 → None (or N → 0, or drifted the
    # field name) would be caught here without waiting for live-traffic
    # behavior divergence.
    # ------------------------------------------------------------------

    async def test_after_turn_pre_prompt_message_count_absent_passes_none(
        self, client, container,
    ):
        """Absent `pre_prompt_message_count` in the HTTP body → lifecycle
        receives `AfterTurnParams(pre_prompt_message_count=None)` →
        runtime takes the tail-walker 'derived' branch."""
        body = {"session_key": "agent:main:main", "session_id": str(uuid.uuid4())}
        r = await client.post("/context/after-turn", json=body)
        assert r.status_code == 200
        container.context_lifecycle.after_turn.assert_called_once()
        params = container.context_lifecycle.after_turn.call_args.args[0]
        assert params.pre_prompt_message_count is None, (
            f"absent body field must surface as None to the lifecycle; "
            f"got {params.pre_prompt_message_count!r}"
        )

    async def test_after_turn_pre_prompt_message_count_zero_honored(
        self, client, container,
    ):
        """Explicit `pre_prompt_message_count=0` in the HTTP body →
        lifecycle receives `AfterTurnParams(pre_prompt_message_count=0)`
        (int zero, NOT None) → runtime takes the 'plugin' branch honoring
        the zero verbatim. Regression that falsy-collapsed 0 → None would
        silently route to 'derived' and break the P4 first-turn contract."""
        body = {
            "session_key": "agent:main:main",
            "session_id": str(uuid.uuid4()),
            "pre_prompt_message_count": 0,
        }
        r = await client.post("/context/after-turn", json=body)
        assert r.status_code == 200
        container.context_lifecycle.after_turn.assert_called_once()
        params = container.context_lifecycle.after_turn.call_args.args[0]
        assert params.pre_prompt_message_count == 0
        assert params.pre_prompt_message_count is not None, (
            "explicit 0 must NOT collapse to None across the HTTP hop — "
            "the P4 hybrid-A+C design distinguishes 'plugin emitted 0' "
            "(honor) from 'plugin silent' (derive)"
        )

    async def test_after_turn_pre_prompt_message_count_positive_passes_through(
        self, client, container,
    ):
        """Positive `pre_prompt_message_count=3` in the HTTP body →
        lifecycle receives `AfterTurnParams(pre_prompt_message_count=3)`
        with the full `messages` list → runtime slices `messages[3:]` as
        the response delta. Pins the positive-integer path end-to-end."""
        body = {
            "session_key": "agent:main:main",
            "session_id": str(uuid.uuid4()),
            "pre_prompt_message_count": 3,
            "messages": [
                {"role": "user", "content": "hist q1"},
                {"role": "assistant", "content": "hist a1"},
                {"role": "user", "content": "current q"},
                {"role": "assistant", "content": "current a"},
            ],
        }
        r = await client.post("/context/after-turn", json=body)
        assert r.status_code == 200
        container.context_lifecycle.after_turn.assert_called_once()
        params = container.context_lifecycle.after_turn.call_args.args[0]
        assert params.pre_prompt_message_count == 3
        assert len(params.messages) == 4, (
            "full messages envelope must survive the hop intact — the "
            "runtime's slice arithmetic depends on it"
        )

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

    async def test_dispose_logs_deprecation_message(self, client, caplog):
        """TF-06-012 V-deprecation: the /context/dispose route is kept for
        backward compatibility only (GF-15) — TS plugins should call
        /sessions/end instead. The route logs a DEPRECATED message at
        INFO level on every invocation. Pins context.py:172-176."""
        body = {"session_key": "agent:main:main", "session_id": "sid-1"}
        with caplog.at_level(logging.INFO, logger="elephantbroker.api.routes.context"):
            r = await client.post("/context/dispose", json=body)
        assert r.status_code == 200

        deprecation_logs = [
            rec for rec in caplog.records
            if rec.levelno == logging.INFO
            and "DEPRECATED" in rec.getMessage()
            and "/context/dispose" in rec.getMessage()
        ]
        assert len(deprecation_logs) == 1, (
            f"expected exactly one deprecation log; got {len(deprecation_logs)}: "
            f"{[r.getMessage() for r in caplog.records]}"
        )

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
        assert "coding" in msg
        # TODO-6-382 (Round 3, Blind Spot INFO): WARN format aligned with
        # /memory/ingest-messages — exc_info=True carries the stack trace
        # in LogRecord.exc_info, not in the formatted message string.
        assert warning_records[0].exc_info is not None
        assert isinstance(warning_records[0].exc_info[1], RuntimeError)
        assert "transient-db-hiccup" in str(warning_records[0].exc_info[1])

    async def test_get_config_passes_gateway_org_id_to_resolve_profile(
        self, client, container,
    ):
        """TODO-6-751 (Round 2, Feature MEDIUM): ``/context/config?profile=X``
        must pass the gateway's configured ``org_id`` to ``resolve_profile()``
        so admin-registered org overrides reach this P6 touchpoint. Before
        the fix, ``org_id=None`` was hardcoded, silently dropping org
        context."""
        from unittest.mock import AsyncMock

        from elephantbroker.schemas.config import ElephantBrokerConfig
        from elephantbroker.schemas.profile import ProfilePolicy

        config = ElephantBrokerConfig()
        config.gateway.org_id = "acme"
        container.config = config
        container.profile_registry.resolve_profile = AsyncMock(
            return_value=ProfilePolicy(id="coding", name="Coding", ingest_batch_size=2),
        )

        r = await client.get("/context/config?profile=coding")
        assert r.status_code == 200
        # Org-overridden flush threshold flowed through to the response.
        assert r.json()["ingest_batch_size"] == 2

        # Assertion: the route must have passed org_id="acme", NOT None.
        container.profile_registry.resolve_profile.assert_awaited_once()
        call_kwargs = container.profile_registry.resolve_profile.call_args.kwargs
        assert call_kwargs.get("org_id") == "acme", (
            f"expected org_id='acme' (from container.config.gateway.org_id) "
            f"to reach resolve_profile(); got kwargs={call_kwargs}"
        )

    async def test_subagent_rollback(self, client):
        body = {"parent_session_key": "p", "child_session_key": "c", "rollback_key": "k"}
        r = await client.post("/context/subagent/rollback", json=body)
        assert r.status_code == 200

    async def test_subagent_rollback_orphans_children_set_entry(self, client, container):
        """TF-06-007 V3 / known gap: POST /context/subagent/rollback only deletes
        the session_parent mapping (rollback_key). It does NOT remove the child
        from the parent's session_children SET — the child is left orphaned in
        the SET until the per-key TTL elapses. Pins context.py:152-162 documented
        intentional scope (rollback is best-effort cleanup of the forward edge)."""
        # TODO(TD-68): rollback should clean children SET entry — TTL-based
        # expiry is current workaround (see TECHNICAL-DEBT.md TD-68 for the
        # full asymmetry rationale and three fix directions).
        from unittest.mock import AsyncMock

        redis = AsyncMock()
        container.redis = redis  # default conftest leaves it None
        body = {
            "parent_session_key": "agent:parent:main",
            "child_session_key": "agent:child:sub1",
            "rollback_key": "eb:test:session_parent:agent:child:sub1",
        }
        r = await client.post("/context/subagent/rollback", json=body)
        assert r.status_code == 200
        assert r.json() == {"rolled_back": True}

        # Forward edge deleted (the parent mapping)
        redis.delete.assert_called_once_with(body["rollback_key"])
        # Reverse edge NOT touched — child remains in parent's session_children SET
        redis.srem.assert_not_called()
        # No write of any kind to the children SET key
        for call in redis.mock_calls:
            assert "session_children" not in str(call), (
                f"rollback must not touch session_children; got call: {call}"
            )


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
