"""Tests for FULL mode ingest gate — Step 3 of FIX-PLAN-Ingest-Architecture."""
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

from elephantbroker.schemas.trace import TraceEventType


class TestIngestGate:
    """Verify that ingest-messages is gated when context lifecycle is active (FULL mode)."""

    async def test_ingest_messages_skips_buffer_in_full_mode(self, client, container):
        """When context_lifecycle is not None, return 202 without touching buffer."""
        # container.context_lifecycle is already set by conftest
        assert container.context_lifecycle is not None

        r = await client.post(
            "/memory/ingest-messages",
            json={
                "session_key": "agent:main:main",
                "messages": [{"role": "user", "content": "hello"}],
            },
        )
        assert r.status_code == 202
        data = r.json()
        assert data["status"] == "buffered"
        assert "Full mode" in data["message"]

        # Buffer should NOT have been touched (it's None in conftest, but gate returns before accessing it)
        assert container.ingest_buffer is None

    async def test_ingest_messages_buffers_when_no_lifecycle(self, client, container):
        """When context_lifecycle is None (MEMORY_ONLY), buffering proceeds."""
        container.context_lifecycle = None

        r = await client.post(
            "/memory/ingest-messages",
            json={
                "session_key": "agent:main:main",
                "messages": [{"role": "user", "content": "hello"}],
            },
        )
        # Buffer is None → falls through to "buffer not available" 202
        assert r.status_code == 202
        data = r.json()
        assert "Buffer not available" in data["message"]

    async def test_ingest_gate_increments_metric(self, client, container):
        """Gate increments inc_ingest_gate_skip('full_mode') when lifecycle active."""
        container.metrics_ctx.inc_ingest_gate_skip = MagicMock()

        r = await client.post(
            "/memory/ingest-messages",
            json={
                "session_key": "agent:main:main",
                "messages": [{"role": "user", "content": "hello"}],
            },
        )
        assert r.status_code == 202
        container.metrics_ctx.inc_ingest_gate_skip.assert_called_once_with("full_mode")

    async def test_ingest_gate_does_not_increment_buffer_flush_metric(self, client, container):
        """TODO-8-R1-012 — gate-skip path is NOT a buffer flush.

        4-reviewer R1 consensus (LT + interop + BS + BL): the gate-skip
        path used to fire ``inc_buffer_flush("gate_skip_full_mode")``,
        polluting the buffer-flush metric with non-flush events. The
        gate-skip is captured by ``inc_ingest_gate_skip("full_mode")``
        on its own metric (``eb_ingest_gate_skips_total``) — that's the
        correct surface. This test pins the post-fix contract: the
        gate-skip path must NOT touch ``inc_buffer_flush``.
        """
        container.metrics_ctx.inc_buffer_flush = MagicMock()

        r = await client.post(
            "/memory/ingest-messages",
            json={
                "session_key": "agent:main:main",
                "messages": [{"role": "user", "content": "hello"}],
            },
        )
        assert r.status_code == 202
        # Pre-fix this asserted ``inc_buffer_flush.assert_called_once_with(
        # "gate_skip_full_mode")``. Post-fix the gate-skip path emits
        # ONLY ``inc_ingest_gate_skip``; ``inc_buffer_flush`` must not
        # fire (the buffer is not flushed in this branch).
        container.metrics_ctx.inc_buffer_flush.assert_not_called()

    async def test_ingest_gate_does_not_emit_buffer_flush_trace(self, client, container):
        """TODO-8-600 — R2 carry-over (LT): gate-skip path must NOT emit
        ``INGEST_BUFFER_FLUSH``.

        Pre-R2 this path emitted ``INGEST_BUFFER_FLUSH`` with payload
        ``action=gate_skip_full_mode`` (the prior ``test_ingest_gate_trace_event_includes_session_id``
        pinned that behaviour as TODO-11-006). The R1 fix removed the
        matching ``inc_buffer_flush`` metric (TODO-8-R1-012); R2 carries
        the trace-event removal. Reason: gate skip is NOT a buffer flush
        — emitting INGEST_BUFFER_FLUSH here poisons
        ``/trace?event_type=INGEST_BUFFER_FLUSH`` queries with non-flush
        events and confuses session-timeline rendering. The gate skip is
        captured by ``eb_ingest_gate_skips_total`` on the metric side
        (no equivalent trace surface today; if one is needed later, a
        dedicated ``INGEST_GATE_SKIPPED`` enum value is the right path,
        not overloading INGEST_BUFFER_FLUSH).

        This test pins the post-R2 contract: zero INGEST_BUFFER_FLUSH
        events on the gate-skip path.
        """
        container.trace_ledger.append_event = AsyncMock(side_effect=lambda e: e)

        r = await client.post(
            "/memory/ingest-messages",
            json={
                "session_key": "agent:main:main",
                "session_id": "00000000-0000-0000-0000-000000000001",
                "messages": [{"role": "user", "content": "hello"}],
            },
        )
        assert r.status_code == 202

        # Post-R2: NO INGEST_BUFFER_FLUSH on gate-skip path.
        flush_calls = [
            c[0][0] for c in container.trace_ledger.append_event.call_args_list
            if c[0][0].event_type == TraceEventType.INGEST_BUFFER_FLUSH
        ]
        assert flush_calls == [], (
            f"gate-skip path must NOT emit INGEST_BUFFER_FLUSH; "
            f"observed {len(flush_calls)} event(s)"
        )

    async def test_ingest_gate_no_metrics_ctx(self, client, container):
        """Gate still works when metrics_ctx is None — no metric emitted, no crash."""
        container.metrics_ctx = None

        r = await client.post(
            "/memory/ingest-messages",
            json={
                "session_key": "agent:main:main",
                "messages": [{"role": "user", "content": "hello"}],
            },
        )
        assert r.status_code == 202
        data = r.json()
        assert data["status"] == "buffered"
        assert "Full mode" in data["message"]


class TestIngestMessagesProfileWireThrough:
    """TODO-6-701 / TODO-6-401: /memory/ingest-messages must resolve the
    per-profile ``ingest_batch_size`` override and pass it through to
    ``buffer.add_messages(effective_batch_size=...)``.

    All tests run in MEMORY_ONLY mode (``context_lifecycle = None``) so the
    FULL-mode short-circuit at memory.py:391 doesn't hide the call. A mocked
    ingest buffer + turn-ingest pipeline lets us assert on the kwarg without
    needing real Redis.
    """

    @staticmethod
    def _arrange_memory_only(container):
        """Switch the fixture to MEMORY_ONLY and wire mock buffer + pipeline.

        Returns (mock_buffer, mock_pipeline). Buffer defaults to returning
        False from ``add_messages`` so the flush branch is not exercised —
        the wiring claim is about the kwarg, not the flush behavior.
        """
        from unittest.mock import AsyncMock

        from elephantbroker.schemas.config import ElephantBrokerConfig
        from elephantbroker.schemas.pipeline import TurnIngestResult

        container.context_lifecycle = None
        container.config = ElephantBrokerConfig()  # global ingest_batch_size = 6

        mock_buffer = AsyncMock()
        mock_buffer.add_messages = AsyncMock(return_value=False)
        mock_buffer.flush = AsyncMock(return_value=[])
        container.ingest_buffer = mock_buffer

        mock_pipeline = AsyncMock()
        mock_pipeline.run = AsyncMock(return_value=TurnIngestResult(facts_stored=0))
        container.turn_ingest = mock_pipeline

        return mock_buffer, mock_pipeline

    async def test_profile_override_wires_through_to_buffer(self, client, container):
        """Profile with ``ingest_batch_size=2`` must call
        ``buffer.add_messages(..., effective_batch_size=2)`` — the core
        TODO-6-701 claim."""
        from unittest.mock import AsyncMock

        from elephantbroker.schemas.profile import ProfilePolicy

        mock_buffer, _ = self._arrange_memory_only(container)
        container.profile_registry.resolve_profile = AsyncMock(
            return_value=ProfilePolicy(id="coding", name="Coding", ingest_batch_size=2),
        )

        r = await client.post(
            "/memory/ingest-messages",
            json={
                "session_key": "agent:main:main",
                "messages": [{"role": "user", "content": "hello"}],
                "profile_name": "coding",
            },
        )
        # add_messages returns False (mock default) → route returns 202
        # "buffered, waiting for batch". The wiring claim is about the kwarg
        # passed into add_messages, not the response shape.
        assert r.status_code == 202
        mock_buffer.add_messages.assert_awaited_once()
        assert mock_buffer.add_messages.call_args.kwargs["effective_batch_size"] == 2

    async def test_profile_without_override_falls_back_to_global(self, client, container):
        """Profile with ``ingest_batch_size=None`` must receive the global
        LLMConfig default (6). The registry helper performs the fallback,
        and that value is what the buffer receives."""
        from unittest.mock import AsyncMock

        from elephantbroker.schemas.profile import ProfilePolicy

        mock_buffer, _ = self._arrange_memory_only(container)
        container.profile_registry.resolve_profile = AsyncMock(
            return_value=ProfilePolicy(id="coding", name="Coding", ingest_batch_size=None),
        )

        r = await client.post(
            "/memory/ingest-messages",
            json={
                "session_key": "agent:main:main",
                "messages": [{"role": "user", "content": "hello"}],
                "profile_name": "coding",
            },
        )
        # add_messages returns False (mock default) → route returns 202
        # "buffered, waiting for batch". The wiring claim is about the kwarg
        # passed into add_messages, not the response shape.
        assert r.status_code == 202
        mock_buffer.add_messages.assert_awaited_once()
        # Global LLMConfig.ingest_batch_size default is 6.
        assert mock_buffer.add_messages.call_args.kwargs["effective_batch_size"] == 6

    async def test_no_profile_registry_passes_none(self, client, container):
        """When ``container.profile_registry is None``, the route must pass
        ``effective_batch_size=None`` — the buffer then falls back to its
        LLMConfig at the ``add_messages`` call site."""
        mock_buffer, _ = self._arrange_memory_only(container)
        container.profile_registry = None

        r = await client.post(
            "/memory/ingest-messages",
            json={
                "session_key": "agent:main:main",
                "messages": [{"role": "user", "content": "hello"}],
                "profile_name": "coding",
            },
        )
        # add_messages returns False (mock default) → route returns 202
        # "buffered, waiting for batch". The wiring claim is about the kwarg
        # passed into add_messages, not the response shape.
        assert r.status_code == 202
        mock_buffer.add_messages.assert_awaited_once()
        assert mock_buffer.add_messages.call_args.kwargs["effective_batch_size"] is None

    async def test_profile_resolution_key_error_warns_and_falls_back(
        self, client, container, caplog,
    ):
        """TODO-6-581 (Round 3, Interop MEDIUM): unknown profile names raise
        ``KeyError`` from ``resolve_profile()``; this endpoint is
        fire-and-forget-write from the TS plugin client (no status check on
        the response — see client.ts:171-183), so surfacing 404 here would
        silently drop messages. The handler now folds ``KeyError`` into the
        broader Exception fallback: WARN-log + global-default
        ``effective_batch_size``. This REPLACES the Round 2 404 behavior
        introduced in 0c67977 (TODO-6-353); the "mirror /context/config"
        rationale did not transfer across HTTP methods (GET read-only vs
        POST fire-and-forget-write)."""
        import logging
        from unittest.mock import AsyncMock

        mock_buffer, _ = self._arrange_memory_only(container)
        container.profile_registry.resolve_profile = AsyncMock(
            side_effect=KeyError("Unknown profile: missing"),
        )

        with caplog.at_level(logging.WARNING, logger="elephantbroker.api.routes.memory"):
            r = await client.post(
                "/memory/ingest-messages",
                json={
                    "session_key": "agent:main:main",
                    "messages": [{"role": "user", "content": "hello"}],
                    "profile_name": "missing",
                },
            )

        # The buffer MUST have been awaited (messages not silently dropped),
        # with effective_batch_size=None (global LLMConfig default applied at
        # the buffer level).
        assert r.status_code == 202, (
            f"expected 202 (buffered with global fallback); got {r.status_code} — "
            "the TS client is fire-and-forget and would silently drop messages on 404."
        )
        mock_buffer.add_messages.assert_awaited_once()
        assert mock_buffer.add_messages.call_args.kwargs["effective_batch_size"] is None

        warning_records = [
            rec for rec in caplog.records
            if rec.levelno == logging.WARNING
            and rec.name == "elephantbroker.api.routes.memory"
            and "profile resolution failed" in rec.getMessage()
        ]
        assert len(warning_records) == 1, (
            f"expected exactly one WARNING on the KeyError-fallback branch, got {len(warning_records)}"
        )
        msg = warning_records[0].getMessage()
        assert "missing" in msg
        # TODO-6-382: exc_info=True captures the stack trace for diagnosability.
        assert warning_records[0].exc_info is not None
        assert isinstance(warning_records[0].exc_info[1], KeyError)

    async def test_profile_resolution_transient_exception_warns_and_falls_back(
        self, client, container, caplog,
    ):
        """TODO-6-581 (Round 3, Interop MEDIUM): transient resolver exceptions
        (e.g. registry/DB faults surfaced as ``RuntimeError``) fall through
        the same ``except Exception`` branch as KeyError: WARN-log + global
        fallback + buffered write. Exercises the transient-origin side of
        the combined branch so regressions that split the handling back
        apart are caught."""
        import logging
        from unittest.mock import AsyncMock

        mock_buffer, _ = self._arrange_memory_only(container)
        container.profile_registry.resolve_profile = AsyncMock(
            side_effect=RuntimeError("transient-db-hiccup"),
        )

        with caplog.at_level(logging.WARNING, logger="elephantbroker.api.routes.memory"):
            r = await client.post(
                "/memory/ingest-messages",
                json={
                    "session_key": "agent:main:main",
                    "messages": [{"role": "user", "content": "hello"}],
                    "profile_name": "coding",
                },
            )

        # add_messages returns False (mock default) → route returns 202
        # "buffered, waiting for batch". The wiring claim is about the kwarg
        # passed into add_messages, not the response shape.
        assert r.status_code == 202
        mock_buffer.add_messages.assert_awaited_once()
        assert mock_buffer.add_messages.call_args.kwargs["effective_batch_size"] is None

        warning_records = [
            rec for rec in caplog.records
            if rec.levelno == logging.WARNING
            and rec.name == "elephantbroker.api.routes.memory"
            and "profile resolution failed" in rec.getMessage()
        ]
        assert len(warning_records) == 1, (
            f"expected exactly one warning log on the transient-fallback branch, got {len(warning_records)}"
        )
        assert "coding" in warning_records[0].getMessage()

    async def test_batch_flush_increments_buffer_flush_metric(self, client, container):
        """Gap #1: inc_buffer_flush('batch_size') emitted when buffer flush triggered."""
        from unittest.mock import AsyncMock

        from elephantbroker.schemas.pipeline import TurnIngestResult

        mock_buffer, mock_pipeline = self._arrange_memory_only(container)
        # Make buffer report batch_ready=True to trigger the flush branch
        mock_buffer.add_messages = AsyncMock(return_value=True)
        mock_buffer.flush = AsyncMock(return_value=[{"role": "user", "content": "hello"}])
        mock_pipeline.run = AsyncMock(return_value=TurnIngestResult(facts_stored=1))
        container.metrics_ctx = MagicMock()

        r = await client.post(
            "/memory/ingest-messages",
            json={
                "session_key": "agent:main:main",
                "messages": [{"role": "user", "content": "hello"}],
            },
        )
        assert r.status_code == 200
        container.metrics_ctx.inc_buffer_flush.assert_called_once_with("batch_size")

    async def test_org_override_wires_through_org_id_to_resolve_profile(
        self, client, container,
    ):
        """TODO-6-751 (Round 2, Feature MEDIUM): the route must pass the
        gateway's configured ``org_id`` to ``resolve_profile()`` so
        admin-registered org overrides reach this P6 touchpoint. Before
        the fix, ``org_id=None`` was hardcoded, silently dropping org
        context."""
        from unittest.mock import AsyncMock

        from elephantbroker.schemas.config import ElephantBrokerConfig
        from elephantbroker.schemas.profile import ProfilePolicy

        mock_buffer, _ = self._arrange_memory_only(container)
        # Configure an org_id on the gateway; this is what must flow through.
        config = ElephantBrokerConfig()
        config.gateway.org_id = "acme"
        container.config = config

        # Return an org-overridden policy (ingest_batch_size=2 — org's tighter
        # flush). The assertion below is on the call kwargs, not on the
        # returned value — that's verified separately by the override test.
        container.profile_registry.resolve_profile = AsyncMock(
            return_value=ProfilePolicy(id="coding", name="Coding", ingest_batch_size=2),
        )

        r = await client.post(
            "/memory/ingest-messages",
            json={
                "session_key": "agent:main:main",
                "messages": [{"role": "user", "content": "hello"}],
                "profile_name": "coding",
            },
        )
        assert r.status_code == 202

        # Assertion: the route must have passed org_id="acme", NOT None.
        container.profile_registry.resolve_profile.assert_awaited_once()
        call_kwargs = container.profile_registry.resolve_profile.call_args.kwargs
        assert call_kwargs.get("org_id") == "acme", (
            f"expected org_id='acme' (from container.config.gateway.org_id) "
            f"to reach resolve_profile(); got kwargs={call_kwargs}"
        )
        # Sanity: the returned policy's ingest_batch_size=2 also flowed through.
        assert mock_buffer.add_messages.call_args.kwargs["effective_batch_size"] == 2
