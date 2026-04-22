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

    async def test_ingest_gate_trace_event_includes_session_id(self, client, container):
        """TODO-11-006: INGEST_BUFFER_FLUSH trace event includes session_id."""
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

        # Find INGEST_BUFFER_FLUSH among captured append_event calls
        flush_calls = [
            c[0][0] for c in container.trace_ledger.append_event.call_args_list
            if c[0][0].event_type == TraceEventType.INGEST_BUFFER_FLUSH
        ]
        assert len(flush_calls) >= 1
        assert str(flush_calls[0].session_id) == "00000000-0000-0000-0000-000000000001"
        assert flush_calls[0].session_key == "agent:main:main"

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

    async def test_profile_resolution_exception_warns_and_falls_back(
        self, client, container, caplog,
    ):
        """On ``resolve_profile`` failure the route must (a) not 500, (b) log
        a warning so the silent fallback is observable in logs, (c) pass
        ``effective_batch_size=None`` to the buffer."""
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
            f"expected exactly one warning log on the except branch, got {len(warning_records)}"
        )
        assert "missing" in warning_records[0].getMessage()
