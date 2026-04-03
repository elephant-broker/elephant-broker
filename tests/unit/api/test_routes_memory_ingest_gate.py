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
