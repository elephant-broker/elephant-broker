"""Tests for OtelTraceQueryClient."""
from __future__ import annotations

from unittest.mock import MagicMock

from elephantbroker.runtime.consolidation.otel_trace_query_client import OtelTraceQueryClient
from elephantbroker.schemas.config import ClickHouseConfig


class TestOtelTraceQueryClient:
    def test_not_available_when_disabled(self):
        config = ClickHouseConfig(enabled=False)
        client = OtelTraceQueryClient(config)
        assert client.available is False

    async def test_get_tool_sequences_returns_empty_when_unavailable(self):
        config = ClickHouseConfig(enabled=False)
        client = OtelTraceQueryClient(config)
        result = await client.get_tool_sequences("gw-1")
        assert result == []

    def test_close_no_error_when_no_client(self):
        config = ClickHouseConfig(enabled=False)
        client = OtelTraceQueryClient(config)
        client.close()  # Should not raise

    def test_available_property(self):
        config = ClickHouseConfig(enabled=False)
        client = OtelTraceQueryClient(config)
        assert client.available is False
        assert client._client is None
