"""Unit tests for VectorAdapter with mocked Qdrant client."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from elephantbroker.runtime.adapters.cognee.vector import VectorAdapter
from elephantbroker.schemas.config import CogneeConfig


def _make_adapter() -> VectorAdapter:
    return VectorAdapter(CogneeConfig(embedding_dimensions=4))


class TestVectorAdapter:
    async def test_search_similar_returns_results(self):
        adapter = _make_adapter()
        mock_client = AsyncMock()
        mock_hit = MagicMock()
        mock_hit.id = "hit1"
        mock_hit.score = 0.95
        mock_hit.payload = {"label": "test"}
        mock_response = MagicMock()
        mock_response.points = [mock_hit]
        mock_client.query_points = AsyncMock(return_value=mock_response)
        adapter._client = mock_client

        results = await adapter.search_similar("col", [1.0, 0.0, 0.0, 0.0], top_k=5)
        assert len(results) == 1
        assert results[0].id == "hit1"
        assert results[0].score == 0.95
        assert results[0].payload == {"label": "test"}
        # Verify named vector "text" is passed (Fix #31)
        call_kwargs = mock_client.query_points.call_args[1]
        assert call_kwargs["using"] == "text"

    async def test_search_similar_custom_using_parameter(self):
        """Verify using parameter can be overridden (TODO-8)."""
        adapter = _make_adapter()
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.points = []
        mock_client.query_points = AsyncMock(return_value=mock_response)
        adapter._client = mock_client

        await adapter.search_similar("col", [1.0, 0.0, 0.0, 0.0], using="summary")
        call_kwargs = mock_client.query_points.call_args[1]
        assert call_kwargs["using"] == "summary"

    async def test_delete_embedding_calls_delete(self):
        adapter = _make_adapter()
        mock_client = AsyncMock()
        adapter._client = mock_client

        await adapter.delete_embedding("col", "del_id")
        mock_client.delete.assert_awaited_once()

    async def test_close_cleans_up_client(self):
        adapter = _make_adapter()
        mock_client = AsyncMock()
        adapter._client = mock_client
        await adapter.close()
        mock_client.close.assert_awaited_once()
        assert adapter._client is None

    async def test_search_similar_none_score_fallback(self):
        adapter = _make_adapter()
        mock_client = AsyncMock()
        mock_hit = MagicMock()
        mock_hit.id = "hit1"
        mock_hit.score = None
        mock_hit.payload = {}
        mock_response = MagicMock()
        mock_response.points = [mock_hit]
        mock_client.query_points = AsyncMock(return_value=mock_response)
        adapter._client = mock_client

        results = await adapter.search_similar("col", [1.0, 0.0, 0.0, 0.0])
        assert results[0].score == 0.0

    async def test_search_similar_empty_results(self):
        adapter = _make_adapter()
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.points = []
        mock_client.query_points = AsyncMock(return_value=mock_response)
        adapter._client = mock_client

        results = await adapter.search_similar("col", [1.0, 0.0, 0.0, 0.0])
        assert results == []

    async def test_search_similar_with_filter(self):
        from qdrant_client.models import FieldCondition, Filter, MatchValue

        adapter = _make_adapter()
        mock_client = AsyncMock()
        mock_response = MagicMock()
        mock_response.points = []
        mock_client.query_points = AsyncMock(return_value=mock_response)
        adapter._client = mock_client

        f = Filter(must=[FieldCondition(key="scope", match=MatchValue(value="session"))])
        await adapter.search_similar("col", [1.0, 0.0, 0.0, 0.0], filters=f)
        call_kwargs = mock_client.query_points.call_args[1]
        assert call_kwargs["query_filter"] is f
