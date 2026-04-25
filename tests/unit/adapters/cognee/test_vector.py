"""Unit tests for VectorAdapter with mocked Qdrant client."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

from elephantbroker.runtime.adapters.cognee.vector import VectorAdapter
from elephantbroker.schemas.config import CogneeConfig


def _make_adapter() -> VectorAdapter:
    # F9: pick a model name that is NOT in KNOWN_EMBEDDING_DIMS so the
    # cross-validator skips us — these tests use a 4-dim placeholder for
    # speed and don't care about real model semantics.
    return VectorAdapter(
        CogneeConfig(embedding_model="test/fake-4d", embedding_dimensions=4),
    )


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
        # G1: client.delete is called with collection_name + PointIdsList(points=[id])
        from qdrant_client.models import PointIdsList
        call_kwargs = mock_client.delete.call_args.kwargs
        assert call_kwargs["collection_name"] == "col"
        assert isinstance(call_kwargs["points_selector"], PointIdsList)
        assert call_kwargs["points_selector"].points == ["del_id"]

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

    # ------------------------------------------------------------------
    # TF-FN-008 additions
    # ------------------------------------------------------------------

    async def test_delete_embedding_silent_on_missing(self):
        """G2: Qdrant delete on a missing point ID returns successfully (0 points affected).

        The adapter doesn't introspect the result, so this is a silent no-op — callers
        that need "at least one point deleted" semantics must check count themselves
        via a pre-delete retrieve.
        """
        adapter = _make_adapter()
        mock_client = AsyncMock()
        mock_client.delete = AsyncMock(return_value=MagicMock())
        adapter._client = mock_client

        # Must not raise even though the ID is nonexistent in the collection
        await adapter.delete_embedding("col", "nonexistent")

    async def test_lazy_init_no_client_until_first_op(self):
        """G3: VectorAdapter constructor does NOT instantiate a Qdrant client.

        Client is lazy-initialized on first op via _get_client(). Enables tests to
        construct adapters without live Qdrant, and defers socket cost until first use.
        Mirrors the same pattern for GraphAdapter (TF-FN-007 G7).
        """
        adapter = VectorAdapter(CogneeConfig())
        assert adapter._client is None

    async def test_delete_embedding_passes_id_verbatim_documented_prod_risk(self):
        """Pins documented PROD risk #1485 — VectorAdapter passes IDs verbatim.

        ID-format mismatch (eb_id string vs Cognee internal UUID) is the caller's
        responsibility (MemoryStoreFacade.delete et al.). Any translation layer must
        live in the caller, not here. If a future change adds ID translation to
        VectorAdapter, update this test, the TF-FN-008 plan, and file a TD.
        """
        adapter = _make_adapter()
        mock_client = AsyncMock()
        adapter._client = mock_client

        await adapter.delete_embedding("col", "some-eb-id-not-uuid-format")
        from qdrant_client.models import PointIdsList
        call_kwargs = mock_client.delete.call_args.kwargs
        assert isinstance(call_kwargs["points_selector"], PointIdsList)
        assert call_kwargs["points_selector"].points == ["some-eb-id-not-uuid-format"]

    # ------------------------------------------------------------------
    # R2-P4 / #1189 RESOLVED — public ping() probe method
    # ------------------------------------------------------------------

    async def test_ping_calls_get_collections_on_success(self):
        """G4 (R2-P4): ``ping()`` obtains the async client and calls
        ``get_collections()`` — works on an empty Qdrant deployment, no
        collection required. Returns None on success.
        """
        adapter = _make_adapter()
        mock_client = AsyncMock()
        mock_client.get_collections = AsyncMock(return_value=MagicMock())
        adapter._client = mock_client

        result = await adapter.ping()
        assert result is None
        mock_client.get_collections.assert_awaited_once()

    async def test_ping_raises_on_qdrant_failure(self):
        """G5 (R2-P4): ``ping()`` does NOT swallow exceptions. Caller
        (the /health/ready route) catches and reports the error
        verbatim — preserves the operational debugging affordance the
        pre-fix inline ``_get_client + get_collections`` provided.
        """
        adapter = _make_adapter()
        mock_client = AsyncMock()
        mock_client.get_collections = AsyncMock(side_effect=ConnectionError("qdrant down"))
        adapter._client = mock_client

        try:
            await adapter.ping()
        except ConnectionError as exc:
            assert "qdrant down" in str(exc)
        else:
            raise AssertionError("ping() must not swallow underlying exceptions")
