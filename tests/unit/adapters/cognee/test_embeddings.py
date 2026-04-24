"""Unit tests for the EmbeddingService."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import httpx

import pytest

from elephantbroker.runtime.adapters.cognee.embeddings import EmbeddingService
from elephantbroker.schemas.config import CogneeConfig


def _make_service(**overrides) -> EmbeddingService:
    cfg = CogneeConfig(
        embedding_endpoint="http://test:8811/v1",
        embedding_model="test-model",
        embedding_api_key="test-key",
        embedding_dimensions=4,
        **overrides,
    )
    return EmbeddingService(cfg)


def _mock_response(data: list[dict]) -> httpx.Response:
    resp = MagicMock(spec=httpx.Response)
    resp.status_code = 200
    resp.json.return_value = {"data": data}
    resp.raise_for_status = MagicMock()
    return resp


class TestEmbeddingService:
    async def test_embed_text_sends_correct_request(self):
        svc = _make_service()
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        mock_client.post.return_value = _mock_response([{"index": 0, "embedding": [1.0, 2.0, 3.0, 4.0]}])
        svc._client = mock_client

        result = await svc.embed_text("hello")
        assert result == [1.0, 2.0, 3.0, 4.0]

        call_args = mock_client.post.call_args
        assert "embeddings" in call_args[0][0]
        body = call_args[1]["json"]
        assert body["model"] == "test-model"
        assert body["input"] == ["hello"]

    async def test_embed_batch_preserves_order(self):
        svc = _make_service()
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        # Response has reversed indices to verify sorting
        mock_client.post.return_value = _mock_response([
            {"index": 1, "embedding": [0.0, 1.0, 0.0, 0.0]},
            {"index": 0, "embedding": [1.0, 0.0, 0.0, 0.0]},
        ])
        svc._client = mock_client

        results = await svc.embed_batch(["first", "second"])
        assert results[0] == [1.0, 0.0, 0.0, 0.0]  # index 0
        assert results[1] == [0.0, 1.0, 0.0, 0.0]  # index 1

    async def test_embed_batch_empty_returns_empty(self):
        svc = _make_service()
        # G2 extension: empty batch must NOT open the HTTP client (no-op fast path).
        svc._get_client = AsyncMock()
        results = await svc.embed_batch([])
        assert results == []
        assert svc._get_client.await_count == 0

    async def test_get_dimension(self):
        svc = _make_service()
        assert svc.get_dimension() == 4

    async def test_authorization_header_set(self):
        svc = _make_service()
        client = await svc._get_client()
        assert client.headers.get("authorization") == "Bearer test-key"
        await svc.close()

    async def test_close_cleans_up_client(self):
        svc = _make_service()
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        svc._client = mock_client
        await svc.close()
        mock_client.aclose.assert_awaited_once()
        assert svc._client is None

    async def test_embed_batch_http_error_raises(self):
        svc = _make_service()
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        error_resp = MagicMock(spec=httpx.Response)
        error_resp.status_code = 500
        error_resp.raise_for_status.side_effect = httpx.HTTPStatusError(
            "Server error", request=MagicMock(), response=error_resp,
        )
        mock_client.post.return_value = error_resp
        svc._client = mock_client
        with pytest.raises(httpx.HTTPStatusError):
            await svc.embed_batch(["test"])

    async def test_no_api_key_omits_auth_header(self):
        svc = EmbeddingService(CogneeConfig(
            embedding_endpoint="http://test:8811/v1",
            embedding_model="test-model",
            embedding_api_key="",
            embedding_dimensions=4,
        ))
        client = await svc._get_client()
        assert "authorization" not in {k.lower() for k in client.headers}
        await svc.close()

    # ------------------------------------------------------------------
    # TF-FN-009 additions
    # ------------------------------------------------------------------

    async def test_embed_text_delegates_to_embed_batch(self):
        """G1: embed_text() is a thin wrapper over embed_batch([text]).

        Pins the single-dispatch path -- there is no separate HTTP call for singleton
        text. Any batching, retry, or caching logic added to embed_batch must
        automatically apply to embed_text.
        """
        svc = _make_service()
        svc.embed_batch = AsyncMock(return_value=[[1.0, 2.0, 3.0, 4.0]])

        result = await svc.embed_text("hello")
        svc.embed_batch.assert_awaited_once_with(["hello"])
        assert result == [1.0, 2.0, 3.0, 4.0]

    async def test_client_uses_30_second_timeout(self):
        """G3: EmbeddingService constructs httpx.AsyncClient with 30s timeout.

        Pins the timeout default -- httpx.Timeout(30.0) sets connect/read/write/pool
        to 30s uniformly. Too short for cold-start BGE embeddings on CPU; too long
        lets hung LLM backends pile up requests. 30s is the verified balance.
        """
        svc = _make_service()
        client = await svc._get_client()
        assert client.timeout.read == 30.0
        assert client.timeout.connect == 30.0
        await svc.close()

    async def test_endpoint_url_trailing_slash_stripped(self):
        """G4: Constructor strips trailing '/' from embedding_endpoint (#1162).

        Ensures that operator-configured endpoints like http://x/v1/ and http://x/v1
        both produce the same final URL when appended with /embeddings.
        """
        svc = EmbeddingService(CogneeConfig(
            embedding_endpoint="http://test:8811/v1/",
            embedding_model="x",
            embedding_api_key="",
            embedding_dimensions=4,
        ))
        assert svc._endpoint == "http://test:8811/v1"

    async def test_embed_batch_missing_data_key_raises_keyerror(self):
        """G5: Malformed responses (no "data" key) raise uncaught KeyError at
        response.json()["data"].

        Pins documented GAP #1156 -- the caller must handle KeyError. EmbeddingService
        intentionally does NOT swallow or translate the error. If a future change adds
        defensive handling here (e.g., translate to a typed exception), update this
        test and the TF-FN-009 plan.
        """
        svc = _make_service()
        mock_client = AsyncMock(spec=httpx.AsyncClient)
        resp = MagicMock(spec=httpx.Response)
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        resp.json.return_value = {"object": "list"}  # no "data" key
        mock_client.post.return_value = resp
        svc._client = mock_client
        with pytest.raises(KeyError):
            await svc.embed_batch(["x"])
