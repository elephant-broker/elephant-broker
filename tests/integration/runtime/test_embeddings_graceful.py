"""R2-P8 / #1156 RESOLVED — EmbeddingService graceful degradation
(integration gate).

Pre-fix: ``embed_batch`` did direct ``response.json()["data"]`` access
— a transient LLM API glitch returning a malformed payload (no
``data`` key, non-dict items, missing ``index`` / ``embedding``)
crashed the whole ingest / retrieval pipeline with an uncaught
``KeyError`` / ``TypeError``.

Post-fix: parsing wrapped in defensive ``try/except`` that returns
``[]`` with a WARNING log. Downstream callers (turn-ingest, working-
set scoring, rerank, consolidation) skip the affected batch and retry
next call.

This integration test pairs with the unit-level test in
``tests/unit/adapters/cognee/test_embeddings.py``:

* **Test 1 (sanity)**: real LiteLLM proxy → ``embed_batch`` returns
  embeddings of the configured dimension.
* **Test 2 (degradation)**: monkeypatch the underlying httpx client to
  return a malformed 200 response → ``embed_batch`` returns ``[]``
  + emits the WARNING log instead of raising.

Requires the integration stack (Neo4j + Qdrant + Redis + LiteLLM
proxy) per ``scripts/run-integration-tests.sh``.
"""
from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest


@pytest.mark.integration
class TestEmbeddingServiceGracefulDegradation:
    """R2-P8 / #1156 — graceful-degradation contract for ``embed_batch``."""

    async def test_embed_batch_sanity_against_real_litellm(self, embedding_service):
        """Sanity check: a normal ``embed_batch`` call against the real
        LiteLLM proxy returns embeddings of the configured dimension —
        confirms the integration stack is healthy and the post-#1156
        try/except did not regress the happy path.
        """
        result = await embedding_service.embed_batch(["hello world"])
        assert len(result) == 1
        assert len(result[0]) == embedding_service.get_dimension()
        # Vector should have at least one non-zero component.
        assert any(abs(x) > 1e-9 for x in result[0])

    async def test_embed_batch_returns_empty_on_simulated_malformed_response(
        self, embedding_service, caplog,
    ):
        """Simulate a malformed upstream response by replacing the
        underlying httpx client with a mock that returns 200 +
        ``{"object": "list"}`` (no ``data`` key). Asserts the
        graceful-degradation contract: ``embed_batch`` returns ``[]``
        with a WARNING log instead of raising.

        The integration angle: the surrounding fixture wires the
        adapter against the real Cognee config; only the HTTP boundary
        is patched. This validates that the safety net composes with
        the rest of the runtime (config loading, retry semantics, etc.)
        rather than just the parsing function in isolation.
        """
        # Save the original client so we can restore it (the fixture
        # cleanup also closes whichever client is bound).
        original_client = embedding_service._client

        mock_client = AsyncMock(spec=httpx.AsyncClient)
        resp = MagicMock(spec=httpx.Response)
        resp.status_code = 200
        resp.raise_for_status = MagicMock()
        resp.json.return_value = {"object": "list"}  # no "data" key
        mock_client.post.return_value = resp
        embedding_service._client = mock_client

        try:
            with caplog.at_level(
                logging.WARNING,
                logger="elephantbroker.runtime.adapters.cognee.embeddings",
            ):
                result = await embedding_service.embed_batch(["irrelevant"])

            assert result == []
            assert "embed_batch malformed response" in caplog.text
            assert "graceful-degradation safety net" in caplog.text
            assert "#1156" in caplog.text
        finally:
            embedding_service._client = original_client
