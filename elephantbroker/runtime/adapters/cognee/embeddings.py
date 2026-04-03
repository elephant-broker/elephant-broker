"""Direct HTTP embedding service using an OpenAI-compatible endpoint."""
from __future__ import annotations

import httpx

from elephantbroker.schemas.config import CogneeConfig


class EmbeddingService:
    """Generates embeddings via an OpenAI-compatible HTTP endpoint.

    Uses httpx directly rather than Cognee's internal embedding pipeline
    so ElephantBroker can request embeddings on-demand for scoring/retrieval
    outside of Cognee's ``cognify`` flow.
    """

    def __init__(self, config: CogneeConfig) -> None:
        self._endpoint = config.embedding_endpoint.rstrip("/")
        self._model = config.embedding_model
        self._api_key = config.embedding_api_key
        self._dimensions = config.embedding_dimensions
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            headers: dict[str, str] = {"Content-Type": "application/json"}
            if self._api_key:
                headers["Authorization"] = f"Bearer {self._api_key}"
            self._client = httpx.AsyncClient(headers=headers, timeout=30.0)
        return self._client

    async def embed_text(self, text: str) -> list[float]:
        """Embed a single text string and return the embedding vector."""
        results = await self.embed_batch([text])
        return results[0]

    async def embed_batch(self, texts: list[str]) -> list[list[float]]:
        """Embed multiple texts in a single request.

        Returns embeddings in the same order as the input texts,
        regardless of server response ordering.
        """
        if not texts:
            return []

        client = await self._get_client()
        response = await client.post(
            f"{self._endpoint}/embeddings",
            json={"model": self._model, "input": texts},
        )
        response.raise_for_status()

        data = response.json()["data"]
        # Sort by index to guarantee ordering matches input
        sorted_data = sorted(data, key=lambda d: d["index"])
        return [item["embedding"] for item in sorted_data]

    def get_dimension(self) -> int:
        """Return the expected embedding dimension."""
        return self._dimensions

    async def close(self) -> None:
        """Close the underlying HTTP client."""
        if self._client is not None:
            await self._client.aclose()
            self._client = None
