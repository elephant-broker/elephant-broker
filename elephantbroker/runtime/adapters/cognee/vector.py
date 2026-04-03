"""Qdrant vector adapter — wraps qdrant-client directly."""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel
from qdrant_client import AsyncQdrantClient
from qdrant_client.models import Filter, PointIdsList

from elephantbroker.schemas.config import CogneeConfig


class VectorSearchResult(BaseModel):
    """A single vector search result."""
    id: str
    score: float
    payload: dict[str, Any] = {}


class VectorAdapter:
    """Read/delete vector adapter for search and cleanup.

    ALL vector indexing happens automatically via ``add_data_points()`` — it embeds
    ``index_fields`` into Qdrant collections named ``{ClassName}_{field_name}``
    (e.g., ``FactDataPoint_text``, ``ArtifactDataPoint_summary``).

    This adapter handles:
    - Filtered vector search: search_similar() on Cognee-managed collections
    - Delete: delete_embedding() for GDPR compliance

    DO NOT add write methods here. Use add_data_points() for all DataPoint storage.
    """

    def __init__(self, config: CogneeConfig) -> None:
        self._url = config.qdrant_url
        self._default_dimension = config.embedding_dimensions
        self._client: AsyncQdrantClient | None = None

    async def _get_client(self) -> AsyncQdrantClient:
        if self._client is None:
            self._client = AsyncQdrantClient(url=self._url)
        return self._client

    async def search_similar(
        self,
        collection: str,
        query_embedding: list[float],
        top_k: int = 10,
        filters: Filter | None = None,
        using: str = "text",
    ) -> list[VectorSearchResult]:
        """Search for nearest neighbors by embedding vector."""
        client = await self._get_client()
        results = await client.query_points(
            collection_name=collection,
            query=query_embedding,
            limit=top_k,
            query_filter=filters,
            using=using,
        )
        return [
            VectorSearchResult(
                id=str(hit.id),
                score=hit.score if hit.score is not None else 0.0,
                payload=dict(hit.payload) if hit.payload else {},
            )
            for hit in results.points
        ]

    async def delete_embedding(self, collection: str, id: str) -> None:
        """Delete a single vector by ID."""
        client = await self._get_client()
        await client.delete(
            collection_name=collection,
            points_selector=PointIdsList(points=[id]),
        )

    async def close(self) -> None:
        """Close the Qdrant client."""
        if self._client is not None:
            await self._client.close()
            self._client = None
