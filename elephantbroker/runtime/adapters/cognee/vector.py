"""Qdrant vector adapter — wraps qdrant-client directly."""
from __future__ import annotations

from typing import Any

from pydantic import BaseModel
from qdrant_client import AsyncQdrantClient
from qdrant_client.models import FieldCondition, Filter, MatchValue, PointIdsList

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

    def __init__(self, config: CogneeConfig, gateway_id: str = "") -> None:
        self._url = config.qdrant_url
        self._default_dimension = config.embedding_dimensions
        self._client: AsyncQdrantClient | None = None
        # #1187 / TD-64 RESOLVED (R2-P1): retain gateway_id for the
        # automatic `database_name` tenant filter added to search_similar.
        # The paired configure_cognee() threads the same gateway_id into
        # Cognee's `vector_db_name` config so every point payload written
        # via add_data_points() carries `database_name=<gateway_id>`. Read
        # + write sides agree on the tenant key.
        # Empty gateway_id disables the filter — preserves legacy
        # single-tenant behavior + back-compat with pre-R2-P1 points that
        # have `database_name=""`.
        self._gateway_id = gateway_id

    async def _get_client(self) -> AsyncQdrantClient:
        if self._client is None:
            self._client = AsyncQdrantClient(url=self._url)
        return self._client

    async def ping(self) -> None:
        """Lightweight Qdrant connectivity probe — raises on failure.

        R2-P4 / #1189 RESOLVED: this is the public connectivity check for
        ``/health/ready``. Internally it obtains the async client and lists
        collections (works on an empty Qdrant deployment, no collection
        required). Replaces the prior coupling where the health route
        reached into ``vector._get_client()`` directly — a leading-
        underscore name that conventionally signals "internal
        implementation detail, not part of the public API."

        Raises whatever the underlying client raises so callers can log
        the original error message + observe latency.
        """
        client = await self._get_client()
        await client.get_collections()

    def _gateway_filter(self) -> FieldCondition | None:
        """Return a ``database_name=<gateway_id>`` filter condition, or
        ``None`` when the adapter is gateway-agnostic (``gateway_id=""``).

        #1187 / TD-64 (R2-P1, path c): the community Qdrant adapter
        indexes ``database_name`` as a tenant field with
        ``is_tenant:true`` (per its ``payload_schema`` at
        ``cognee_community_vector_adapter_qdrant.qdrant_adapter``), so
        equality match on this key uses Qdrant's native multi-tenancy
        fast-path.
        """
        if not self._gateway_id:
            return None
        return FieldCondition(
            key="database_name",
            match=MatchValue(value=self._gateway_id),
        )

    async def search_similar(
        self,
        collection: str,
        query_embedding: list[float],
        top_k: int = 10,
        filters: Filter | None = None,
        using: str = "text",
    ) -> list[VectorSearchResult]:
        """Search for nearest neighbors by embedding vector.

        #1187 / TD-64 RESOLVED (R2-P1): when ``gateway_id`` is set, a
        ``database_name=<gateway_id>`` FieldCondition is merged into
        ``filters`` (as an additional ``must`` clause). This guarantees
        search results come from the calling gateway only — the
        dedup-leak surface pinned in TF-FN-018 G10 is closed post-fix.

        Caller-supplied ``filters`` retain their existing semantics
        (their ``must`` / ``should`` clauses are preserved); the gateway
        condition is appended to whichever ``must`` list they carried,
        never replacing the caller's filter.
        """
        client = await self._get_client()
        effective_filter = self._merge_gateway_filter(filters)
        results = await client.query_points(
            collection_name=collection,
            query=query_embedding,
            limit=top_k,
            query_filter=effective_filter,
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

    def _merge_gateway_filter(self, filters: Filter | None) -> Filter | None:
        """Combine the gateway tenant-filter with a caller-supplied Filter.

        * No gateway filter configured -> return ``filters`` unchanged.
        * No caller filter -> wrap the gateway condition in a fresh
          ``Filter(must=[gateway_cond])``.
        * Both present -> clone the caller's ``must`` list and append the
          gateway condition, preserving ``should`` / ``must_not``.

        Keeps the gateway-isolation guarantee even if a caller forgets to
        thread its own gateway clause (e.g., a downstream refactor that
        pulls search_similar into a new flow).
        """
        gateway_cond = self._gateway_filter()
        if gateway_cond is None:
            return filters
        if filters is None:
            return Filter(must=[gateway_cond])
        caller_must = list(filters.must or [])
        caller_must.append(gateway_cond)
        return Filter(
            must=caller_must,
            should=filters.should,
            must_not=filters.must_not,
        )

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
