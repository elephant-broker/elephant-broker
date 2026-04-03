"""Integration tests for VectorAdapter against a live Qdrant instance.

Storage and collection creation is done via add_data_points() (Cognee-first).
VectorAdapter only handles search and delete on Cognee-managed collections.

For search tests, we use the Qdrant client directly to set up test data in
ephemeral collections, then test search_similar and delete_embedding.
"""
from __future__ import annotations

import uuid

from qdrant_client.models import (
    Distance,
    FieldCondition,
    Filter,
    MatchValue,
    PointStruct,
    VectorParams,
)


class TestVectorAdapterIntegration:
    async def _setup_collection(self, vector_adapter, col, dim=4, points=None):
        """Create a test collection and insert points via raw Qdrant client."""
        client = await vector_adapter._get_client()
        exists = await client.collection_exists(col)
        if not exists:
            await client.create_collection(
                collection_name=col,
                vectors_config={"text": VectorParams(size=dim, distance=Distance.COSINE)},
            )
        if points:
            await client.upsert(collection_name=col, points=points)

    async def test_search_similar(self, vector_adapter):
        col = f"test_{uuid.uuid4().hex[:8]}"
        id1 = str(uuid.uuid4())
        id2 = str(uuid.uuid4())
        await self._setup_collection(vector_adapter, col, points=[
            PointStruct(id=id1, vector={"text": [1.0, 0.0, 0.0, 0.0]}, payload={"label": "north"}),
            PointStruct(id=id2, vector={"text": [0.0, 1.0, 0.0, 0.0]}, payload={"label": "east"}),
        ])

        results = await vector_adapter.search_similar(col, [1.0, 0.0, 0.0, 0.0], top_k=1)
        assert len(results) == 1
        assert results[0].id == id1
        assert results[0].payload["label"] == "north"

    async def test_search_with_payload_filter(self, vector_adapter):
        col = f"test_{uuid.uuid4().hex[:8]}"
        id_a = str(uuid.uuid4())
        id_b = str(uuid.uuid4())
        await self._setup_collection(vector_adapter, col, points=[
            PointStruct(id=id_a, vector={"text": [1.0, 0.0, 0.0, 0.0]}, payload={"kind": "fact"}),
            PointStruct(id=id_b, vector={"text": [0.9, 0.1, 0.0, 0.0]}, payload={"kind": "goal"}),
        ])

        filt = Filter(must=[FieldCondition(key="kind", match=MatchValue(value="goal"))])
        results = await vector_adapter.search_similar(col, [1.0, 0.0, 0.0, 0.0], top_k=5, filters=filt)
        assert len(results) == 1
        assert results[0].id == id_b

    async def test_search_top_k_limit(self, vector_adapter):
        col = f"test_{uuid.uuid4().hex[:8]}"
        points = [
            PointStruct(id=str(uuid.uuid4()), vector={"text": [1.0, 0.0, 0.0, float(i) / 10]}, payload={"i": i})
            for i in range(5)
        ]
        await self._setup_collection(vector_adapter, col, points=points)

        results = await vector_adapter.search_similar(col, [1.0, 0.0, 0.0, 0.0], top_k=2)
        assert len(results) == 2

    async def test_delete_embedding(self, vector_adapter):
        col = f"test_{uuid.uuid4().hex[:8]}"
        del_id = str(uuid.uuid4())
        await self._setup_collection(vector_adapter, col, points=[
            PointStruct(id=del_id, vector={"text": [0.5, 0.5, 0.0, 0.0]}, payload={"x": 1}),
        ])

        await vector_adapter.delete_embedding(col, del_id)
        results = await vector_adapter.search_similar(col, [0.5, 0.5, 0.0, 0.0], top_k=5)
        ids = [r.id for r in results]
        assert del_id not in ids
