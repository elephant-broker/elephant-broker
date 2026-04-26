"""Tests for ToolArtifactStore."""
import inspect
from unittest.mock import AsyncMock

from elephantbroker.runtime.adapters.cognee.datapoints import ArtifactDataPoint
from elephantbroker.runtime.artifacts.store import ToolArtifactStore
from elephantbroker.runtime.interfaces.artifact_store import IToolArtifactStore
from elephantbroker.runtime.trace.ledger import TraceLedger
from tests.fixtures.factories import make_tool_artifact


class TestArtifactStore:
    def _make(self):
        graph = AsyncMock()
        vector = AsyncMock()
        embeddings = AsyncMock()
        embeddings.embed_text = AsyncMock(return_value=[0.1] * 1024)
        ledger = TraceLedger()
        return ToolArtifactStore(graph, vector, embeddings, ledger, dataset_name="test_ds"), graph, vector, embeddings, ledger

    async def test_store_artifact(self, monkeypatch, mock_add_data_points, mock_cognee):
        store, graph, vector, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.artifacts.store.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.artifacts.store.cognee", mock_cognee)
        art = make_tool_artifact()
        result = await store.store_artifact(art)
        assert result.content_hash is not None

    async def test_search_artifacts(self, monkeypatch, mock_add_data_points, mock_cognee):
        store, graph, vector, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.artifacts.store.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.artifacts.store.cognee", mock_cognee)
        graph.query_cypher = AsyncMock(return_value=[])
        results = await store.search_artifacts("test")
        assert results == []

    async def test_get_by_hash_not_found(self):
        store, graph, _, _, _ = self._make()
        graph.query_cypher = AsyncMock(return_value=[])
        from elephantbroker.schemas.artifact import ArtifactHash
        result = await store.get_by_hash(ArtifactHash(value="abc123"))
        assert result is None

    async def test_store_emits_trace(self, monkeypatch, mock_add_data_points, mock_cognee):
        store, graph, vector, _, ledger = self._make()
        monkeypatch.setattr("elephantbroker.runtime.artifacts.store.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.artifacts.store.cognee", mock_cognee)
        await store.store_artifact(make_tool_artifact())
        from elephantbroker.schemas.trace import TraceQuery
        events = await ledger.query_trace(TraceQuery())
        assert len(events) == 1

    async def test_store_computes_hash(self, monkeypatch, mock_add_data_points, mock_cognee):
        store, graph, vector, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.artifacts.store.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.artifacts.store.cognee", mock_cognee)
        art = make_tool_artifact(content="hello world")
        result = await store.store_artifact(art)
        assert result.content_hash.algorithm == "sha256"

    async def test_store_calls_add_data_points(self, monkeypatch, mock_add_data_points, mock_cognee):
        """store_artifact() calls add_data_points with ArtifactDataPoint."""
        store, graph, vector, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.artifacts.store.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.artifacts.store.cognee", mock_cognee)
        art = make_tool_artifact()
        await store.store_artifact(art)
        assert len(mock_add_data_points.calls) == 1
        dp = mock_add_data_points.calls[0]["data_points"][0]
        assert dp.eb_id == str(art.artifact_id)

    async def test_store_calls_cognee_add(self, monkeypatch, mock_add_data_points, mock_cognee):
        """store_artifact() calls cognee.add() with summary or content[:500]."""
        store, graph, vector, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.artifacts.store.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.artifacts.store.cognee", mock_cognee)
        art = make_tool_artifact(summary="Test summary")
        await store.store_artifact(art)
        mock_cognee.add.assert_called_once()
        text = mock_cognee.add.call_args[0][0]
        assert text == "Test summary"

    async def test_store_does_not_call_vector_index_embedding(self, monkeypatch, mock_add_data_points, mock_cognee):
        """store_artifact() no longer calls VectorAdapter write methods."""
        store, graph, vector, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.artifacts.store.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.artifacts.store.cognee", mock_cognee)
        await store.store_artifact(make_tool_artifact())
        assert not hasattr(vector, 'index_embedding') or not vector.index_embedding.called
        assert not hasattr(vector, 'ensure_collection') or not vector.ensure_collection.called

    async def test_search_artifacts_hybrid_calls_cognee_search(self, monkeypatch, mock_add_data_points, mock_cognee):
        """search_artifacts() calls cognee.search(GRAPH_COMPLETION)."""
        store, graph, vector, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.artifacts.store.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.artifacts.store.cognee", mock_cognee)
        graph.query_cypher = AsyncMock(return_value=[])
        await store.search_artifacts("test")
        mock_cognee.search.assert_called_once()

    async def test_search_artifacts_returns_structural(self, monkeypatch, mock_add_data_points, mock_cognee):
        store, graph, vector, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.artifacts.store.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.artifacts.store.cognee", mock_cognee)
        art = make_tool_artifact()
        dp = ArtifactDataPoint.from_schema(art)
        props = {
            "eb_id": dp.eb_id, "tool_name": dp.tool_name, "summary": dp.summary,
            "content": dp.content, "eb_created_at": dp.eb_created_at,
            "token_estimate": dp.token_estimate, "tags": dp.tags,
        }
        graph.query_cypher = AsyncMock(return_value=[{"props": props}])
        results = await store.search_artifacts("test")
        assert len(results) == 1
        assert results[0].tool_name == art.tool_name

    async def test_get_by_hash_returns_match(self, monkeypatch, mock_add_data_points, mock_cognee):
        import hashlib

        store, graph, _, _, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.artifacts.store.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.artifacts.store.cognee", mock_cognee)
        content = "hello world"
        digest = hashlib.sha256(content.encode()).hexdigest()
        art = make_tool_artifact(content=content)
        dp = ArtifactDataPoint.from_schema(art)
        props = {
            "eb_id": dp.eb_id, "tool_name": dp.tool_name, "summary": dp.summary,
            "content": content, "eb_created_at": dp.eb_created_at,
            "token_estimate": dp.token_estimate, "tags": dp.tags,
        }
        graph.query_cypher = AsyncMock(return_value=[{"props": props}])
        from elephantbroker.schemas.artifact import ArtifactHash
        result = await store.get_by_hash(ArtifactHash(value=digest))
        assert result is not None
        assert result.content == content


class TestArtifactStoreABCConformance:
    def test_search_artifacts_abc_signature_matches_concrete(self):
        """H5: ABC and concrete search_artifacts must accept the same kwargs."""
        abc_sig = inspect.signature(IToolArtifactStore.search_artifacts)
        concrete_sig = inspect.signature(ToolArtifactStore.search_artifacts)
        abc_params = set(abc_sig.parameters.keys())
        concrete_params = set(concrete_sig.parameters.keys())
        assert abc_params == concrete_params, (
            f"ABC/concrete signature drift: "
            f"ABC-only={abc_params - concrete_params}, "
            f"concrete-only={concrete_params - abc_params}"
        )
