"""Integration tests for ToolArtifactStore with real Neo4j + Qdrant."""
from __future__ import annotations

import pytest

from tests.fixtures.factories import make_tool_artifact


@pytest.mark.integration
class TestToolArtifactStoreIntegration:
    async def test_store_and_search_artifact(self, artifact_store):
        art = make_tool_artifact(tool_name="grep", content="search results for foobar")
        await artifact_store.store_artifact(art)
        results = await artifact_store.search_artifacts("grep search results")
        assert len(results) >= 1

    async def test_get_by_hash_round_trip(self, artifact_store):
        art = make_tool_artifact(content="unique content for hash test")
        stored = await artifact_store.store_artifact(art)
        assert stored.content_hash is not None
        found = await artifact_store.get_by_hash(stored.content_hash)
        assert found is not None
        assert found.content == art.content
