"""Tests for RetrievalOrchestrator — dataset name fix (Fix #32)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elephantbroker.runtime.retrieval.orchestrator import RetrievalOrchestrator
from elephantbroker.schemas.profile import RetrievalPolicy


def _make_orchestrator(dataset_name: str = "gw__elephantbroker") -> RetrievalOrchestrator:
    """Build a RetrievalOrchestrator with mocked adapters."""
    return RetrievalOrchestrator(
        vector=AsyncMock(),
        graph=AsyncMock(),
        embeddings=AsyncMock(),
        trace_ledger=AsyncMock(),
        dataset_name=dataset_name,
        gateway_id="test-gw",
    )


class TestDatasetNameFix:
    """Fix #32: Cognee search must use dataset_name, not session_key."""

    async def test_keyword_search_uses_dataset_name_not_session_key(self):
        """When session_key is provided, keyword search must still use dataset_name."""
        orch = _make_orchestrator(dataset_name="gw__elephantbroker")

        # Only enable keyword search to isolate the test
        policy = RetrievalPolicy(
            keyword_enabled=True,
            structural_enabled=False,
            vector_enabled=False,
            graph_expansion_enabled=False,
            artifact_enabled=False,
        )

        with patch.object(orch, "get_keyword_hits", new_callable=AsyncMock, return_value=[]) as mock_kw:
            await orch.retrieve_candidates(
                "test query",
                policy=policy,
                session_key="agent:main:main",
            )
            mock_kw.assert_called_once()
            # Second arg is the dataset name — must be dataset_name, NOT session_key
            call_args = mock_kw.call_args[0]
            assert call_args[1] == "gw__elephantbroker"
            assert call_args[1] != "agent:main:main"

    async def test_semantic_search_uses_dataset_name(self):
        """Semantic search source also uses dataset_name."""
        orch = _make_orchestrator(dataset_name="gw__elephantbroker")

        policy = RetrievalPolicy(
            keyword_enabled=False,
            structural_enabled=False,
            vector_enabled=True,
            graph_expansion_enabled=False,
            artifact_enabled=False,
        )

        with patch.object(orch, "get_semantic_hits_cognee", new_callable=AsyncMock, return_value=[]) as mock_sem:
            await orch.retrieve_candidates(
                "test query",
                policy=policy,
                session_key="agent:main:main",
            )
            mock_sem.assert_called_once()
            call_args = mock_sem.call_args[0]
            assert call_args[1] == "gw__elephantbroker"

    async def test_graph_search_uses_dataset_name(self):
        """Graph expansion source uses dataset_name."""
        orch = _make_orchestrator(dataset_name="gw__elephantbroker")

        policy = RetrievalPolicy(
            keyword_enabled=False,
            structural_enabled=False,
            vector_enabled=False,
            graph_expansion_enabled=True,
            artifact_enabled=False,
        )

        with patch.object(orch, "get_graph_neighbors", new_callable=AsyncMock, return_value=[]) as mock_graph:
            await orch.retrieve_candidates(
                "test query",
                policy=policy,
                session_key="agent:main:main",
            )
            mock_graph.assert_called_once()
            call_args = mock_graph.call_args[0]
            assert call_args[1] == "gw__elephantbroker"
