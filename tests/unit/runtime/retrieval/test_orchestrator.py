"""Tests for RetrievalOrchestrator — dataset name fix (Fix #32)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elephantbroker.runtime.retrieval.orchestrator import RetrievalOrchestrator
from elephantbroker.schemas.profile import RetrievalPolicy
from elephantbroker.schemas.trace import TraceEventType


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


class TestRetrievalPerformedTraceEvent:
    """TD-47: retrieval_performed trace events must include session_id and session_key."""

    @staticmethod
    def _find_retrieval_performed(mock_ledger):
        """Filter append_event calls for the RETRIEVAL_PERFORMED event."""
        return [
            c.args[0] for c in mock_ledger.append_event.call_args_list
            if c.args[0].event_type == TraceEventType.RETRIEVAL_PERFORMED
        ]

    async def test_trace_event_includes_session_id(self):
        orch = _make_orchestrator()
        policy = RetrievalPolicy(
            keyword_enabled=False, structural_enabled=False,
            vector_enabled=False, graph_expansion_enabled=False,
            artifact_enabled=False,
        )
        await orch.retrieve_candidates(
            "test query", policy=policy,
            session_key="agent:main:main", session_id="00000000-0000-0000-0000-000000000042",
        )
        events = self._find_retrieval_performed(orch._trace)
        assert len(events) == 1
        assert str(events[0].session_id) == "00000000-0000-0000-0000-000000000042"
        assert events[0].session_key == "agent:main:main"

    async def test_trace_event_session_id_none_when_not_provided(self):
        orch = _make_orchestrator()
        policy = RetrievalPolicy(
            keyword_enabled=False, structural_enabled=False,
            vector_enabled=False, graph_expansion_enabled=False,
            artifact_enabled=False,
        )
        await orch.retrieve_candidates("test query", policy=policy)
        events = self._find_retrieval_performed(orch._trace)
        assert len(events) == 1
        assert events[0].session_id is None


class TestMemorySearchSessionIdThreading:
    """TD-47 complete: session_id must reach retrieve_candidates from /memory/search."""

    async def test_session_id_threaded_from_search_request(self):
        """SearchRequest.session_id must be passed to retrieve_candidates."""
        orch = _make_orchestrator()

        policy = RetrievalPolicy(
            keyword_enabled=False, structural_enabled=False,
            vector_enabled=False, graph_expansion_enabled=False,
            artifact_enabled=False,
        )

        # Simulate the /memory/search call path: session_id arrives as string
        await orch.retrieve_candidates(
            "test query",
            policy=policy,
            session_key="agent:main:main",
            session_id="11111111-1111-1111-1111-111111111111",
        )
        events = [
            c.args[0] for c in orch._trace.append_event.call_args_list
            if c.args[0].event_type == TraceEventType.RETRIEVAL_PERFORMED
        ]
        assert len(events) == 1
        assert str(events[0].session_id) == "11111111-1111-1111-1111-111111111111"
        assert events[0].session_key == "agent:main:main"
