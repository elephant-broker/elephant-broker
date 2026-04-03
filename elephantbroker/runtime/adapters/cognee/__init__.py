"""Cognee adapter layer — storage/retrieval primitives for ElephantBroker runtime."""
from __future__ import annotations

from elephantbroker.runtime.adapters.cognee.config import configure_cognee
from elephantbroker.runtime.adapters.cognee.datasets import DatasetManager
from elephantbroker.runtime.adapters.cognee.embeddings import EmbeddingService
from elephantbroker.runtime.adapters.cognee.graph import GraphAdapter, SubgraphResult
from elephantbroker.runtime.adapters.cognee.pipeline_runner import PipelineResult, PipelineRunner
from elephantbroker.runtime.adapters.cognee.vector import VectorAdapter, VectorSearchResult

__all__ = [
    "configure_cognee",
    "DatasetManager",
    "EmbeddingService",
    "GraphAdapter",
    "PipelineResult",
    "PipelineRunner",
    "SubgraphResult",
    "VectorAdapter",
    "VectorSearchResult",
]
