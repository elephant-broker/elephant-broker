"""Tool artifact store — graph + vector storage for tool outputs."""
from __future__ import annotations

import hashlib

import cognee
from cognee.modules.search.types import SearchType
from cognee.tasks.storage import add_data_points

from elephantbroker.runtime.adapters.cognee.datapoints import ArtifactDataPoint
from elephantbroker.runtime.adapters.cognee.embeddings import EmbeddingService
from elephantbroker.runtime.adapters.cognee.graph import GraphAdapter
from elephantbroker.runtime.adapters.cognee.vector import VectorAdapter
from elephantbroker.runtime.graph_utils import clean_graph_props
from elephantbroker.runtime.interfaces.artifact_store import IToolArtifactStore
from elephantbroker.runtime.interfaces.trace_ledger import ITraceLedger
from elephantbroker.schemas.artifact import ArtifactHash, ToolArtifact
from elephantbroker.schemas.trace import TraceEvent, TraceEventType

_ARTIFACTS_COLLECTION = "ArtifactDataPoint_summary"


class ToolArtifactStore(IToolArtifactStore):

    def __init__(
        self,
        graph: GraphAdapter,
        vector: VectorAdapter,
        embeddings: EmbeddingService,
        trace_ledger: ITraceLedger,
        dataset_name: str = "elephantbroker",
        gateway_id: str = "",
    ) -> None:
        self._graph = graph
        self._vector = vector
        self._embeddings = embeddings
        self._trace = trace_ledger
        self._dataset_name = dataset_name
        self._gateway_id = gateway_id

    async def store_artifact(self, artifact: ToolArtifact) -> ToolArtifact:
        artifact.gateway_id = artifact.gateway_id or self._gateway_id
        # Compute content hash if not set
        if artifact.content_hash is None:
            digest = hashlib.sha256(artifact.content.encode()).hexdigest()
            artifact.content_hash = ArtifactHash(value=digest)

        dp = ArtifactDataPoint.from_schema(artifact)
        await add_data_points([dp])  # CREATE — graph + vector in one call
        text_for_cognee = artifact.summary or artifact.content[:500]
        await cognee.add(text_for_cognee, dataset_name=self._dataset_name)

        await self._trace.append_event(
            TraceEvent(
                event_type=TraceEventType.ARTIFACT_CREATED,
                artifact_ids=[artifact.artifact_id],
                payload={"action": "store_artifact", "tool_name": artifact.tool_name},
            )
        )
        return artifact

    async def search_artifacts(self, query: str, max_results: int = 10) -> list[ToolArtifact]:
        results: dict[str, ToolArtifact] = {}

        # Stage 1: Semantic — GRAPH_COMPLETION discovers ArtifactDataPoints
        try:
            cognee_hits = await cognee.search(
                query_type=SearchType.GRAPH_COMPLETION,
                query_text=query,
                only_context=True,
                datasets=[self._dataset_name],
            )
            for artifact in self._parse_graph_completion_to_artifacts(cognee_hits):
                results[str(artifact.artifact_id)] = artifact
        except Exception:
            pass

        # Stage 2: Structural fallback — scan by label
        cypher = "MATCH (a:ArtifactDataPoint) WHERE a.gateway_id = $gateway_id RETURN properties(a) AS props LIMIT $limit"
        records = await self._graph.query_cypher(cypher, {"limit": max_results, "gateway_id": self._gateway_id})
        for rec in records:
            props = clean_graph_props(rec["props"])
            try:
                dp = ArtifactDataPoint(**props)
                art = dp.to_schema()
                if str(art.artifact_id) not in results:
                    results[str(art.artifact_id)] = art
            except Exception:
                continue

        return list(results.values())[:max_results]

    def _parse_graph_completion_to_artifacts(self, cognee_hits: list) -> list[ToolArtifact]:
        """Extract ToolArtifacts from GRAPH_COMPLETION results."""
        artifacts: list[ToolArtifact] = []
        if not cognee_hits:
            return artifacts
        for item in cognee_hits:
            try:
                if isinstance(item, dict):
                    eb_id = item.get("eb_id") or item.get("id")
                    if eb_id:
                        props = clean_graph_props(item)
                        dp = ArtifactDataPoint(**props)
                        artifacts.append(dp.to_schema())
            except Exception:
                continue
        return artifacts

    async def get_by_hash(self, content_hash: ArtifactHash) -> ToolArtifact | None:
        cypher = (
            "MATCH (a:ArtifactDataPoint) WHERE a.gateway_id = $gateway_id "
            "RETURN properties(a) AS props"
        )
        records = await self._graph.query_cypher(cypher, {"gateway_id": self._gateway_id})
        for rec in records:
            props = rec["props"]
            # Check if the content hash matches by hashing stored content
            content = props.get("content", "")
            digest = hashlib.sha256(content.encode()).hexdigest()
            if digest == content_hash.value:
                clean_props = clean_graph_props(props)
                dp = ArtifactDataPoint(**clean_props)
                return dp.to_schema()
        return None
