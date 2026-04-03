"""Artifact ingest pipeline -- stores tool outputs with summaries."""
from __future__ import annotations

import hashlib
import logging

from elephantbroker.runtime.adapters.cognee.tasks.summarize_artifact import summarize_artifact
from elephantbroker.runtime.interfaces.trace_ledger import ITraceLedger
from elephantbroker.runtime.observability import traced
from elephantbroker.schemas.artifact import ArtifactHash, ToolArtifact
from elephantbroker.schemas.pipeline import ArtifactIngestResult, ArtifactInput
from elephantbroker.schemas.trace import TraceEvent, TraceEventType

logger = logging.getLogger("elephantbroker.pipelines.artifact_ingest")


class ArtifactIngestPipeline:
    """Stores tool artifacts with deduplication, summarization, and tracing."""

    def __init__(
        self, artifact_store, memory_facade, llm_client, trace_ledger: ITraceLedger,
        config=None, gateway_id: str = "local",
    ):
        self._store = artifact_store
        self._facade = memory_facade
        self._llm = llm_client
        self._trace = trace_ledger
        self._config = config
        self._gateway_id = gateway_id
        self._seen_hashes: set[str] = set()

    @traced
    async def run(self, input: ArtifactInput) -> ArtifactIngestResult:
        """Run the artifact ingest pipeline."""
        # Hash includes tool_name + sorted args + output per plan §4.3
        hash_input = input.tool_name + str(sorted(input.tool_args.items())) + input.tool_output
        content_hash = hashlib.sha256(hash_input.encode()).hexdigest()

        # Dedup by hash (in-memory + database)
        if content_hash in self._seen_hashes:
            return ArtifactIngestResult(is_duplicate=True)
        try:
            artifact_hash = ArtifactHash(value=content_hash)
            existing = await self._store.get_by_hash(artifact_hash)
            if existing:
                self._seen_hashes.add(content_hash)
                return ArtifactIngestResult(is_duplicate=True)
        except Exception as exc:
            logger.debug("DB dedup check failed (will proceed): %s", exc)
        self._seen_hashes.add(content_hash)

        # Create artifact
        gw = getattr(input, "gateway_id", "") or self._gateway_id
        artifact = ToolArtifact(
            tool_name=input.tool_name,
            content=input.tool_output,
            session_id=input.session_id,
            actor_id=input.actor_id,
            goal_id=input.goal_id,
            gateway_id=gw,
        )

        # Store raw
        try:
            await self._store.store_artifact(artifact)
        except Exception as exc:
            logger.warning("Failed to store artifact: %s", exc)

        # Summarize
        summary = await summarize_artifact(artifact, self._llm, self._config)

        trace_event = TraceEvent(
            event_type=TraceEventType.ARTIFACT_CREATED,
            payload={
                "artifact_id": str(artifact.artifact_id),
                "tool_name": input.tool_name,
            },
        )
        await self._trace.append_event(trace_event)

        return ArtifactIngestResult(
            artifact=artifact,
            summary=summary,
            is_duplicate=False,
            trace_event_id=trace_event.id,
        )
