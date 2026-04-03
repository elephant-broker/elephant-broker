"""Task: summarize a tool artifact, with LLM or truncation fallback."""
from __future__ import annotations

import logging
from typing import Any

from elephantbroker.runtime.observability import traced
from elephantbroker.schemas.artifact import ArtifactSummary, ToolArtifact

logger = logging.getLogger("elephantbroker.tasks.summarize_artifact")


@traced
async def summarize_artifact(
    artifact: ToolArtifact,
    llm_client: Any = None,
    config: Any = None,
) -> ArtifactSummary:
    """Generate a compact summary of a tool artifact.

    - If content < summarization_min_artifact_chars (default 500): truncate first 200 chars.
    - Otherwise: LLM call with summarization prompt.
    - No LLM client: fallback to truncation.
    """
    min_chars = 500
    if config is not None:
        min_chars = getattr(config, "summarization_min_artifact_chars", 500)

    content = artifact.content or ""

    if len(content) < min_chars or llm_client is None:
        # Truncation fallback
        summary_text = content[:200]
    else:
        # LLM summarization
        try:
            max_output = 200
            if config is not None:
                max_output = getattr(config, "summarization_max_output_tokens", 200)
            summary_text = await llm_client.complete(
                "You are a concise summarizer. Summarize the following tool output in 1-3 sentences. "
                "Focus on the key result, changes, or findings.",
                f"Tool: {artifact.tool_name}\n\nOutput:\n{content}",
                max_tokens=max_output,
                temperature=0.0,
            )
        except Exception as exc:
            logger.warning("LLM summarization failed: %s", exc)
            summary_text = content[:200]

    return ArtifactSummary(
        artifact_id=artifact.artifact_id,
        tool_name=artifact.tool_name,
        summary=summary_text,
        token_estimate=artifact.token_estimate,
        created_at=artifact.created_at,
    )
