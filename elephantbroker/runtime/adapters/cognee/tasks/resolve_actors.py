"""Task: resolve actor references in conversation messages — no LLM, pattern-based."""
from __future__ import annotations

import logging
import re

from elephantbroker.runtime.observability import traced
from elephantbroker.schemas.actor import ActorRef

logger = logging.getLogger("elephantbroker.tasks.resolve_actors")

# Pattern to match @handles in message content
_HANDLE_RE = re.compile(r"@(\w+)")


@traced
async def resolve_actors(
    messages: list[dict],
    known: list[ActorRef],
    actor_registry=None,
    message_provider: str | None = None,
    peer_id: str | None = None,
) -> list[ActorRef]:
    """Resolve actor mentions in messages against known actors.

    Resolution strategies (no LLM):
    0. Platform-qualified handle lookup (Phase 8): {message_provider}:{peer_id}
    1. Exact handle match: @username matches actor.handles
    2. Case-insensitive display_name substring match

    Returns deduplicated list of matched ActorRef objects.
    """
    matched_ids: set = set()
    matched: list[ActorRef] = []

    # Strategy 0 (Phase 8): Platform-qualified handle lookup
    if actor_registry and message_provider and peer_id:
        handle = f"{message_provider}:{peer_id}"
        if hasattr(actor_registry, "resolve_by_handle"):
            try:
                actor = await actor_registry.resolve_by_handle(handle)
                if actor and actor.id not in matched_ids:
                    matched_ids.add(actor.id)
                    matched.append(actor)
                    logger.debug("Resolved actor via platform handle: %s → %s", handle, actor.display_name)
            except Exception:
                logger.debug("Platform handle lookup failed for %s", handle)

    if not messages or not known:
        return matched

    # Collect all text content from messages
    all_text = " ".join(msg.get("content", "") for msg in messages)
    if not all_text.strip():
        return []

    # Extract @mentions
    mentions = set(_HANDLE_RE.findall(all_text))
    text_lower = all_text.lower()

    for actor in known:
        if actor.id in matched_ids:
            continue

        # Strategy 1: Exact handle match
        for handle in actor.handles:
            clean_handle = handle.lstrip("@")
            if clean_handle in mentions:
                matched_ids.add(actor.id)
                matched.append(actor)
                break

        if actor.id in matched_ids:
            continue

        # Strategy 2: Case-insensitive display name substring
        if actor.display_name and actor.display_name.lower() in text_lower:
            matched_ids.add(actor.id)
            matched.append(actor)

    logger.debug("Resolved %d actors from %d messages", len(matched), len(messages))
    return matched
