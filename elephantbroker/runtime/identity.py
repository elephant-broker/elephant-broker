"""Gateway identity utilities — deterministic agent UUIDs."""
from __future__ import annotations

import uuid

# Fixed namespace for agent_key → UUID v5 derivation.
# Same key always produces the same UUID across restarts.
EB_AGENT_NAMESPACE = uuid.UUID("e1e9b4a0-7c3d-4f8e-9a2b-1d5f6e8c0a3b")


def deterministic_uuid_from(agent_key: str) -> uuid.UUID:
    """Derive a stable UUID v5 from an agent_key string.

    >>> deterministic_uuid_from("gw-prod:main") == deterministic_uuid_from("gw-prod:main")
    True
    """
    return uuid.uuid5(EB_AGENT_NAMESPACE, agent_key)
