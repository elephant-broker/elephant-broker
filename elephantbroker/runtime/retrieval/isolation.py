"""SUBAGENT_INHERIT isolation — walks the parent chain for effective session keys."""
from __future__ import annotations

from elephantbroker.schemas.profile import IsolationScope


async def resolve_effective_session_keys(
    session_key: str,
    scope: IsolationScope,
    redis,
    redis_keys,
    max_depth: int = 5,
) -> list[str]:
    """Return the list of session keys visible to this session.

    For SUBAGENT_INHERIT: walks up the parent chain via Redis,
    returning ``[session_key, parent1, parent2, ...]``.
    For all other scopes: returns ``[session_key]``.
    """
    if scope != IsolationScope.SUBAGENT_INHERIT:
        return [session_key]

    if redis is None or redis_keys is None:
        return [session_key]

    result = [session_key]
    seen = {session_key}
    current = session_key

    for _ in range(max_depth):
        try:
            parent = await redis.get(redis_keys.session_parent(current))
        except Exception:
            break
        if parent is None or parent in seen:
            break
        result.append(parent)
        seen.add(parent)
        current = parent

    return result
