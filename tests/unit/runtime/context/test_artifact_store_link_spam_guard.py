"""R2-P7 / link-spam guard — ``SessionArtifactStore.promote_to_persistent``
rejects cross-gateway goals (SERVES_GOAL) with ``PermissionError``
(→ HTTP 403 via R2-P5 middleware).

D11 contract: GraphAdapter primitives are gateway-agnostic.
Promotion to persistent storage creates 3 graph edges
(CREATED_BY, SERVES_GOAL, OWNED_BY); only SERVES_GOAL accepts a
caller-supplied target (the goal_id stamped on the artifact).
This test pins the validation: a ``goal_id`` belonging to a
different gateway results in a rejected promotion, not a silent
cross-tenant link.
"""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from elephantbroker.runtime.context.session_artifact_store import SessionArtifactStore
from elephantbroker.schemas.artifact import ToolArtifact
from elephantbroker.schemas.trace import TraceEventType


@pytest.mark.asyncio
async def test_promote_to_persistent_rejects_cross_gateway_goal_for_SERVES_GOAL():
    """G_LinkSpam (R2-P7, SERVES_GOAL): when the artifact's goal_id
    resolves to a goal owned by a different gateway, the SERVES_GOAL
    edge creation raises ``PermissionError`` and is re-raised
    explicitly out of the existing best-effort try/except so the
    cross-gateway link surfaces as 403.

    Side note: CREATED_BY and OWNED_BY use the agent_actor_id
    derived deterministically from the gateway_id itself, so they
    can never be cross-gateway by construction — we don't pin them
    here, only the goal-edge surface that's actually attackable.
    """
    goal_id = uuid.uuid4()
    artifact_id = uuid.uuid4()

    # Configure the underlying graph adapter to claim the goal lives
    # on a different gateway.
    graph = AsyncMock()
    graph.get_entity = AsyncMock(return_value={"gateway_id": "gw-b", "eb_id": str(goal_id)})
    graph.add_relation = AsyncMock()

    # Inner persistent ``ToolArtifactStore`` returns a result with a
    # cross-gateway goal_id; the SessionArtifactStore reads it from
    # ``self._artifact_store`` and uses ``self._artifact_store._graph``.
    inner_store = MagicMock()
    inner_store._graph = graph
    inner_result = MagicMock()
    inner_result.artifact_id = artifact_id
    inner_result.goal_id = goal_id
    inner_store.store_artifact = AsyncMock(return_value=inner_result)

    # Redis stub returning a session artifact JSON for the read path.
    session_artifact_json = (
        f'{{"artifact_id": "{artifact_id}", "session_key": "agent:m:m", '
        f'"session_id": "{uuid.uuid4()}", "tool_name": "x", "kind": "result", '
        f'"params": {{}}, "content": "irrelevant", "content_hash": "h", '
        f'"goal_id": "{goal_id}", "actor_id": null, "gateway_id": "gw-a", '
        f'"size_bytes": 0, "produced_at": "2026-04-25T00:00:00Z", '
        f'"injected_count": 0, "last_searched_at": null}}'
    )
    redis = AsyncMock()
    redis.hget = AsyncMock(return_value=session_artifact_json)

    keys = MagicMock()
    keys.session_artifacts = MagicMock(return_value="session:artifacts:test")

    config = MagicMock()
    config.consolidation_min_retention_seconds = 3600

    store = SessionArtifactStore(
        redis=redis,
        config=config,
        redis_keys=keys,
        artifact_store=inner_store,
        trace_ledger=AsyncMock(),
        gateway_id="gw-a",
    )

    with pytest.raises(PermissionError) as excinfo:
        await store.promote_to_persistent("agent:m:m", str(uuid.uuid4()), str(artifact_id))

    assert "R2-P7" in str(excinfo.value)
    assert "gw-b" in str(excinfo.value)
    # SERVES_GOAL edge never created — guard fires before add_relation.
    # (CREATED_BY may have been created since it precedes SERVES_GOAL
    # in the promote flow; that edge uses an agent_actor_id derived
    # from gw-a deterministically, so it's same-tenant by construction.)
    serves_goal_calls = [
        c for c in graph.add_relation.await_args_list
        if c.args and c.args[2] == "SERVES_GOAL"
    ]
    assert serves_goal_calls == []
    # R2-001: AUTHORITY_CHECK_FAILED trace emitted
    trace_ledger = store._trace
    trace_ledger.append_event.assert_awaited()
    event = trace_ledger.append_event.call_args[0][0]
    assert event.event_type == TraceEventType.AUTHORITY_CHECK_FAILED
    assert event.payload["action"] == "promote_artifact"
