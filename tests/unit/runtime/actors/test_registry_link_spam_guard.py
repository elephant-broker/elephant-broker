"""R2-P7 / link-spam guard — ``ActorRegistry.register_actor`` rejects
cross-gateway team IDs (MEMBER_OF) with ``PermissionError`` (→
HTTP 403 via R2-P5 middleware).

D11 contract: GraphAdapter primitives are gateway-agnostic.
ActorRegistry.register_actor must validate ``actor.team_ids``
before creating MEMBER_OF edges. Pre-R2-P7 a privileged caller in
tenant A could register an actor as a member of tenant B's team —
the cross-gateway link went through silently.
"""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elephantbroker.runtime.actors.registry import ActorRegistry
from elephantbroker.schemas.actor import ActorRef, ActorType


def _make_actor(team_ids=None) -> ActorRef:
    return ActorRef(
        id=uuid.uuid4(),
        type=ActorType.WORKER_AGENT,
        display_name="test worker",
        handles=[],
        team_ids=team_ids or [],
        gateway_id="gw-a",
    )


@pytest.mark.asyncio
async def test_register_actor_rejects_cross_gateway_team_for_MEMBER_OF():
    """G_LinkSpam (R2-P7, MEMBER_OF): ``register_actor`` raises
    ``PermissionError`` when a team_id resolves to a team owned by a
    different gateway. ``add_relation`` for MEMBER_OF is NEVER
    invoked. Even though the MEMBER_OF loop has its own try/except
    for best-effort runtime errors, ``PermissionError`` is re-
    raised explicitly so cross-gateway link attempts surface as
    403, not silent skips.
    """
    team_id = uuid.uuid4()
    graph = AsyncMock()
    graph.get_entity = AsyncMock(return_value={"gateway_id": "gw-b", "eb_id": str(team_id)})
    graph.add_relation = AsyncMock()

    trace = AsyncMock()
    trace.append_event = AsyncMock()

    registry = ActorRegistry(graph=graph, trace_ledger=trace, dataset_name="t", gateway_id="gw-a")
    actor = _make_actor(team_ids=[team_id])

    with patch("elephantbroker.runtime.actors.registry.add_data_points", new=AsyncMock()), \
         patch("elephantbroker.runtime.actors.registry.cognee.add", new=AsyncMock()):
        with pytest.raises(PermissionError) as excinfo:
            await registry.register_actor(actor)

    assert "R2-P7" in str(excinfo.value)
    assert "gw-b" in str(excinfo.value)
    graph.add_relation.assert_not_awaited()
