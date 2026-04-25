"""R2-P7 / link-spam guard — ``GoalManager.set_goal`` rejects
cross-gateway parent goals (CHILD_OF) and cross-gateway owners
(OWNS_GOAL) with ``PermissionError`` (→ HTTP 403 via R2-P5
middleware).

D11 contract: GraphAdapter primitives are gateway-agnostic.
GoalManager.set_goal must validate the supplied
``parent_goal_id`` and ``owner_actor_ids`` before issuing the
``add_relation`` calls. Pre-R2-P7 a privileged caller in tenant A
could attach goals in tenant A as children of tenant B's goals
(or assign tenant B's actors as owners) — neither was rejected
at the runtime layer.
"""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elephantbroker.runtime.goals.manager import GoalManager
from elephantbroker.schemas.goal import GoalState, GoalStatus


def _make_goal(parent_goal_id=None, owner_actor_ids=None) -> GoalState:
    return GoalState(
        id=uuid.uuid4(),
        title="test goal",
        description="",
        success_criteria=[],
        status=GoalStatus.ACTIVE,
        parent_goal_id=parent_goal_id,
        owner_actor_ids=owner_actor_ids or [],
        gateway_id="gw-a",
    )


@pytest.mark.asyncio
async def test_set_goal_rejects_cross_gateway_parent_for_CHILD_OF():
    """G_LinkSpam (R2-P7, CHILD_OF): ``set_goal`` raises
    ``PermissionError`` when ``parent_goal_id`` resolves to a goal
    owned by a different gateway. ``add_relation`` for CHILD_OF is
    NEVER invoked.
    """
    parent_id = uuid.uuid4()
    graph = AsyncMock()
    # parent goal exists and belongs to gw-b (not the caller's gw-a)
    graph.get_entity = AsyncMock(return_value={"gateway_id": "gw-b", "eb_id": str(parent_id)})
    graph.add_relation = AsyncMock()

    trace = AsyncMock()
    trace.append_event = AsyncMock()

    manager = GoalManager(graph=graph, trace_ledger=trace, dataset_name="t", gateway_id="gw-a")
    goal = _make_goal(parent_goal_id=parent_id)

    with patch("elephantbroker.runtime.goals.manager.add_data_points", new=AsyncMock()), \
         patch("elephantbroker.runtime.goals.manager.cognee.add", new=AsyncMock()):
        with pytest.raises(PermissionError) as excinfo:
            await manager.set_goal(goal)

    assert "R2-P7" in str(excinfo.value)
    assert "gw-b" in str(excinfo.value)
    # CHILD_OF add_relation never called.
    graph.add_relation.assert_not_awaited()


@pytest.mark.asyncio
async def test_set_goal_rejects_cross_gateway_owner_for_OWNS_GOAL():
    """G_LinkSpam-bis (R2-P7, OWNS_GOAL): ``set_goal`` raises
    ``PermissionError`` when an owner_actor_id resolves to an actor
    owned by a different gateway — even though the OWNS_GOAL loop
    has its own try/except for runtime errors, ``PermissionError``
    is re-raised explicitly so cross-gateway link attempts surface
    as 403.
    """
    owner_id = uuid.uuid4()
    graph = AsyncMock()
    # owner actor exists and belongs to gw-b
    graph.get_entity = AsyncMock(return_value={"gateway_id": "gw-b", "eb_id": str(owner_id)})
    graph.add_relation = AsyncMock()

    trace = AsyncMock()
    trace.append_event = AsyncMock()

    manager = GoalManager(graph=graph, trace_ledger=trace, dataset_name="t", gateway_id="gw-a")
    goal = _make_goal(owner_actor_ids=[owner_id])

    with patch("elephantbroker.runtime.goals.manager.add_data_points", new=AsyncMock()), \
         patch("elephantbroker.runtime.goals.manager.cognee.add", new=AsyncMock()):
        with pytest.raises(PermissionError) as excinfo:
            await manager.set_goal(goal)

    assert "R2-P7" in str(excinfo.value)
    assert "gw-b" in str(excinfo.value)
    # OWNS_GOAL add_relation never called.
    graph.add_relation.assert_not_awaited()
