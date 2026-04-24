"""TF-FN-019 G12 — GoalManager.get_goal_hierarchy and update_goal_status
disagree on the "entity not found" shape.

PROD #1188 pin. Two adjacent methods on ``GoalManager`` disagree on how
to handle a goal_id that does not resolve in Neo4j:

* ``get_goal_hierarchy(root_goal_id)`` at ``goals/manager.py:81-84``
  returns an empty ``GoalHierarchy()`` silently when the root entity is
  missing (TF-FN-002 G10 already pins this).

* ``update_goal_status(goal_id, ...)`` at ``goals/manager.py:107-111``
  raises ``KeyError(f"Goal not found: {goal_id}")`` when the entity is
  missing.

Both are "read a goal by id and act on it" flows. Callers handling the
pair have to branch on the return shape (empty hierarchy vs. exception)
even though the underlying condition is identical.

Pin: document the inconsistency so a future harmonization — either
align both on empty return, OR align both on raise — will flip this
test and force an explicit unification.
"""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock

import pytest

from elephantbroker.runtime.goals.manager import GoalManager
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.schemas.goal import GoalStatus


async def test_get_goal_hierarchy_returns_empty_for_missing_root_but_update_status_raises():
    """G12 (#1188): the same "goal not found" condition yields two
    different shapes depending on which GoalManager method is called.
    """
    graph = AsyncMock()
    graph.get_entity = AsyncMock(return_value=None)  # missing in both cases
    graph.query_cypher = AsyncMock(return_value=[])
    manager = GoalManager(graph, TraceLedger(), dataset_name="t")

    missing_id = uuid.uuid4()

    # Path A: get_goal_hierarchy returns empty, no exception.
    hierarchy = await manager.get_goal_hierarchy(missing_id)
    assert hierarchy.root_goals == []
    assert hierarchy.children == {}

    # Path B: update_goal_status raises KeyError with the same condition.
    with pytest.raises(KeyError, match="Goal not found"):
        await manager.update_goal_status(missing_id, GoalStatus.COMPLETED)
