"""TF-FN-019 G12 — GoalManager.get_goal_hierarchy and update_goal_status
follow two distinct EB conventions for "entity not found" (#1188 —
R2-P10: documented as **correct alignment**, not inconsistency).

Researcher's R2-P10 audit (option c) surveyed every read/mutate site
in the EB runtime and confirmed two distinct conventions:

* **Collection-getter convention** — read-side methods that return a
  container/list shape (``GoalHierarchy``, ``list[FactAssertion]``,
  ``dict[str, ...]``) return an empty instance for "not found" rather
  than raising. Callers iterate the result without branching on a
  sentinel.

* **Mutation-method convention** — write-side methods raise
  ``KeyError`` (or ``PermissionError``) for the same condition so
  callers cannot silently fail to mutate. The route layer translates
  to HTTP 404.

``get_goal_hierarchy`` correctly follows the first convention;
``update_goal_status`` correctly follows the second. The two
behaviors are NOT inconsistent — they're aligned with the two
conventions. The actual #1188 bug was at the **route layer**:
``api/routes/goals.py update_goal`` did not catch the runtime
``KeyError``, which surfaced as HTTP 500 instead of 404. R2-P10
fixed the route bridge and added the pin in
``test_goals_route_404.py``.

This test pins the runtime contract: **both behaviors are intended**
and a future "harmonization" attempt that aligns them on a single
shape (e.g., make ``get_goal_hierarchy`` raise too) would force a
broader audit of the entire EB runtime. The pin shape is unchanged
from the pre-R2-P10 version; only the docstring is updated to
reflect the convention-alignment interpretation.
"""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock

import pytest

from elephantbroker.runtime.goals.manager import GoalManager
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.schemas.goal import GoalStatus


async def test_get_goal_hierarchy_returns_empty_for_missing_root_but_update_status_raises():
    """G12 (#1188 — R2-P10): the same "goal not found" condition
    yields two different shapes per EB convention:

    * Collection-getter (``get_goal_hierarchy``) → empty container.
    * Mutation-method (``update_goal_status``) → ``KeyError``.

    This test pins both behaviors so a future "harmonization" that
    aligns them on a single shape forces an audit of the entire
    EB runtime convention. The route layer (api/routes/goals.py)
    bridges the mutation-method KeyError to HTTP 404 — see
    test_goals_route_404.py for that pin.
    """
    graph = AsyncMock()
    graph.get_entity = AsyncMock(return_value=None)  # missing in both cases
    graph.query_cypher = AsyncMock(return_value=[])
    manager = GoalManager(graph, TraceLedger(), dataset_name="t")

    missing_id = uuid.uuid4()

    # Path A: collection-getter convention → empty container, no exception.
    hierarchy = await manager.get_goal_hierarchy(missing_id)
    assert hierarchy.root_goals == []
    assert hierarchy.children == {}

    # Path B: mutation-method convention → KeyError on the same condition.
    with pytest.raises(KeyError, match="Goal not found"):
        await manager.update_goal_status(missing_id, GoalStatus.COMPLETED)
