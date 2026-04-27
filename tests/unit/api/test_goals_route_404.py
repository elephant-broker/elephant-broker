"""R2-P10 / #1188 RESOLVED — PUT /goals/{goal_id} returns 404 (not 500)
when the goal is missing.

Pre-fix: the route handler called ``manager.update_goal_status()`` and
let any ``KeyError`` propagate. The runtime-layer KeyError ("Goal not
found") hit the generic Exception branch in
``error_handler_middleware`` and surfaced as HTTP 500 — confused both
API consumers (looks like a server bug) and ops dashboards (5xx
alerts).

Post-fix: the route catches ``KeyError`` and raises
``HTTPException(404)``. The runtime convention split (mutation methods
raise KeyError, collection getters return empty container) stays
unchanged — this fix bridges the runtime contract to the right HTTP
status code at the route boundary.

Companion pin: ``tests/unit/runtime/goals/test_get_hierarchy_inconsistency.py``
documents the runtime-layer convention split (G12) is **correct
alignment**, not inconsistency. This test pins the route bridge.
"""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock

import pytest


@pytest.mark.asyncio
async def test_update_goal_route_returns_404_for_missing_goal(client, container):
    """G_R2P10 (R2-P10): PUT /goals/<missing-uuid> returns HTTP 404
    (not 500) when the goal is not found in the graph.

    Setup: mock the goal_manager to raise the runtime-convention
    ``KeyError`` (matches what GoalManager.update_goal_status does
    when ``graph.get_entity`` returns None).

    Expected: route catches the KeyError and surfaces as 404 with a
    ``"Goal not found"`` detail message.
    """
    missing_id = str(uuid.uuid4())
    container.goal_manager.update_goal_status = AsyncMock(
        side_effect=KeyError(f"Goal not found: {missing_id}")
    )

    r = await client.put(
        f"/goals/{missing_id}",
        json={"status": "completed"},
    )
    assert r.status_code == 404, (
        f"Expected 404 for missing goal; got {r.status_code}. "
        "Pre-R2-P10 this was 500 because the runtime KeyError fell "
        "through to the generic Exception handler."
    )
    body = r.json()
    assert "Goal not found" in body.get("detail", "")


@pytest.mark.asyncio
async def test_update_goal_route_passes_through_other_errors(client, container):
    """G_R2P10-bis (R2-P10): the new try/except catches **only**
    ``KeyError`` — other runtime exceptions (e.g., ``ValueError`` from
    schema validation, ``ConnectionError`` from the graph driver)
    still propagate to the generic 500 handler. Pins the narrow scope
    of the bridge so a future broadening accidentally swallowing
    other errors surfaces here.
    """
    target_id = str(uuid.uuid4())
    container.goal_manager.update_goal_status = AsyncMock(
        side_effect=ConnectionError("neo4j down")
    )
    r = await client.put(
        f"/goals/{target_id}",
        json={"status": "completed"},
    )
    # ConnectionError → generic Exception handler → 500.
    assert r.status_code == 500
