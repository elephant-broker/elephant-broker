"""R2-P7 / link-spam guard — admin team-membership routes reject
cross-gateway actor / team IDs with HTTP 403.

D11 contract: GraphAdapter primitives are gateway-agnostic. The
admin POST and DELETE on ``/admin/teams/{team_id}/members`` accept
caller-supplied ``actor_id`` and ``team_id``. Pre-R2-P7 a privileged
caller in tenant A could manipulate tenant B's MEMBER_OF edges by
guessing ID values. Post-R2-P7 the route invokes
``assert_same_gateway`` on both IDs before hitting the graph; a
mismatch raises ``PermissionError`` which the R2-P5 error-handler
middleware converts to HTTP 403.
"""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock

import pytest


_FOREIGN_ACTOR_ID = str(uuid.uuid4())
_FOREIGN_TEAM_ID = str(uuid.uuid4())
_LOCAL_ACTOR_ID = str(uuid.uuid4())
_LOCAL_TEAM_ID = str(uuid.uuid4())


def _enable_bootstrap(container):
    """Bootstrap mode skips authority checks so the link-spam guard
    is the only gating layer this test exercises."""
    container._bootstrap_mode = True
    container._bootstrap_checked = True


@pytest.mark.asyncio
async def test_add_team_member_rejects_cross_gateway_actor_with_403(client, container):
    """G_LinkSpam-POST (R2-P7): a POST that supplies an actor_id
    belonging to a different gateway returns 403. ``add_relation``
    is never invoked.
    """
    _enable_bootstrap(container)
    container.gateway_id = "gw-a"

    # Foreign actor: graph reports it lives on gw-b.
    async def fake_get_entity(entity_id, *, gateway_id=None):
        if entity_id == _FOREIGN_ACTOR_ID:
            return {"gateway_id": "gw-b", "eb_id": _FOREIGN_ACTOR_ID}
        # Same-gateway team for the team_id arg.
        return {"gateway_id": "gw-a", "eb_id": entity_id}

    container.graph.get_entity = AsyncMock(side_effect=fake_get_entity)
    container.graph.add_relation = AsyncMock()

    r = await client.post(
        f"/admin/teams/{_LOCAL_TEAM_ID}/members",
        json={"actor_id": _FOREIGN_ACTOR_ID},
        headers={"X-EB-Actor-Id": str(uuid.uuid4()), "X-EB-Gateway-ID": "gw-a"},
    )
    assert r.status_code == 403
    container.graph.add_relation.assert_not_awaited()


@pytest.mark.asyncio
async def test_remove_team_member_rejects_cross_gateway_team_with_403(client, container):
    """G_LinkSpam-DELETE (R2-P7): a DELETE on a team_id belonging to
    a different gateway returns 403. ``delete_relation`` /
    ``query_cypher`` is never invoked.
    """
    _enable_bootstrap(container)
    container.gateway_id = "gw-a"

    async def fake_get_entity(entity_id, *, gateway_id=None):
        if entity_id == _FOREIGN_TEAM_ID:
            return {"gateway_id": "gw-b", "eb_id": _FOREIGN_TEAM_ID}
        # Same-gateway actor.
        return {"gateway_id": "gw-a", "eb_id": entity_id}

    container.graph.get_entity = AsyncMock(side_effect=fake_get_entity)
    container.graph.delete_relation = AsyncMock()
    container.graph.query_cypher = AsyncMock(return_value=[])

    r = await client.delete(
        f"/admin/teams/{_FOREIGN_TEAM_ID}/members/{_LOCAL_ACTOR_ID}",
        headers={"X-EB-Actor-Id": str(uuid.uuid4()), "X-EB-Gateway-ID": "gw-a"},
    )
    assert r.status_code == 403
    container.graph.delete_relation.assert_not_awaited()
    # Even the Cypher fallback path is bypassed by the guard.
    cypher_member_of_calls = [
        c for c in container.graph.query_cypher.await_args_list
        if c.args and "MEMBER_OF" in c.args[0]
    ]
    assert cypher_member_of_calls == []
