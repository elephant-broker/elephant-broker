"""R2-P7 / link-spam guard — ``MemoryStoreFacade._try_add_edge`` rejects
cross-gateway target IDs with ``PermissionError`` (→ HTTP 403 via
the R2-P5 error-handler middleware).

D11 contract: ``GraphAdapter`` primitives (``add_relation`` etc.) are
intentionally gateway-agnostic — gateway scoping happens at the
caller layer. This test pins the caller-level enforcement: when
``_try_add_edge`` is asked to attach an edge to a target node owned
by a different gateway, the helper raises ``PermissionError`` BEFORE
``graph.add_relation`` is invoked. Pre-R2-P7 the cross-gateway link
went through silently — observers had no way to detect that a fact
in tenant A had edges to actors / goals in tenant B.
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from elephantbroker.runtime.memory.facade import MemoryStoreFacade


@pytest.mark.asyncio
async def test_try_add_edge_rejects_cross_gateway_target():
    """G_LinkSpam (R2-P7): ``_try_add_edge`` raises ``PermissionError``
    when the target node belongs to a different gateway.

    Setup:
    * Facade bound to gateway ``"gw-a"``.
    * Mock ``graph.get_entity(target)`` returns a node with
      ``gateway_id="gw-b"`` — cross-gateway.

    Expected:
    * ``PermissionError`` raised; message names both gateway ids and
      the R2-P7 guard.
    * ``graph.add_relation`` never called — the guard fires BEFORE
      the edge is attempted.
    """
    graph = AsyncMock()
    graph.get_entity = AsyncMock(return_value={"gateway_id": "gw-b", "eb_id": "tgt-1"})
    graph.add_relation = AsyncMock()

    facade = MemoryStoreFacade(
        graph=graph,
        vector=MagicMock(),
        embeddings=MagicMock(),
        trace_ledger=MagicMock(),
        dataset_name="test",
        gateway_id="gw-a",
    )

    with pytest.raises(PermissionError) as excinfo:
        await facade._try_add_edge("src-1", "tgt-1", "ABOUT_ACTOR")

    assert "gw-b" in str(excinfo.value)
    assert "gw-a" in str(excinfo.value)
    assert "R2-P7" in str(excinfo.value)
    # Guard fires BEFORE add_relation — verify never called.
    graph.add_relation.assert_not_awaited()


@pytest.mark.asyncio
async def test_try_add_edge_allows_same_gateway_target():
    """G_LinkSpam-bis (R2-P7): same-gateway target proceeds normally —
    ``add_relation`` is invoked and the edge is created. Pins the
    happy path so a future tightening of the guard (e.g.,
    requiring an explicit allow-list) surfaces as a regression here.
    """
    graph = AsyncMock()
    graph.get_entity = AsyncMock(return_value={"gateway_id": "gw-a", "eb_id": "tgt-1"})
    graph.add_relation = AsyncMock()

    facade = MemoryStoreFacade(
        graph=graph,
        vector=MagicMock(),
        embeddings=MagicMock(),
        trace_ledger=MagicMock(),
        dataset_name="test",
        gateway_id="gw-a",
    )

    result = await facade._try_add_edge("src-1", "tgt-1", "ABOUT_ACTOR")
    # Edge created — return value is the count of edges added.
    assert result == 1
    graph.add_relation.assert_awaited_once_with("src-1", "tgt-1", "ABOUT_ACTOR")
