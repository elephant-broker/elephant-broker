"""Cross-cutting identity validation utilities.

R2-P7 / link-spam closure (#1497 / #1498 / #1499 / #1158 / #1495):
:class:`elephantbroker.runtime.adapters.cognee.graph.GraphAdapter`'s
structural primitives (``add_relation``, ``delete_relation``,
``get_neighbors``, ``query_subgraph``, ``delete_entity``) are
**intentionally gateway-agnostic** per CLAUDE.md D11 — the rule
applies to *module-level Cypher* (queries issued via
``query_cypher()`` by runtime modules), not to the structural
primitives.

The corollary: callers that pass user-supplied entity IDs into the
primitives must validate that the target node belongs to the
caller's gateway *before* the edge is created. Otherwise a
malicious or buggy caller can attach an edge from one tenant's
fact to another tenant's actor / goal — "link-spam" surface.

This module exposes ``assert_same_gateway`` — a thin defensive
helper that fetches the target node and raises ``PermissionError``
on cross-gateway mismatch. The R2-P5 error-handler middleware
maps ``PermissionError`` to HTTP 403, so a route-layer caller
gets the right status code without any additional wiring.
"""
from __future__ import annotations

from typing import Any, Protocol


class _GraphLike(Protocol):
    """Minimal protocol — anything with an ``async get_entity(id)``
    method that returns ``dict | None``. Avoids importing the
    concrete GraphAdapter to keep this module dependency-light."""

    async def get_entity(self, entity_id: str, *, gateway_id: str | None = ...) -> dict[str, Any] | None: ...


class _GraphCypherLike(Protocol):
    async def query_cypher(self, query: str, params: dict[str, Any]) -> list[dict[str, Any]]: ...


async def assert_same_gateway(
    graph: _GraphLike,
    target_id: str,
    expected_gw: str,
) -> None:
    """Raise ``PermissionError`` if the target node's ``gateway_id``
    differs from ``expected_gw``.

    Best-effort by design:

    * Skips the check if ``graph`` is ``None`` (test fixtures /
      tier-disabled paths).
    * Skips the check if the target node is not found — callers
      already handle missing-node downstream (MERGE no-op or
      KeyError) and we don't want to double-fail on the
      404-equivalent path.
    * Skips the check if either ``target_gw`` or ``expected_gw``
      is empty (legacy/single-tenant call sites where gateway
      stamping has not propagated yet — empty acts as a
      "match-anything" sentinel).

    Raises ``PermissionError`` only when both gateway_ids are
    non-empty and differ. The R2-P5 error-handler middleware
    converts ``PermissionError`` to HTTP 403.

    Args:
        graph: any object exposing ``async get_entity(id)``.
        target_id: ``eb_id`` of the node we're about to attach an
            edge to.
        expected_gw: caller's gateway id (typically
            ``self._gateway_id`` on a runtime module).
    """
    # TOCTOU accepted: GatewayIdentityMiddleware rejects cross-gateway requests
    # at the HTTP boundary; this check is defense-in-depth only.
    if graph is None:
        return
    entity = await graph.get_entity(target_id)
    if entity is None:
        return
    target_gw = entity.get("gateway_id", "") or ""
    if target_gw and expected_gw and target_gw != expected_gw:
        raise PermissionError(
            f"Cross-gateway link rejected: target {target_id!r} "
            f"belongs to gateway {target_gw!r}, caller is {expected_gw!r}. "
            f"R2-P7 link-spam guard."
        )


async def assert_same_gateway_batch(
    graph: _GraphCypherLike | None,
    ids: list[str],
    expected_gw: str,
) -> None:
    """Batch variant of ``assert_same_gateway`` — one Cypher round-trip
    for N target IDs instead of N ``get_entity`` calls.

    Same best-effort skip rules: graph is None, ids empty, expected_gw
    empty → silently return. Raises ``PermissionError`` on the first
    violating node found.
    """
    if graph is None or not ids or not expected_gw:
        return
    rows = await graph.query_cypher(
        "MATCH (n) WHERE n.eb_id IN $ids "
        "AND n.gateway_id <> '' AND n.gateway_id <> $expected_gw "
        "RETURN n.eb_id AS id, n.gateway_id AS gw LIMIT 1",
        {"ids": ids, "expected_gw": expected_gw},
    )
    if rows:
        row = rows[0]
        raise PermissionError(
            f"Cross-gateway link rejected: target {row['id']!r} "
            f"belongs to gateway {row['gw']!r}, caller is {expected_gw!r}. "
            f"R2-P7 link-spam guard (batch)."
        )
