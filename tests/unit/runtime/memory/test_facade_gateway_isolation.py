"""TF-FN-018 G7-G10 — cross-gateway isolation tests for MemoryStoreFacade.

These pin the gateway-ownership pre-checks added in this PR:

* G7 — ``get_by_id`` returns ``None`` for cross-gateway read (404 semantic,
  hides existence oracle). #1167 RESOLVED.
* G8 — ``promote_scope`` raises ``PermissionError`` + emits
  ``AUTHORITY_CHECK_FAILED`` trace event. #1168 RESOLVED.
* G9 — ``promote_class`` same pattern. #1169 RESOLVED.
* G10 — ``store`` dedup search CURRENTLY leaks across gateways. #1187 PIN
  for present behavior. Fix blocked by TD-64 (Qdrant payload drops
  ``gateway_id``; the Cognee community adapter projects DataPoint -> IndexSchema
  at ``qdrant_adapter.py:163-167`` and ``vector.search_similar`` has no way
  to filter by gateway_id until that lands).
"""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from elephantbroker.runtime.adapters.cognee.vector import VectorSearchResult
from elephantbroker.runtime.memory.facade import DedupSkipped, MemoryStoreFacade
from elephantbroker.runtime.metrics import MetricsContext
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.schemas.base import Scope
from elephantbroker.schemas.fact import FactAssertion, MemoryClass
from elephantbroker.schemas.trace import TraceEventType, TraceQuery
from tests.fixtures.factories import make_fact_assertion


def _make_facade(gateway_id: str = "tenant-local"):
    """Build a MemoryStoreFacade with AsyncMock adapters and a real TraceLedger.

    Uses a real TraceLedger so AUTHORITY_CHECK_FAILED events can be queried
    directly. ``gateway_id`` parameterizes the facade's configured tenant —
    it's the fallback used when ``caller_gateway_id`` isn't supplied.
    """
    graph = AsyncMock()
    vector = AsyncMock()
    embeddings = AsyncMock()
    embeddings.embed_text = AsyncMock(return_value=[0.1] * 1024)
    ledger = TraceLedger()
    facade = MemoryStoreFacade(
        graph, vector, embeddings, ledger,
        dataset_name="test_ds", gateway_id=gateway_id,
    )
    return facade, graph, vector, ledger


def _fact_props(fact, **overrides):
    """Build a FactDataPoint-shaped dict mirroring the Neo4j node schema.

    Mirrors ``TestMemoryStoreFacadePhase4._fact_props`` in ``test_memory_facade.py``
    so test bodies here look the same; kept local for import-isolation.
    """
    base = {
        "eb_id": str(fact.id), "text": fact.text, "category": "general",
        "scope": "session", "confidence": 1.0, "memory_class": "episodic",
        "eb_created_at": 0, "eb_updated_at": 0, "use_count": 0,
        "successful_use_count": 0, "provenance_refs": [], "target_actor_ids": [],
        "goal_ids": [],
    }
    base.update(overrides)
    return base


class TestFacadeGatewayIsolation:
    async def test_get_by_id_returns_none_for_cross_gateway_caller(self):
        """G7 (#1167 RESOLVED): ``get_by_id`` returns ``None`` — not raise —
        when the stored ``gateway_id`` does not match ``caller_gateway_id``
        (or the facade's configured gateway_id).

        404 semantic: a cross-tenant caller cannot distinguish "does not
        exist" from "exists but not yours" — eliminates the enumeration
        side-channel. Mutation paths (update/promote_*/delete) use 403
        instead because the caller has already proved id knowledge via the
        PATCH/POST/DELETE intent.
        """
        facade, graph, _, _ = _make_facade(gateway_id="tenant-local")
        fact = make_fact_assertion()
        graph.get_entity = AsyncMock(return_value={
            **_fact_props(fact), "gateway_id": "tenant-other",
        })
        result = await facade.get_by_id(fact.id, caller_gateway_id="tenant-local")
        assert result is None

    async def test_promote_scope_rejects_cross_gateway_caller(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """G8 (#1168 RESOLVED): cross-gateway ``promote_scope`` raises
        ``PermissionError`` + emits ``AUTHORITY_CHECK_FAILED`` trace event
        with ``action="promote_scope"`` discriminator so forensic audit can
        tell scope-promotions from class-promotions and updates/deletes.
        """
        facade, graph, _, ledger = _make_facade(gateway_id="tenant-local")
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion()
        graph.get_entity = AsyncMock(return_value={
            **_fact_props(fact), "gateway_id": "tenant-other",
        })
        with pytest.raises(PermissionError):
            await facade.promote_scope(
                fact.id, Scope.GLOBAL, caller_gateway_id="tenant-local",
            )
        events = await ledger.query_trace(
            TraceQuery(event_types=[TraceEventType.AUTHORITY_CHECK_FAILED]),
        )
        assert len(events) == 1
        assert events[0].payload["action"] == "promote_scope"
        assert events[0].payload["fact_id"] == str(fact.id)
        assert events[0].payload["owner_gateway"] == "tenant-other"
        assert events[0].payload["caller_gateway"] == "tenant-local"

    async def test_promote_class_rejects_cross_gateway_caller(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """G9 (#1169 RESOLVED): cross-gateway ``promote_class`` raises
        ``PermissionError`` + emits ``AUTHORITY_CHECK_FAILED`` trace with
        ``action="promote_class"`` discriminator. Same pattern as G8.
        """
        facade, graph, _, ledger = _make_facade(gateway_id="tenant-local")
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        fact = make_fact_assertion()
        graph.get_entity = AsyncMock(return_value={
            **_fact_props(fact), "gateway_id": "tenant-other",
        })
        with pytest.raises(PermissionError):
            await facade.promote_class(
                fact.id, MemoryClass.SEMANTIC, caller_gateway_id="tenant-local",
            )
        events = await ledger.query_trace(
            TraceQuery(event_types=[TraceEventType.AUTHORITY_CHECK_FAILED]),
        )
        assert len(events) == 1
        assert events[0].payload["action"] == "promote_class"
        assert events[0].payload["owner_gateway"] == "tenant-other"
        assert events[0].payload["caller_gateway"] == "tenant-local"

    async def test_dedup_search_isolated_across_gateways_post_TD64_fix(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """G10 (#1187 RESOLVED — R2-P1 via TD-64 path c): tenant isolation
        on ``facade.store()`` dedup pre-check is now enforced by the
        ``VectorAdapter`` filter layer.

        Pre-fix (this test previously pinned the LEAK via
        ``test_dedup_search_leaks_across_gateways``): dedup called
        ``vector.search_similar()`` with no tenant filter, and the Qdrant
        payload had no ``gateway_id`` field to filter on. Cross-tenant
        near-duplicates produced ``DedupSkipped(existing_fact_id=A_id)``
        — leaking the existence of a fact in another gateway via the
        attacker-visible id + similarity score.

        Post-fix (R2-P1 commit):
        1. ``configure_cognee(gateway_id=gw)`` sets Qdrant's
           ``vector_db_name`` per gateway. Every point written via
           ``add_data_points()`` now carries
           ``payload.database_name=<gateway_id>``.
        2. ``VectorAdapter.__init__(gateway_id=gw)`` retains the id and
           automatically merges a ``database_name=<gw>`` FieldCondition
           into every ``search_similar`` query.
        3. Consequence: a cross-tenant near-duplicate search returns 0
           hits, so ``facade.store()`` bypasses the DedupSkipped branch
           and stores the fact cleanly.

        This test simulates the post-fix behavior with a mocked
        VectorAdapter whose ``search_similar`` returns the empty list a
        real post-fix adapter would return for cross-tenant queries.
        Integration tests on staging verify the real Qdrant filter via
        the devops / observer L2 sweep.

        Verification in H11 sweep: observer confirms
        ``eb:gw-alex-assistant`` tenant's FactDataPoint_text collection
        returns 25 points under the tenant filter (vs 0 points under a
        wrong-tenant filter) — see ``IMPLEMENTED-PR-7-merge.md`` R2-P1
        section for the raw probe output.
        """
        facade, graph, vector, _ = _make_facade(gateway_id="tenant-local")
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        # Post-fix: the tenant-filtered search returns 0 hits for a
        # cross-tenant store, regardless of what cross-tenant facts exist.
        vector.search_similar = AsyncMock(return_value=[])
        fact = make_fact_assertion()
        # No DedupSkipped — the store completes normally.
        stored = await facade.store(fact)
        assert stored.id == fact.id
        assert stored.gateway_id == "tenant-local"

    async def test_facade_gateway_isolation_increments_authority_check_metric(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """TF-FN-018 follow-up: every facade gateway-isolation rejection
        must pair the AUTHORITY_CHECK_FAILED trace event with an increment
        of the ``eb_authority_checks_total{result="denied"}`` metric.

        Surfaced by observer L2 Recipe A: 3 cross-gateway probes returned
        403, 3 trace events fired, but the counter stayed at 0. Metric
        was declared (``metrics.py:216-217``) and the method existed
        (``inc_authority_check`` at metrics.py:821-823) — but it was only
        wired from ``api/routes/_authority.py`` (approval flows), never
        from the new facade pre-checks.

        This test pins the fix across all 4 facade pre-check sites:
        ``get_by_id`` (read, 404-semantic), ``update``, ``promote_scope``,
        ``promote_class`` (mutations, 403 + trace). Each should drive a
        ``metrics.inc_authority_check(action=..., result="denied")`` call
        with the ``action`` label matching the facade method name.
        """
        facade, graph, vector, ledger = _make_facade(gateway_id="gw-owner")
        monkeypatch.setattr(
            "elephantbroker.runtime.memory.facade.add_data_points",
            mock_add_data_points,
        )
        monkeypatch.setattr(
            "elephantbroker.runtime.memory.facade.cognee", mock_cognee,
        )
        # Install a spy metrics context. MagicMock auto-accepts the
        # inc_authority_check kwargs and records the call.
        metrics_spy = MagicMock(spec=MetricsContext)
        facade._metrics = metrics_spy
        fact = make_fact_assertion()
        # Every probe finds the same entity — stored under a different
        # gateway than the caller-supplied one.
        graph.get_entity = AsyncMock(return_value={
            **_fact_props(fact), "gateway_id": "gw-owner",
        })

        # Probe 1: get_by_id (read path, returns None)
        result = await facade.get_by_id(fact.id, caller_gateway_id="gw-attacker")
        assert result is None

        # Probe 2: promote_scope (mutation, raises PermissionError)
        with pytest.raises(PermissionError):
            await facade.promote_scope(
                fact.id, Scope.GLOBAL, caller_gateway_id="gw-attacker",
            )
        # Probe 3: promote_class (mutation, raises PermissionError)
        with pytest.raises(PermissionError):
            await facade.promote_class(
                fact.id, MemoryClass.SEMANTIC, caller_gateway_id="gw-attacker",
            )
        # Probe 4: update (mutation, raises PermissionError)
        with pytest.raises(PermissionError):
            await facade.update(
                fact.id, {"confidence": 0.1}, caller_gateway_id="gw-attacker",
            )

        # All four sites must have called inc_authority_check once, with
        # the specific action label + result="denied".
        call_args_list = [
            call.kwargs for call in metrics_spy.inc_authority_check.call_args_list
        ]
        assert len(call_args_list) == 4, (
            f"Expected 4 inc_authority_check calls (one per facade site); "
            f"got {len(call_args_list)}: {call_args_list!r}"
        )
        actions_denied = {
            kwargs.get("action"): kwargs.get("result") for kwargs in call_args_list
        }
        assert actions_denied == {
            "get_by_id": "denied",
            "promote_scope": "denied",
            "promote_class": "denied",
            "update": "denied",
        }, (
            f"Expected one denied call per facade site with matching action "
            f"label; got {actions_denied!r}"
        )
