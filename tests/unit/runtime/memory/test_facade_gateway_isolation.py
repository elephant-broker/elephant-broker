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

    async def test_dedup_search_leaks_across_gateways(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """G10 (#1187 PIN — BLOCKED by TD-64): documents CURRENT leak
        behavior in the store() dedup pre-check. This is NOT yet resolved.

        Root cause chain:
        1. ``facade.store()`` dedup step calls ``vector.search_similar()``
           to find near-duplicate existing facts.
        2. ``VectorAdapter.search_similar`` (``adapters/cognee/vector.py``)
           queries Qdrant's ``FactDataPoint_text`` collection. It has NO
           per-tenant filter — the Cognee community adapter indexes only
           ``IndexSchema`` fields (``id``, ``text``, ``database_name``),
           NOT the custom DataPoint fields (``gateway_id``, ``eb_id``,
           ``session_key``, ``memory_class``).
        3. This is TD-64: Cognee ``qdrant_adapter.py:163-167`` projects
           ``DataPoint -> IndexSchema(id=data_point.id, text=...)`` — a
           closed Pydantic model with no ``extra="allow"``. Every custom
           field is dropped at the index boundary.
        4. Consequence: when tenant B stores a near-duplicate of tenant A's
           fact, the vector search finds A's id + score and facade.store()
           raises ``DedupSkipped(existing_fact_id=A_id)`` — leaking the
           existence of a cross-tenant fact via the attacker-visible
           ``existing_fact_id`` / similarity score.

        Resolution path: TD-64 path (c) — populate Cognee's
        ``database_name`` tenant field with ``gateway_id`` per gateway init
        so Qdrant's native multi-tenancy filters kick in on
        ``search_similar``. See TD-64 entry in TECHNICAL-DEBT.md for full
        context and three alternative paths.

        If this test starts FAILING (dedup stops leaking), TD-64 is
        resolved — update this test to assert the new isolation behavior
        and mark #1187 RESOLVED.
        """
        facade, graph, vector, _ = _make_facade(gateway_id="tenant-local")
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        # Dedup search returns a cross-tenant high-similarity hit. Today the
        # facade has no way to know it's cross-tenant because the Qdrant
        # payload doesn't carry gateway_id.
        other_tenant_fact_id = uuid.uuid4()
        vector.search_similar = AsyncMock(return_value=[
            VectorSearchResult(
                id=str(other_tenant_fact_id),
                score=0.99,
                payload={"text": "near-duplicate text"},
            ),
        ])
        fact = make_fact_assertion()
        with pytest.raises(DedupSkipped) as excinfo:
            await facade.store(fact)
        # Leak surface: the raised error carries the OTHER tenant's id.
        # When TD-64 ships and search_similar filters by gateway_id,
        # DedupSkipped should NOT fire here — remove this assertion and
        # assert a clean store instead.
        assert excinfo.value.existing_fact_id == str(other_tenant_fact_id)

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
