"""TF-FN-018 G2 + G4 — MemoryStoreFacade.update() preserves and audits
``gateway_id`` across read-modify-write.

* G2 pins the in-tenant happy path: an update from the owning gateway must
  NOT strip / rewrite the stored ``gateway_id`` when the new DataPoint is
  persisted via ``add_data_points()``. This guards against a regression
  where ``gateway_id`` is listed in the update-payload whitelist by
  accident (today ``facade.py:523`` treats it as immutable alongside
  ``id``/``created_at``/``source_actor_id``).

* G4 pins the cross-tenant failure path: the AUTHORITY_CHECK_FAILED trace
  event for a rejected update must carry BOTH the stored owner's
  ``gateway_id`` AND the caller's ``gateway_id`` in payload keys, so
  forensic audit can distinguish the two sides of an authz denial.

These are gap-fills on top of the existing ``test_update_permission_error_emits_authority_check_trace``
in ``test_memory_facade.py`` — that test verifies the trace is emitted but
does not check both payload keys, nor does any existing test verify the
round-trip preservation of ``gateway_id`` on a successful RMW.
"""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock

import pytest

from elephantbroker.runtime.adapters.cognee.datapoints import FactDataPoint
from elephantbroker.runtime.memory.facade import MemoryStoreFacade
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.schemas.trace import TraceEventType, TraceQuery
from tests.fixtures.factories import make_fact_assertion


def _fact_props(fact, **overrides):
    """FactDataPoint-shaped Neo4j-node dict (mirrors test_memory_facade.py)."""
    base = {
        "eb_id": str(fact.id), "text": fact.text, "category": "general",
        "scope": "session", "confidence": 1.0, "memory_class": "episodic",
        "eb_created_at": 0, "eb_updated_at": 0, "use_count": 0,
        "successful_use_count": 0, "provenance_refs": [], "target_actor_ids": [],
        "goal_ids": [],
    }
    base.update(overrides)
    return base


def _make_facade(gateway_id: str = "gw-a"):
    graph = AsyncMock()
    vector = AsyncMock()
    embeddings = AsyncMock()
    embeddings.embed_text = AsyncMock(return_value=[0.1] * 1024)
    ledger = TraceLedger()
    facade = MemoryStoreFacade(
        graph, vector, embeddings, ledger,
        dataset_name="test_ds", gateway_id=gateway_id,
    )
    return facade, graph, ledger


async def test_update_preserves_gateway_id_on_RMW(
    monkeypatch, mock_add_data_points, mock_cognee,
):
    """G2 (TF-FN-018): update() from the owning gateway preserves
    ``gateway_id`` through the full read-modify-write round-trip.

    Facade pattern: ``entity = get_entity(...)`` → build ``FactDataPoint`` →
    apply updates → ``add_data_points([updated_dp])``. The update-payload
    whitelist at ``facade.py:523`` rejects writes to ``gateway_id``, but
    that only blocks attacker-supplied overwrites. The test here asserts
    the BENIGN case: when caller + owner agree on gateway, the persisted
    DataPoint still carries the right gateway_id (not None, not blank).
    """
    facade, graph, _ = _make_facade(gateway_id="gw-a")
    monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
    monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
    fact = make_fact_assertion()
    graph.get_entity = AsyncMock(return_value={
        **_fact_props(fact), "gateway_id": "gw-a",
    })
    await facade.update(
        fact.id, {"confidence": 0.5}, caller_gateway_id="gw-a",
    )
    # Exactly one add_data_points call on the happy path.
    assert len(mock_add_data_points.calls) == 1
    persisted_dp = mock_add_data_points.calls[0]["data_points"][0]
    assert isinstance(persisted_dp, FactDataPoint)
    assert persisted_dp.gateway_id == "gw-a", (
        f"gateway_id must be preserved on RMW; got {persisted_dp.gateway_id!r}"
    )


async def test_audit_table_records_caller_gateway_for_failed_update(
    monkeypatch, mock_add_data_points, mock_cognee,
):
    """G4 (TF-FN-018): an AUTHORITY_CHECK_FAILED trace for a cross-gateway
    update must carry both ``owner_gateway`` and ``caller_gateway`` keys.

    The existing test_update_permission_error_emits_authority_check_trace
    in test_memory_facade.py verifies the event is emitted and the
    ``action`` discriminator is set, but does not pin that BOTH gateway
    identifiers are in the payload. Without both, an audit tool cannot
    tell which tenant attacked which — only that an authz failure
    happened. This test pins the dual-key shape.
    """
    facade, graph, ledger = _make_facade(gateway_id="gw-owner")
    monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
    monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
    fact = make_fact_assertion()
    graph.get_entity = AsyncMock(return_value={
        **_fact_props(fact), "gateway_id": "gw-owner",
    })
    with pytest.raises(PermissionError):
        await facade.update(
            fact.id, {"confidence": 0.1}, caller_gateway_id="gw-attacker",
        )
    events = await ledger.query_trace(
        TraceQuery(event_types=[TraceEventType.AUTHORITY_CHECK_FAILED]),
    )
    assert len(events) == 1
    payload = events[0].payload
    # Both sides of the authz denial must be in the payload — audit needs
    # to know both "who owned this" and "who tried to touch it".
    assert payload.get("owner_gateway") == "gw-owner", (
        f"owner_gateway missing or wrong: {payload!r}"
    )
    assert payload.get("caller_gateway") == "gw-attacker", (
        f"caller_gateway missing or wrong: {payload!r}"
    )
    assert payload.get("action") == "update", (
        f"action discriminator missing: {payload!r}"
    )
