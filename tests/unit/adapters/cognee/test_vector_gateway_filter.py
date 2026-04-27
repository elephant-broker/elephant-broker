"""R2-P1 / TD-64 #1187 RESOLVED — VectorAdapter injects a
``database_name=<gateway_id>`` tenant filter into every ``search_similar``
query when constructed with a non-empty gateway_id.

Paired with ``test_config_database_name.py`` (write side — populates the
tenant field) + ``test_container_gateway_id_passed_to_cognee.py`` (wiring
verification).
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from qdrant_client.models import FieldCondition, Filter, MatchValue

from elephantbroker.runtime.adapters.cognee.vector import VectorAdapter
from elephantbroker.schemas.config import CogneeConfig


def _stub_client(points=None) -> AsyncMock:
    """Build an AsyncMock Qdrant client whose ``query_points`` returns a
    shape compatible with the real client (an object with a ``points``
    attribute)."""
    client = AsyncMock()
    client.query_points = AsyncMock(return_value=SimpleNamespace(points=points or []))
    return client


async def test_search_similar_injects_gateway_filter_when_gateway_id_set():
    """G3: when VectorAdapter has ``gateway_id="gw-a"``, every
    ``search_similar`` call carries a ``database_name=gw-a``
    FieldCondition in the ``must`` list of the Qdrant query filter.

    The condition uses the Cognee community adapter's tenant field
    (``database_name`` with ``is_tenant:true``), so Qdrant's native
    multi-tenancy fast-path fires.
    """
    adapter = VectorAdapter(CogneeConfig(neo4j_password="x"), gateway_id="gw-a")
    adapter._client = _stub_client()
    await adapter.search_similar("FactDataPoint_text", [0.1] * 16, top_k=5)
    call_kwargs = adapter._client.query_points.call_args.kwargs
    filt: Filter | None = call_kwargs["query_filter"]
    assert filt is not None
    # The filter's must list must contain the gateway condition.
    assert filt.must is not None
    gateway_conds = [
        c for c in filt.must
        if isinstance(c, FieldCondition) and c.key == "database_name"
    ]
    assert len(gateway_conds) == 1
    assert gateway_conds[0].match.value == "gw-a"


async def test_search_similar_merges_gateway_filter_with_caller_filter():
    """G3-extend: caller-supplied filters are preserved; the gateway
    condition is appended to the caller's ``must`` list (not replaced).
    ``should`` / ``must_not`` survive untouched.
    """
    adapter = VectorAdapter(CogneeConfig(neo4j_password="x"), gateway_id="gw-a")
    adapter._client = _stub_client()
    caller_cond = FieldCondition(key="category", match=MatchValue(value="fact"))
    caller_should = FieldCondition(key="scope", match=MatchValue(value="session"))
    caller_filter = Filter(must=[caller_cond], should=[caller_should])

    await adapter.search_similar(
        "FactDataPoint_text", [0.1] * 16, top_k=5, filters=caller_filter,
    )
    effective = adapter._client.query_points.call_args.kwargs["query_filter"]
    assert effective is not None
    # Caller's must condition preserved.
    must_keys = {c.key for c in effective.must if isinstance(c, FieldCondition)}
    assert "category" in must_keys
    # Gateway condition appended.
    assert "database_name" in must_keys
    # Should list preserved (pointer equality not guaranteed; check content).
    assert effective.should == [caller_should]


async def test_search_similar_skips_filter_when_gateway_id_empty():
    """G3-regression: empty gateway_id means no filter injected —
    preserves legacy single-tenant behavior and back-compat with
    pre-R2-P1 points that have ``database_name=""``. If this test flips
    to pass (i.e., the filter ALWAYS injects), legacy data becomes
    filter-invisible.
    """
    adapter = VectorAdapter(CogneeConfig(neo4j_password="x"), gateway_id="")
    adapter._client = _stub_client()
    await adapter.search_similar("FactDataPoint_text", [0.1] * 16, top_k=5)
    filt = adapter._client.query_points.call_args.kwargs["query_filter"]
    # No filter injected at all when gateway_id is empty AND caller passed none.
    assert filt is None

    # And with a caller filter: pass-through unchanged.
    caller_filter = Filter(must=[
        FieldCondition(key="category", match=MatchValue(value="fact")),
    ])
    await adapter.search_similar(
        "FactDataPoint_text", [0.1] * 16, top_k=5, filters=caller_filter,
    )
    effective = adapter._client.query_points.call_args.kwargs["query_filter"]
    assert effective is caller_filter  # no merge attempt
