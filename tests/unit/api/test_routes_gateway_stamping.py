"""TF-FN-018 G3 — every mutation/read route on /memory propagates the
X-EB-Gateway-ID header to the facade layer.

Five routes use the ``caller_gateway_id`` kwarg (get_by_id, update, delete,
promote_scope, promote_class); one (POST /memory/store) stamps
``fact.gateway_id`` from ``request.state.gateway_id`` before the facade
call. This test pins both wirings in one parametrized surface so a regression
on any of the six is caught in a single test run.

Note: file placed flat under ``tests/unit/api/`` to match the existing
``test_routes_*.py`` convention rather than the dispatch-brief path
``tests/unit/api/routes/test_routes_gateway_stamping.py`` — minor location
deviation, same semantics.
"""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock

import pytest

from elephantbroker.schemas.base import Scope
from elephantbroker.schemas.fact import FactAssertion, MemoryClass


# ---------------------------------------------------------------------------
# Kwarg-based routes: caller_gateway_id is passed explicitly to the facade
# method via keyword argument. Each tuple: (method, path_template, body,
# facade_method_name, status_success).
# ---------------------------------------------------------------------------

_KWARG_ROUTES = [
    # GET /memory/{fact_id} -> get_by_id(caller_gateway_id=...)
    (
        "GET", "/memory/{fact_id}", None, "get_by_id", 404,  # 404 because mock returns None
    ),
    # PATCH /memory/{fact_id} -> update(caller_gateway_id=...)
    (
        "PATCH", "/memory/{fact_id}", {"confidence": 0.9}, "update", 404,
    ),
    # DELETE /memory/{fact_id} -> delete(caller_gateway_id=...)
    (
        "DELETE", "/memory/{fact_id}", None, "delete", 404,
    ),
    # POST /memory/promote-scope -> promote_scope(caller_gateway_id=...)
    (
        "POST", "/memory/promote-scope",
        {"fact_id": None, "to_scope": "global"},  # fact_id filled at runtime
        "promote_scope", 404,
    ),
    # POST /memory/promote-class -> promote_class(caller_gateway_id=...)
    (
        "POST", "/memory/promote-class",
        {"fact_id": None, "to_class": "semantic"},
        "promote_class", 404,
    ),
]


@pytest.mark.parametrize(
    "method,path_tpl,body,facade_method,expected_status",
    _KWARG_ROUTES,
    ids=["GET_by_id", "PATCH_update", "DELETE_delete", "POST_promote_scope", "POST_promote_class"],
)
async def test_five_routes_propagate_gateway_id_to_facade_as_kwarg(
    method, path_tpl, body, facade_method, expected_status, client, container,
):
    """G3 (TF-FN-018): five memory routes propagate ``X-EB-Gateway-ID`` header
    as ``caller_gateway_id=`` kwarg to the corresponding facade method.

    We replace the facade method with an AsyncMock that records its kwargs,
    then issue the HTTP request with a known header and assert the captured
    ``caller_gateway_id`` matches.

    The mock raises ``KeyError`` so the route returns 404 — we only care
    that the kwarg was propagated, not that the facade succeeds.
    """
    recorded: dict = {}

    async def _spy(*args, **kwargs):
        recorded.update(kwargs)
        raise KeyError("pinned 404 — only kwarg propagation is under test")

    # Install the spy on the facade method.
    setattr(container.memory_store, facade_method, AsyncMock(side_effect=_spy))

    # Fill in a dummy fact_id for body-based routes + path routes alike.
    fact_id = uuid.uuid4()
    path = path_tpl.format(fact_id=fact_id)
    if body is not None and "fact_id" in body and body["fact_id"] is None:
        body = {**body, "fact_id": str(fact_id)}

    headers = {"X-EB-Gateway-ID": "tenant-alpha"}
    resp = await client.request(method, path, json=body, headers=headers)

    assert resp.status_code == expected_status, (
        f"{method} {path} returned {resp.status_code}; body={resp.text!r}"
    )
    assert recorded.get("caller_gateway_id") == "tenant-alpha", (
        f"{method} {path} did not propagate caller_gateway_id; captured kwargs={recorded!r}"
    )


async def test_store_route_stamps_gateway_id_from_request_state(client, container):
    """G3 (TF-FN-018) — POST /memory/store DIFFERS from the other five: it
    stamps ``fact.gateway_id`` from ``request.state.gateway_id`` BEFORE
    calling the facade (see memory.py:115-119) rather than passing a
    ``caller_gateway_id`` kwarg.

    Pinned here so a refactor that accidentally removes the stamp — or
    switches to the kwarg pattern without updating the facade — surfaces
    in the same test run as the other five routes.
    """
    recorded: list[FactAssertion] = []

    async def _spy(fact, **kwargs):
        recorded.append(fact)
        return fact

    container.memory_store.store = AsyncMock(side_effect=_spy)

    # StoreRequest wraps FactAssertion in a "fact" field (see memory.py:47-52).
    body = {
        "fact": {
            "text": "a fact under a specific tenant",
            "source_actor_id": str(uuid.uuid4()),
            "category": "general",
            "scope": "session",
        },
    }
    headers = {"X-EB-Gateway-ID": "tenant-alpha"}
    resp = await client.post("/memory/store", json=body, headers=headers)
    assert resp.status_code == 200, f"store failed: {resp.text!r}"
    assert len(recorded) == 1
    assert recorded[0].gateway_id == "tenant-alpha", (
        f"fact.gateway_id not stamped from header; got {recorded[0].gateway_id!r}"
    )
