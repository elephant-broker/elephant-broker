"""TF-FN-019 G8 — MemoryStoreFacade.search() structural Cypher collects
relations but the facade discards them.

PROD #1177 pin. The structural query in ``_build_search_cypher()``
(facade.py:385-390) returns ``collect({type: type(r), target: properties(target)})
AS relations`` alongside ``properties(f) AS props``. The search() consumer
at facade.py:~265-274 only reads ``rec["props"]`` and never touches
``rec["relations"]``.

Two issues:
1. The database wastes cycles collecting relations that never reach Python.
2. A consumer expecting relations (e.g., to build a hierarchy) would need
   to either make a second round-trip or change the search contract.

Pin the dead-relations behavior so a future fix that either (a) removes
``collect(...)`` from the Cypher or (b) propagates relations to the
FactAssertion surface will flip this test and force an explicit contract
change.
"""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock

import pytest

from elephantbroker.runtime.memory.facade import MemoryStoreFacade
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.schemas.base import Scope
from tests.fixtures.factories import make_fact_assertion


async def test_structural_query_collects_relations_but_facade_discards_them():
    """G8 (#1177): the structural search Cypher collects `relations` but
    the Python search consumer reads only `props`, so every relation
    tuple the DB produces is thrown away.

    Strategy: mock `graph.query_cypher` to return records carrying BOTH
    `props` and `relations`. Assert the returned FactAssertion list has
    no relations-derived data — FactAssertion has no place for relations
    today. The assertion is structural: the schema has no
    `.relations` / `.related_*` field, and nothing in the return pipeline
    would know to emit one.
    """
    graph = AsyncMock()
    vector = AsyncMock()
    embeddings = AsyncMock()
    embeddings.embed_text = AsyncMock(return_value=[0.1] * 1024)
    facade = MemoryStoreFacade(
        graph, vector, embeddings, TraceLedger(), dataset_name="t",
    )
    fact = make_fact_assertion()
    # Carefully crafted `relations` list — would be meaningful data if
    # consumed. Asserting that it's discarded is the point.
    relations_payload = [
        {"type": "SERVES_GOAL", "target": {"eb_id": str(uuid.uuid4()), "title": "goal-a"}},
        {"type": "ABOUT_ACTOR", "target": {"eb_id": str(uuid.uuid4()), "display_name": "actor-b"}},
    ]
    graph.query_cypher = AsyncMock(return_value=[{
        "props": {
            "eb_id": str(fact.id), "text": fact.text, "category": "general",
            "scope": "session", "confidence": 1.0, "eb_created_at": 0,
            "eb_updated_at": 0, "use_count": 0, "successful_use_count": 0,
            "provenance_refs": [], "target_actor_ids": [], "goal_ids": [],
        },
        "relations": relations_payload,
    }])
    results = await facade.search("q", scope=Scope.SESSION)
    assert len(results) == 1
    returned_fact = results[0]
    # FactAssertion has no field that could carry arbitrary relation tuples;
    # verify structurally: no attribute named `relations`, and the sibling
    # fields that COULD carry related-entity ids (target_actor_ids,
    # goal_ids) are empty because they were not populated from relations.
    assert not hasattr(returned_fact, "relations"), (
        "FactAssertion gained a `relations` field — relations are no "
        "longer discarded. Update this test and #1177."
    )
    assert returned_fact.target_actor_ids == []
    assert returned_fact.goal_ids == []
