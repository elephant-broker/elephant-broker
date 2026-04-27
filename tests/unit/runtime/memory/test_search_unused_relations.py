"""TF-FN-019 G8 FLIPPED — MemoryStoreFacade.search() structural Cypher
no longer collects relations (#1177 RESOLVED — R2-P9).

Pre-fix: the structural query in ``_build_structural_query()``
(facade.py around 393-399) emitted
``OPTIONAL MATCH (f)-[r]->(target) ... collect({type, target}) AS
relations`` alongside ``properties(f) AS props``. The
``search()`` consumer at facade.py read only ``rec["props"]`` and
discarded ``rec["relations"]`` — every relation tuple cost a Neo4j
round-trip without reaching Python.

Post-R2-P9: the OPTIONAL MATCH + collect() are gone. The Cypher is
now a single ``MATCH (f:FactDataPoint) WHERE ... RETURN
properties(f) AS props LIMIT $limit`` — same shape the consumer
already used. If a future feature wires relations into the
FactAssertion surface, restore the collect() clause and update the
schema in the same commit (this test will surface the regression).
"""
from __future__ import annotations

import uuid
from unittest.mock import AsyncMock

import pytest

from elephantbroker.runtime.memory.facade import MemoryStoreFacade
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.schemas.base import Scope
from tests.fixtures.factories import make_fact_assertion


async def test_search_structural_cypher_does_not_collect_relations_post_R2P9_fix():
    """G8 FLIPPED (#1177 RESOLVED — R2-P9): the structural search Cypher
    no longer contains ``OPTIONAL MATCH`` or ``collect(`` — the
    relation-tuple collection that the consumer always discarded is
    gone, cutting the Neo4j work to a label scan + WHERE filter.
    """
    graph = AsyncMock()
    vector = AsyncMock()
    embeddings = AsyncMock()
    embeddings.embed_text = AsyncMock(return_value=[0.1] * 1024)
    facade = MemoryStoreFacade(
        graph, vector, embeddings, TraceLedger(), dataset_name="t",
    )
    fact = make_fact_assertion()
    graph.query_cypher = AsyncMock(return_value=[{
        "props": {
            "eb_id": str(fact.id), "text": fact.text, "category": "general",
            "scope": "session", "confidence": 1.0, "eb_created_at": 0,
            "eb_updated_at": 0, "use_count": 0, "successful_use_count": 0,
            "provenance_refs": [], "target_actor_ids": [], "goal_ids": [],
        },
    }])
    await facade.search("q", scope=Scope.SESSION)
    # query_cypher was called — extract the Cypher string from the call args.
    call_args = graph.query_cypher.call_args
    cypher = call_args.args[0] if call_args.args else call_args.kwargs.get("cypher", "")

    # Post-fix: no OPTIONAL MATCH, no collect( — the slim shape only.
    assert "OPTIONAL MATCH" not in cypher, (
        "Structural search Cypher still contains OPTIONAL MATCH — the "
        "relation-collect was supposed to be dropped in R2-P9. If a "
        "future feature wired relations into FactAssertion, update this "
        "test and #1177 / TF-FN-019 G8."
    )
    assert "collect(" not in cypher, (
        "Structural search Cypher still contains collect( — the relation-"
        "tuple collection was supposed to be dropped in R2-P9."
    )
    # Defensive: the slim shape MUST still RETURN properties(f) AS props
    # for the consumer at search() to work.
    assert "properties(f) AS props" in cypher
