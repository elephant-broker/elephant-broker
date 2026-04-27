"""TF-FN-019 G9 FLIPPED — ToolArtifactStore.search_artifacts gained 5
filter kwargs (#1179 RESOLVED — R2-P9).

Pre-fix: ``search_artifacts(query, max_results)`` accepted no
structural filter kwargs. The Stage 2 Cypher narrowed only by
``gateway_id``. A caller wanting artifacts from a specific tool /
actor / goal had to pull the gateway-wide list and filter in
Python — full-scan surface on busy gateways.

Post-R2-P9: signature gained 5 keyword-only kwargs:
``tool_name``, ``actor_id``, ``goal_id``, ``tags``, ``created_after``.
Each maps to a conditional WHERE clause appended to the Cypher;
they're opt-in (``None`` keeps the legacy unfiltered shape) and
combine with AND. The semantic Stage 1 path (Cognee
GRAPH_COMPLETION) still runs unfiltered — Cognee has no structured
filter API — but the structural Stage 2 + final dedup mean
filters narrow the actual result set.

This test exercises both the **signature contract** (the kwargs
are present and keyword-only) and the **runtime contract** (each
kwarg, when supplied, surfaces as a Cypher WHERE clause + the
expected param key).
"""
from __future__ import annotations

import inspect
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from elephantbroker.runtime.adapters.cognee.embeddings import EmbeddingService
from elephantbroker.runtime.adapters.cognee.graph import GraphAdapter
from elephantbroker.runtime.adapters.cognee.vector import VectorAdapter
from elephantbroker.runtime.artifacts.store import ToolArtifactStore
from elephantbroker.runtime.trace.ledger import TraceLedger


def test_search_artifacts_signature_includes_all_five_filter_kwargs():
    """G9 FLIPPED (#1179, signature contract): all 5 R2-P9 filter
    kwargs (``tool_name``, ``actor_id``, ``goal_id``, ``tags``,
    ``created_after``) are present and **keyword-only** so a caller
    cannot accidentally pass them positionally.
    """
    sig = inspect.signature(ToolArtifactStore.search_artifacts)
    params = sig.parameters
    expected_kwargs = {"tool_name", "actor_id", "goal_id", "tags", "created_after"}
    assert expected_kwargs.issubset(set(params.keys())), (
        f"Missing R2-P9 filter kwargs: "
        f"{expected_kwargs - set(params.keys())}. Got signature: {sig}"
    )
    # All 5 filter kwargs must be KEYWORD_ONLY (after a ``*`` separator)
    # — this prevents callers from accidentally swapping argument order.
    for name in expected_kwargs:
        kind = params[name].kind
        assert kind == inspect.Parameter.KEYWORD_ONLY, (
            f"Filter kwarg {name!r} must be KEYWORD_ONLY; got {kind}."
        )


def _make_store():
    graph = AsyncMock()
    graph.query_cypher = AsyncMock(return_value=[])
    vector = AsyncMock()
    embeddings = AsyncMock()
    embeddings.embed_text = AsyncMock(return_value=[0.1] * 1024)
    return graph, ToolArtifactStore(
        graph=graph,
        vector=vector,
        embeddings=embeddings,
        trace_ledger=TraceLedger(),
        dataset_name="t",
        gateway_id="gw-a",
    )


@pytest.mark.parametrize(
    "kwarg,value,expected_param_key,expected_clause_substring",
    [
        ("tool_name", "search_web",
         "tool_name", "a.tool_name = $tool_name"),
        ("actor_id", uuid.uuid4(),
         "actor_id", "a.actor_id = $actor_id"),
        ("goal_id", uuid.uuid4(),
         "goal_id", "a.goal_id = $goal_id"),
        ("tags", ["urgent", "draft"],
         "tags", "ANY(t IN a.tags WHERE t IN $tags)"),
        ("created_after", datetime(2026, 4, 1, tzinfo=timezone.utc),
         "created_after_ms", "a.eb_created_at >= $created_after_ms"),
    ],
    ids=["tool_name", "actor_id", "goal_id", "tags", "created_after"],
)
async def test_search_artifacts_supports_filter_kwargs_post_R2P9_fix(
    kwarg, value, expected_param_key, expected_clause_substring,
):
    """G9-runtime FLIPPED (#1179): each of the 5 filter kwargs surfaces
    as a Cypher WHERE clause + the expected param key when supplied.
    Parametrized so a future kwarg-rename / clause-rewrite surfaces
    case-by-case.
    """
    graph, store = _make_store()
    await store.search_artifacts("q", **{kwarg: value})
    # query_cypher is called from Stage 2 — extract its args.
    cypher_call = graph.query_cypher.call_args
    assert cypher_call is not None
    cypher = cypher_call.args[0]
    params = cypher_call.args[1] if len(cypher_call.args) > 1 else cypher_call.kwargs.get("params")

    assert expected_clause_substring in cypher, (
        f"Cypher missing clause for kwarg={kwarg!r}: {cypher}"
    )
    assert expected_param_key in params, (
        f"Cypher params missing key {expected_param_key!r}: {params}"
    )


async def test_search_artifacts_no_filters_passes_only_gateway_id_post_R2P9_fix():
    """G9-baseline (R2-P9): the legacy 2-arg call shape still works —
    only ``gateway_id`` ends up in the WHERE clause when no filter
    kwargs are supplied. Pins back-compat with pre-R2-P9 callers.
    """
    graph, store = _make_store()
    await store.search_artifacts("q")
    cypher_call = graph.query_cypher.call_args
    cypher = cypher_call.args[0]
    params = cypher_call.args[1] if len(cypher_call.args) > 1 else cypher_call.kwargs.get("params")

    assert "a.gateway_id = $gateway_id" in cypher
    # No filter clauses present.
    for clause in (
        "a.tool_name", "a.actor_id", "a.goal_id",
        "a.tags", "a.eb_created_at",
    ):
        assert clause not in cypher, f"Unexpected filter clause: {clause}"
    # Only the 2 baseline params.
    assert set(params.keys()) == {"limit", "gateway_id"}
