"""TF-FN-019 G9 — ToolArtifactStore.search_artifacts only filters by gateway_id.

PROD #1179 pin. ``search_artifacts(query, max_results)`` at
``artifacts/store.py:62-92`` accepts no additional filter kwargs —
not by ``tool_name``, not by ``content_hash``, not by time range, not by
``session_key``. The Cypher at line 80 is
``MATCH (a:ArtifactDataPoint) WHERE a.gateway_id = $gateway_id``, and
the semantic stage (Cognee GRAPH_COMPLETION) has no structured filter
either.

A caller wanting artifacts from a specific tool or a specific session
must pull the full gateway-wide list and filter in Python. On a busy
gateway with thousands of artifacts this is a full-scan surface.

Pin the current signature-level gap so a future fix that threads
``tool_name`` / ``session_key`` / ``created_after`` filters will flip
this test and force an explicit API contract update.
"""
from __future__ import annotations

import inspect

from elephantbroker.runtime.artifacts.store import ToolArtifactStore


def test_search_artifacts_only_filters_by_gateway_id():
    """G9 (#1179): the public signature of ``search_artifacts`` accepts
    ONLY ``query`` and ``max_results``. Filter kwargs (``tool_name``,
    ``session_key``, ``content_hash``, ``created_after``) are absent.

    This test inspects the signature structurally rather than exercising
    behavior, because the absence of a feature is harder to demonstrate
    via mocks than its presence. If any of those filter kwargs gets
    added to the signature in a future commit, this test breaks and the
    developer must update the #1179 plan entry.
    """
    sig = inspect.signature(ToolArtifactStore.search_artifacts)
    params = dict(sig.parameters)
    # `self` + the two supported kwargs are the only parameters.
    assert set(params.keys()) == {"self", "query", "max_results"}, (
        f"ToolArtifactStore.search_artifacts signature drifted: {sig}. "
        "If a filter kwarg was added (tool_name, session_key, etc.), "
        "update this test and the #1179 plan entry — the pin is no "
        "longer accurate."
    )
    # Defensive: no filter-shaped parameters snuck in under a different
    # name — guard against "filters: dict" or similar catch-all patterns.
    forbidden = {"tool_name", "session_key", "content_hash", "created_after", "filters"}
    assert not (set(params.keys()) & forbidden), (
        f"Unexpected filter parameter found: "
        f"{set(params.keys()) & forbidden}"
    )
