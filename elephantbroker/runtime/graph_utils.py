"""Utilities for reconstructing DataPoints from Neo4j graph properties."""
from __future__ import annotations

import json
from typing import Any


def clean_graph_props(raw: dict[str, Any]) -> dict[str, Any]:
    """Prepare Neo4j node properties for DataPoint construction.

    - Strips internal keys (``_labels``, etc.)
    - Strips Cognee base-class keys that conflict with subclass fields
    - Deserialises JSON-encoded dict fields (Neo4j stores dicts as strings)
    """
    # Keys added by Cognee's base DataPoint that must not be forwarded
    # into the subclass constructor when the node props already carry
    # a Cognee-generated ``id`` (UUID) that differs from ``eb_id``.
    skip = {"_labels", "id"}

    out: dict[str, Any] = {}
    for k, v in raw.items():
        if k.startswith("_") or k in skip:
            continue
        # Dicts may be stored as JSON strings by add_data_points() or Neo4j property serialization
        if isinstance(v, str) and v.startswith("{"):
            try:
                v = json.loads(v)
            except (json.JSONDecodeError, ValueError):
                pass
        out[k] = v
    return out
