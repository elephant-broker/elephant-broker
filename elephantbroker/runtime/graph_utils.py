"""Utilities for reconstructing DataPoints from Neo4j graph properties."""
from __future__ import annotations

import json
from typing import Any


def clean_graph_props(raw: dict[str, Any]) -> dict[str, Any]:
    """Prepare Neo4j node properties for DataPoint construction.

    - Strips internal keys (``_labels``, etc.) and Cognee-injected ``id``.
    - Deserialises JSON-encoded dict (``{``-prefix) and list (``[``-prefix)
      fields. Neo4j stores dicts and lists as JSON strings; the consuming
      DataPoint subclass expects them as dict / list values.
    - **Skip rule for ``*_json`` suffix keys:** fields whose key ends in
      ``_json`` are an explicit JSON-string-storage contract (e.g.,
      ``ProcedureDataPoint.steps_json`` / ``red_line_bindings_json`` /
      ``approval_requirements_json``). The DataPoint class's own
      ``to_schema()`` calls ``json.loads()`` inside the method body, so
      the prop must arrive as ``str``. We preserve those untouched.

    #1163 RESOLVED (R2-P3): previously only ``{``-prefix strings were
    deserialised, so JSON-array fields stayed as strings even when the
    consumer needed a list. Now both ``{`` and ``[`` are handled, with
    the ``*_json`` skip-suffix as the explicit opt-out.
    """
    # Keys added by Cognee's base DataPoint that must not be forwarded
    # into the subclass constructor when the node props already carry
    # a Cognee-generated ``id`` (UUID) that differs from ``eb_id``.
    #
    # TF-FN-020 G5 defensive note: this is a closed-list strip — only
    # the literal ``id`` key is removed. A future DataPoint subclass that
    # introduces its own custom id-shaped field (e.g., ``relation_id``,
    # ``execution_id``) would NOT be caught by this set and could collide
    # with the inherited Cognee ``id`` if the subclass stores that
    # custom field as a UUID. Mitigation: keep custom id field names
    # disambiguated from the base ``id`` (e.g., ``eb_id``,
    # ``execution_id``) by schema convention. If a future DataPoint
    # subclass does need a literal ``id`` field, this skip set must be
    # widened by call site (e.g., per-class extra-skip arg) rather than
    # silently dropped.
    skip = {"_labels", "id"}

    out: dict[str, Any] = {}
    for k, v in raw.items():
        if k.startswith("_") or k in skip:
            continue
        # `*_json` suffix opts out of deserialization — see docstring
        # rationale. The consuming DataPoint class json.loads() inside
        # to_schema, so the prop must arrive as str.
        if isinstance(v, str) and not k.endswith("_json"):
            if v.startswith("{") or v.startswith("["):
                try:
                    v = json.loads(v)
                except (json.JSONDecodeError, ValueError):
                    pass
        out[k] = v
    return out
