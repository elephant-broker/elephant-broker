"""R2-P1 / TD-64 #1187 RESOLVED — container.from_config plumbs
``gateway_id`` into ``configure_cognee()`` AND the ``VectorAdapter``
constructor. Both sides of the tenant-isolation contract have to agree
on the same key; this test pins the wiring at the container level.
"""
from __future__ import annotations

import inspect

from elephantbroker.runtime import container as container_mod
from elephantbroker.runtime.container import RuntimeContainer


def test_container_from_config_passes_gateway_id_to_cognee_and_vector_adapter():
    """Introspection-only pin: the source of ``RuntimeContainer.from_config``
    must (a) extract ``gw_id`` before calling ``configure_cognee``, (b)
    pass ``gateway_id=gw_id`` to ``configure_cognee``, and (c) pass
    ``gateway_id=gw_id`` to ``VectorAdapter(...)``.

    Asserted via source-text search to keep the test independent of the
    live Cognee / Qdrant infra (full from_config setup would need real
    adapters). Three substring checks cover all three wiring points; if
    any is lost in a refactor, the counterpart layer loses its tenant
    key and isolation silently breaks.

    Mirrors the pattern TF-FN-019 G11 uses for
    ``test_container_close_otel_provider`` — introspection as a
    pre-deploy contract lock.
    """
    src = inspect.getsource(RuntimeContainer.from_config)
    # (a) gw_id extracted (pre-existing; made order-sensitive by R2-P1).
    assert "gw_id = config.gateway.gateway_id" in src
    # (b) threaded into configure_cognee.
    assert "configure_cognee(config.cognee, config.llm, gateway_id=gw_id)" in src, (
        "configure_cognee must receive gateway_id=gw_id — otherwise "
        "Qdrant points written via add_data_points() won't carry the "
        "tenant id and cross-gateway isolation (TF-FN-018 G10) breaks."
    )
    # (c) threaded into VectorAdapter constructor.
    assert "VectorAdapter(config.cognee, gateway_id=gw_id)" in src, (
        "VectorAdapter must receive gateway_id=gw_id — otherwise "
        "search_similar won't add the database_name FieldCondition and "
        "cross-gateway dedup queries return other tenants' hits."
    )
    # (d) extraction MUST precede configure_cognee call — enforced by
    # finding gw_id before configure_cognee in the source.
    gw_extract_idx = src.index("gw_id = config.gateway.gateway_id")
    configure_cognee_idx = src.index("configure_cognee(config.cognee, config.llm")
    assert gw_extract_idx < configure_cognee_idx, (
        "gw_id extraction must come BEFORE configure_cognee call."
    )
