"""R2-P7 / #1485 — pin: all 3 callers pass ``str(eb_id)`` to
``VectorAdapter.delete_embedding``, never the Cognee internal
``cognee_data_id``.

Background: VectorAdapter.delete_embedding takes an ``id`` argument
that is forwarded verbatim to Qdrant's PointIdsList. ID-format
mismatch (eb_id string vs cognee_data_id UUID) is the caller's
responsibility — the adapter contract documents this as
documented PROD risk #1485 (TF-FN-008 G6).

The 3 callers in production:

1. ``runtime/memory/facade.py:890`` —
   ``await self._vector.delete_embedding(_FACTS_COLLECTION, str(fact_id))``
2. ``runtime/consolidation/engine.py:658`` —
   ``await self._vector.delete_embedding("FactDataPoint_text", fact_id)``
   (``fact_id`` is already a string from the calling context).
3. ``runtime/consolidation/stages/canonicalize.py:322`` —
   ``await self._vector.delete_embedding("FactDataPoint_text", str(member.id))``

This test pins the 3 sites at the source level via ``inspect``
substring checks — a refactor that switched to passing
``cognee_data_id`` (without the eb_id-style stringification)
would surface as a regression here. Pure documentation/source
contract; no runtime mock involved.

#1485 is **already resolved at the call-site level** — this pin
guards against a future regression.
"""
from __future__ import annotations

import inspect

from elephantbroker.runtime.consolidation import engine as consolidation_engine
from elephantbroker.runtime.consolidation.stages import canonicalize as canonicalize_stage
from elephantbroker.runtime.memory import facade as memory_facade


def test_all_three_callers_pass_str_eb_id_to_delete_embedding():
    """G_1485 (R2-P7): the 3 production callers of
    ``vector.delete_embedding`` pass an eb_id-shaped string, NOT the
    cognee-internal data UUID. Source-level pin so a regression that
    re-introduces ``cognee_data_id`` plumbing surfaces immediately.
    """
    # Caller 1: memory facade — ``str(fact_id)``.
    facade_src = inspect.getsource(memory_facade)
    assert "delete_embedding(_FACTS_COLLECTION, str(fact_id))" in facade_src

    # Caller 2: consolidation engine — ``fact_id`` (already string).
    engine_src = inspect.getsource(consolidation_engine)
    assert 'delete_embedding("FactDataPoint_text", fact_id)' in engine_src
    # Defense-in-depth: must NOT pass cognee_data_id to delete_embedding.
    assert "delete_embedding(\"FactDataPoint_text\", cognee_data_id" not in engine_src

    # Caller 3: canonicalize stage — ``str(member.id)``.
    canon_src = inspect.getsource(canonicalize_stage)
    assert 'delete_embedding("FactDataPoint_text", str(member.id))' in canon_src
