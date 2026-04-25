"""TF-FN-018 G1 — audit that every Prometheus metric increment carries a
``gateway_id`` label.

The gateway-isolation contract requires every metric emitted by the runtime
to be scoped by ``gateway_id`` so operators can filter dashboards per tenant.
``MetricsContext`` (``elephantbroker/runtime/metrics.py``) is the canonical
entry point. Some ``inc_*`` methods delegate to a module-level function that
takes a ``gateway_id=`` kwarg; others emit directly via the underlying
``eb_*`` Counter/Histogram with ``.labels(gateway_id=self._gw, ...)``. Both
patterns are valid — the invariant we pin is that the METHOD SOURCE carries
a ``gateway_id`` reference tied to ``self._gw``.

This test walks ``MetricsContext``'s ``inc_*`` methods via introspection and
asserts each method's source contains ``gateway_id=self._gw`` — either in a
``.labels(...)`` call or in a module-function-delegation call.

Historic bugs this would catch:
* TD-65 follow-up: the ``inc_session_boundary`` label was renamed ``action``
  -> ``event`` without a sibling audit test. A similar rename that also
  drops ``gateway_id`` would silently ship today without this test.
* Any new ``inc_*`` method added to ``MetricsContext`` that forgets to
  thread ``gateway_id`` through (direct or delegated).
"""
from __future__ import annotations

import inspect

import pytest

from elephantbroker.runtime.metrics import MetricsContext


def _collect_inc_methods() -> list[str]:
    """All public ``inc_*`` methods on ``MetricsContext``."""
    return [
        name for name, _obj in inspect.getmembers(MetricsContext, inspect.isfunction)
        if name.startswith("inc_") and not name.startswith("_")
    ]


def test_all_inc_methods_carry_gateway_id_label():
    """G1 (TF-FN-018): every ``MetricsContext.inc_*`` method must carry a
    ``gateway_id`` reference tied to ``self._gw``.

    Two valid emission patterns, both exercised across the class:

      (a) Delegation: ``inc_store(...)`` -> ``metrics.inc_store(..., gateway_id=self._gw)``
      (b) Direct:     ``inc_facts_stored(...)`` -> ``eb_facts_stored_total.labels(gateway_id=self._gw, ...)``

    Rather than branching on which shape each method uses, we grep the
    METHOD SOURCE for the literal string ``gateway_id=self._gw`` — both
    shapes contain it. If a method-body emits without the label, this
    test fails with the method name so the gap is visible.

    The only ``MetricsContext`` method where this rule doesn't apply:
    ``__init__`` (no ``inc_`` prefix, excluded by the collector).
    """
    methods = _collect_inc_methods()
    # Sanity check on introspection — catches a fixture break masking as GREEN.
    assert len(methods) >= 15, f"Expected >=15 inc_* methods, got {methods!r}"

    missing_gateway_ref: list[str] = []
    for method_name in methods:
        method = getattr(MetricsContext, method_name)
        try:
            src = inspect.getsource(method)
        except (OSError, TypeError):
            # Couldn't read source for this method — treat as failure so the
            # audit doesn't silently skip anything.
            missing_gateway_ref.append(f"{method_name} (source unavailable)")
            continue
        if "gateway_id=self._gw" not in src:
            missing_gateway_ref.append(method_name)

    assert not missing_gateway_ref, (
        f"MetricsContext.inc_* methods missing gateway_id=self._gw in their "
        f"source: {missing_gateway_ref}. Every metric increment must carry "
        f"the gateway_id label, either via delegation to a module-level "
        f"function with gateway_id kwarg, or via direct .labels(gateway_id=self._gw, ...)."
    )
