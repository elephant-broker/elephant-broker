"""TF-FN-019 G15 FLIPPED — /health/ready uses VectorAdapter's public
``ping()`` method (#1189 RESOLVED — R2-P4).

Pre-R2-P4: ``api/routes/health.py:47`` reached into
``container.vector._get_client()`` — a leading-underscore name that
conventionally signals "internal implementation detail, not part of the
public API." Pinned as documented coupling.

R2-P4: VectorAdapter gained a public ``ping()`` method
(``elephantbroker/runtime/adapters/cognee/vector.py``) that performs the
connectivity probe (``_get_client + get_collections``) and raises on
failure. The health route now calls ``await container.vector.ping()``.

This test pins the post-fix shape:
* The health route source contains ``vector.ping(`` — the public call.
* The health route source does NOT contain ``vector._get_client(`` —
  the private coupling is closed.

If a future refactor reverts to direct private-method access (or
substitutes a different private API), this pin breaks and forces an
explicit re-evaluation. If the adapter contract evolves further
(e.g., ``ping()`` is renamed to ``health_check()``), update both this
test and the route in the same commit.

Cross-flow: TF-FN-012 (health endpoints) covers the success/failure
shape; this test pins the public-vs-private API surface.
"""
from __future__ import annotations

import inspect

from elephantbroker.api.routes import health


def test_health_ready_uses_vector_ping_public_method():
    """G15 FLIPPED (#1189 RESOLVED — R2-P4): the /health/ready route body
    references the public ``vector.ping()`` accessor and no longer
    references the private ``_get_client()`` accessor.
    """
    src = inspect.getsource(health)
    assert "vector.ping(" in src, (
        "The /health/ready endpoint no longer references "
        "`vector.ping()`. If the adapter contract was refactored "
        "(e.g., the public probe method was renamed), update this "
        "test and the health route in the same commit."
    )
    assert "vector._get_client(" not in src, (
        "The /health/ready endpoint regressed to using the private "
        "`vector._get_client()` accessor. Use the public `ping()` "
        "method instead — see VectorAdapter.ping() for the contract."
    )
