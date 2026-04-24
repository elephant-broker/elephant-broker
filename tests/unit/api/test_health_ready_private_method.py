"""TF-FN-019 G15 — /health/ready couples to VectorAdapter's private
``_get_client()`` method.

PROD #1189 pin. ``api/routes/health.py:47`` reaches into
``container.vector._get_client()`` — a leading-underscore name that
conventionally signals "internal implementation detail, not part of the
public API."

Consequences:
* The health endpoint breaks silently if VectorAdapter is refactored to
  rename / remove ``_get_client`` — because refactors of private methods
  don't go through a deprecation cycle.
* Substituting a different vector adapter (e.g., for tests or for a
  future multi-backend configuration) requires the same private method
  contract to be honored, defeating the purpose of the IVectorAdapter
  interface.

Pin the coupling so any future refactor that drops ``_get_client``
surfaces as a broken pin test and forces either a public accessor
(e.g., a ``ping()`` method on the adapter interface) or a direct Qdrant
client dep injection.

Cross-flow: TF-FN-012 (health endpoints) covered the success/failure
shape; this test adds the private-method coupling surface that was
deferred.
"""
from __future__ import annotations

import inspect

from elephantbroker.api.routes import health


def test_health_ready_calls_vector_private_get_client():
    """G15 (#1189): the /health/ready route body contains a direct
    reference to ``vector._get_client()``. Pin this string-level coupling
    so a refactor drop forces an explicit endpoint update.
    """
    src = inspect.getsource(health)
    # The health.py module body must contain the private-method call.
    assert "vector._get_client" in src, (
        "The /health/ready endpoint no longer references "
        "`vector._get_client()`. If it was switched to a public accessor "
        "or the adapter gained a ping() method, update this test and "
        "remove the #1189 pin — the private-method coupling is closed."
    )
