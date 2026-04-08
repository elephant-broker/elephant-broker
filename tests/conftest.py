"""Top-level pytest fixtures shared by unit + integration tests.

Bucket A-R2-Test (TODO-3-343): the previous version of this file set the
three Bucket A safety-guard opt-out env vars (``EB_ALLOW_DEFAULT_GATEWAY_ID``,
``EB_DEV_MODE``, ``EB_ALLOW_DATASET_CHANGE``) unconditionally at process
start via ``os.environ.setdefault``. That global opt-in masked the guards
across the entire test suite — meaning a test that *should* have failed
because production code accidentally booted with the empty gateway_id
default would silently pass.

Per Rule 5.5 ("tests must verify behavior, not be adapted to fit broken
logic"), the global opt-outs are removed. Tests that genuinely need to
construct a ``RuntimeContainer`` from a bare ``ElephantBrokerConfig()``
must opt in explicitly via the ``allow_default_gateway`` fixture below.

Production hosts MUST NOT set these env vars. Tests that verify the
guards themselves fire (``test_container_startup_safety.py``) use
``monkeypatch.delenv``/``setenv`` per-test and do NOT need this fixture.
"""
import pytest


@pytest.fixture
def allow_default_gateway(monkeypatch):
    """Opt out of the Bucket A startup safety guards for a single test.

    Sets the three opt-out env vars that ``RuntimeContainer.from_config``
    consults during ``_validate_startup_safety``:

    * ``EB_ALLOW_DEFAULT_GATEWAY_ID=true`` — A3 bypass: allows the empty
      ``gateway.gateway_id`` default. Required when constructing a
      container from a bare ``ElephantBrokerConfig()`` without an
      explicit ``GatewayConfig(gateway_id=...)`` override.
    * ``EB_DEV_MODE=true`` — A4 bypass: allows the empty
      ``cognee.neo4j_password`` default. Required for the same reason
      as A3 — bare ``CogneeConfig()`` has no password set.
    * ``EB_ALLOW_DATASET_CHANGE=true`` — A5 bypass: allows the
      dataset-rename guard to short-circuit. The lock check is already
      a no-op when ``/var/lib/elephantbroker`` does not exist (typical
      test environment), but a developer running the suite on a host
      with a real install would otherwise hit it. Defensive belt-and-
      braces.

    Apply at the test class or module level via::

        pytestmark = pytest.mark.usefixtures("allow_default_gateway")

    or per-test by adding ``allow_default_gateway`` as a parameter.

    The ``monkeypatch`` dependency makes the env-var changes function-
    scoped: pytest restores them after the test, so this fixture cannot
    leak into adjacent tests that explicitly want to verify the guards
    fire (which the previous global ``setdefault`` could).
    """
    monkeypatch.setenv("EB_ALLOW_DEFAULT_GATEWAY_ID", "true")
    monkeypatch.setenv("EB_DEV_MODE", "true")
    monkeypatch.setenv("EB_ALLOW_DATASET_CHANGE", "true")
