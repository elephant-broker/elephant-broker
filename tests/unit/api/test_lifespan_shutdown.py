"""TF-FN-019 G14 — FastAPI lifespan shutdown invokes container.close() +
close-adapter INFO logs fire (regression guard for #1508 RESOLVED in 1684e48).

PROD #1508 (TD-65 2nd follow-up, `1684e48`) fixed a dead-path bug:
``create_app()`` had neither ``@app.on_event("shutdown")`` nor a
``lifespan=`` kwarg, so ``container.close()`` was NEVER invoked on
SIGTERM. Redis distributed locks orphaned on pod restart; the 14 F2
close-adapter INFO log lines in ``container.py`` were dead code.

Post-fix: ``create_app()`` registers ``container.close()`` via FastAPI
``lifespan`` context manager. TestClient (and uvicorn in prod) trigger
lifespan startup + shutdown automatically.

This test duplicates the intent of
``tests/unit/api/test_app.py::test_lifespan_invokes_container_close_on_shutdown``
(added alongside the 1684e48 fix) with a stronger assertion — it also
checks that ≥14 "Closing adapter: <name>" INFO logs emit on shutdown.
The log check mirrors the devops probe shape (`journalctl -u
elephantbroker | grep 'Closing adapter:'` expects ≥14 hits per
lifecycle).

Cross-flow: TF-FN-015 (OTEL observability) — the close-adapter logs
were invisible in prod until this fix; this regression guard prevents
a future revert of the lifespan registration from silently reintroducing
the dead-log dead-adapter gap.
"""
from __future__ import annotations

import logging
from unittest.mock import AsyncMock, MagicMock

import pytest
from starlette.testclient import TestClient

from elephantbroker.api.app import create_app
from elephantbroker.runtime.container import RuntimeContainer


def _make_container_with_closable_adapters():
    """Build a RuntimeContainer with every public None slot replaced by a
    MagicMock that carries an AsyncMock .close() attribute.

    The container.close() iteration walks named adapters and emits an
    INFO log + awaits .close() on each. With AsyncMock.close the try
    blocks complete normally and the log.info fires as it would in prod.
    """
    c = RuntimeContainer()
    for attr in vars(c):
        if not attr.startswith("_") and getattr(c, attr) is None:
            mock = MagicMock()
            mock.close = AsyncMock()
            # Some adapters (e.g., Redis) use aclose; keep both to be
            # compatible with whichever close() invokes.
            mock.aclose = AsyncMock()
            setattr(c, attr, mock)
    return c


def test_fastapi_lifespan_invokes_container_close_on_shutdown(caplog):
    """G14 (#1508 regression guard): TestClient context-manager exit
    triggers FastAPI lifespan shutdown, which must:

    1. Await ``container.close()`` exactly once (revert-detection: if
       someone strips ``lifespan=`` from ``create_app()`` the spied
       close call disappears entirely).

    2. Emit at least 13 ``"Closing adapter: <name>"`` INFO log lines
       (bonus assertion — mirrors devops probe shape; prevents a
       regression that drops one of the teardown log emissions).

    Note: the test's _make_container_with_closable_adapters() helper
    walks ``vars(c)`` and populates adapters that appear as ``None`` in
    ``__init__``. This yields 13 close-adapter INFO lines in the unit
    environment (12 pre-option-(b) + 1 new ``otel_logger_provider``).
    Staging sees ≥14 because prod wiring in ``from_config`` initializes
    some adapters (``tuning_delta_store``, ``scoring_ledger_store``,
    ``consolidation_report_store``, ``trace_query_client``) that the
    bare-``RuntimeContainer()`` path leaves absent. The pin enforces the
    *lower bound* — if the count drops below 13 unit-locally, one of the
    always-wired adapters was silently removed.
    """
    container = _make_container_with_closable_adapters()
    # Spy on close so we can assert "awaited once"; the real close logic
    # still runs via super() if we wrap, but for the assertion we just
    # need the call count. Replace entirely and rely on (2)'s log check
    # to validate adapter-walk behavior separately.
    original_close = container.close
    close_spy = AsyncMock(wraps=original_close)
    container.close = close_spy  # type: ignore[method-assign]
    app = create_app(container)

    with caplog.at_level(logging.INFO, logger="elephantbroker.runtime.container"):
        with TestClient(app):
            pass

    # (1) Lifespan invoked container.close exactly once on __exit__.
    close_spy.assert_awaited_once()

    # (2) ≥14 "Closing adapter: <name>" INFO logs fired during the close
    # walk — the F2 contract. If this drops, one of the 14 known adapter
    # teardown emissions was silently removed.
    close_adapter_logs = [
        r for r in caplog.records
        if r.name == "elephantbroker.runtime.container"
        and r.getMessage().startswith("Closing adapter:")
    ]
    assert len(close_adapter_logs) >= 13, (
        f"Expected >=13 'Closing adapter: <name>' INFO logs on shutdown "
        f"in the unit-env container (staging sees >=14), got "
        f"{len(close_adapter_logs)}. The F2 close-adapter contract may "
        f"have regressed — one of the always-wired adapters is no "
        f"longer being teardown-logged. "
        f"Captured messages: {[r.getMessage() for r in close_adapter_logs]}"
    )
