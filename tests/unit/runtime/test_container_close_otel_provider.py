"""TF-FN-019 G11 — RuntimeContainer.close() shuts down the OTEL
LoggerProvider so buffered LogRecords are flushed on SIGTERM.

PROD #1181 RESOLVED in-PR (option (b) inline fix). Before this commit,
the OTEL ``LoggerProvider`` that ``runtime/observability.py:setup_otel_logging()``
constructed was architecturally orphaned — the function returned only
the ``Logger`` instance, dropping the ``LoggerProvider`` reference.
``BatchLogRecordProcessor`` buffers records up to 5s before export; on
SIGTERM the buffer was lost because nothing called
``LoggerProvider.shutdown()``.

Post-fix:
1. ``setup_otel_logging()`` now returns ``(logger, provider)``.
2. ``RuntimeContainer`` holds the provider as
   ``self.otel_logger_provider`` (initialized ``None``).
3. ``RuntimeContainer.close()`` iteration gains an
   ``otel_logger_provider`` entry that logs
   ``"Closing adapter: otel_logger_provider"`` and invokes
   ``provider.shutdown()`` (inside try/except for best-effort shutdown).

TraceLedger itself remains closeless — that is intentional: the
LoggerProvider is what owns the OTEL buffer, and separating the two
keeps ``TraceLedger`` a pure in-memory ring with no IO shape.

Devops F2 probe: previously 14 ``"Closing adapter: <name>"`` lines per
lifecycle; post-fix expect 15 when OTEL logging is enabled.
"""
from __future__ import annotations

import logging
from unittest.mock import MagicMock

import pytest

from elephantbroker.runtime.container import RuntimeContainer


async def test_container_close_invokes_otel_logger_provider_shutdown(caplog):
    """G11 (#1181 RESOLVED, option b): the OTEL LoggerProvider shutdown
    hook fires when ``RuntimeContainer.close()`` runs.

    Constructs a minimal container with a spy ``otel_logger_provider``
    and awaits ``close()`` directly. Asserts:
      (1) ``provider.shutdown()`` called exactly once.
      (2) ``"Closing adapter: otel_logger_provider"`` INFO log emitted.

    The test does NOT exercise the full from_config wiring — that would
    require network-fronted adapters. The unit-level close() walk is
    the contract this test pins.
    """
    container = RuntimeContainer()
    provider_spy = MagicMock()
    container.otel_logger_provider = provider_spy

    with caplog.at_level(logging.INFO, logger="elephantbroker.runtime.container"):
        await container.close()

    # (1) shutdown() was called exactly once.
    provider_spy.shutdown.assert_called_once_with()

    # (2) The expected INFO log line fired.
    close_adapter_logs = [
        r.getMessage() for r in caplog.records
        if r.name == "elephantbroker.runtime.container"
        and r.getMessage().startswith("Closing adapter:")
    ]
    assert "Closing adapter: otel_logger_provider" in close_adapter_logs, (
        f"Expected 'Closing adapter: otel_logger_provider' INFO log; "
        f"captured close-adapter messages: {close_adapter_logs!r}"
    )


async def test_container_close_tolerates_missing_otel_provider():
    """G11 side-assertion: when OTEL logging is not enabled (config
    lacked endpoint, or exporter package not installed),
    ``otel_logger_provider`` stays ``None`` and ``close()`` must still
    complete cleanly — no AttributeError, no extra log.

    Prevents a regression where the new close-site block accidentally
    assumes the provider is always set.
    """
    container = RuntimeContainer()
    # otel_logger_provider defaults to None per __init__ change.
    assert container.otel_logger_provider is None
    await container.close()  # no raise


@pytest.mark.asyncio
async def test_close_logs_swallowed_exception_at_debug(caplog):
    """L4: container.close() logs adapter close failures at DEBUG level
    instead of silently swallowing them with bare `except: pass`."""
    from unittest.mock import AsyncMock
    container = RuntimeContainer()
    mock_redis = AsyncMock()
    mock_redis.aclose = AsyncMock(side_effect=ConnectionError("redis gone"))
    container.redis = mock_redis

    with caplog.at_level(logging.DEBUG, logger="elephantbroker.runtime.container"):
        await container.close()

    assert "Close failed for redis" in caplog.text
    assert "redis gone" in caplog.text
