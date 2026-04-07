"""Tests for the startup safety guards in RuntimeContainer.from_config (Bucket A — A3/A4/A5).

These guards refuse to boot the runtime when the operator left a safety-critical
default in place:

* A3 — gateway.gateway_id must not be empty or "local" unless EB_ALLOW_DEFAULT_GATEWAY_ID=true
* A4 — cognee.neo4j_password must not be empty unless EB_DEV_MODE=true
* A5 — dataset rename forbidden once /var/lib/elephantbroker/.dataset_lock exists
       unless EB_ALLOW_DATASET_CHANGE=true

The opt-out env vars are set unconditionally in tests/conftest.py for the rest
of the test suite. Each test below clears the relevant opt-out locally to
prove the guard fires.
"""
from __future__ import annotations

import os
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from elephantbroker.runtime.container import (
    RuntimeContainer,
    UnsafeStartupConfigError,
    _validate_startup_safety,
)
from elephantbroker.schemas.config import CogneeConfig, ElephantBrokerConfig, GatewayConfig
from elephantbroker.schemas.tiers import BusinessTier


@pytest.fixture(autouse=True)
def _mock_configure_cognee():
    """Stub configure_cognee so the validator runs without touching the network."""
    with patch("elephantbroker.runtime.container.configure_cognee", new_callable=AsyncMock):
        yield


def _safe_config(**kwargs) -> ElephantBrokerConfig:
    """Build a config with the minimum safety fields populated.

    Callers may override `cognee` or `gateway` via kwargs; we pop them so the
    explicit args below don't collide with the keyword arguments.
    """
    cognee = kwargs.pop("cognee", CogneeConfig(neo4j_password="test-password"))
    gateway = kwargs.pop("gateway", GatewayConfig(gateway_id="test-gateway"))
    return ElephantBrokerConfig(cognee=cognee, gateway=gateway, **kwargs)


# ---------------------------------------------------------------------------
# A3 — gateway_id default refusal
# ---------------------------------------------------------------------------


class TestGatewayIdStartupGuard:
    def test_empty_gateway_id_refuses_boot(self, monkeypatch):
        monkeypatch.delenv("EB_ALLOW_DEFAULT_GATEWAY_ID", raising=False)
        config = _safe_config(gateway=GatewayConfig(gateway_id=""))
        with pytest.raises(UnsafeStartupConfigError, match="gateway_id"):
            _validate_startup_safety(config)

    def test_local_sentinel_gateway_id_refuses_boot(self, monkeypatch):
        monkeypatch.delenv("EB_ALLOW_DEFAULT_GATEWAY_ID", raising=False)
        config = _safe_config(gateway=GatewayConfig(gateway_id="local"))
        with pytest.raises(UnsafeStartupConfigError, match="gateway_id"):
            _validate_startup_safety(config)

    def test_real_gateway_id_passes(self, monkeypatch):
        monkeypatch.delenv("EB_ALLOW_DEFAULT_GATEWAY_ID", raising=False)
        config = _safe_config(gateway=GatewayConfig(gateway_id="gw-prod-eu1"))
        _validate_startup_safety(config)  # no exception

    def test_opt_out_allows_default(self, monkeypatch):
        monkeypatch.setenv("EB_ALLOW_DEFAULT_GATEWAY_ID", "true")
        config = _safe_config(gateway=GatewayConfig(gateway_id="local"))
        _validate_startup_safety(config)  # no exception

    @pytest.mark.asyncio
    async def test_from_config_propagates_refusal(self, monkeypatch):
        monkeypatch.delenv("EB_ALLOW_DEFAULT_GATEWAY_ID", raising=False)
        config = _safe_config(gateway=GatewayConfig(gateway_id=""))
        with pytest.raises(UnsafeStartupConfigError, match="gateway_id"):
            await RuntimeContainer.from_config(config, BusinessTier.FULL)


# ---------------------------------------------------------------------------
# A4 — empty neo4j_password refusal
# ---------------------------------------------------------------------------


class TestNeo4jPasswordStartupGuard:
    def test_empty_password_refuses_boot(self, monkeypatch):
        monkeypatch.delenv("EB_DEV_MODE", raising=False)
        config = _safe_config(cognee=CogneeConfig(neo4j_password=""))
        with pytest.raises(UnsafeStartupConfigError, match="neo4j_password"):
            _validate_startup_safety(config)

    def test_real_password_passes(self, monkeypatch):
        monkeypatch.delenv("EB_DEV_MODE", raising=False)
        config = _safe_config(cognee=CogneeConfig(neo4j_password="hunter2-prod"))
        _validate_startup_safety(config)

    def test_dev_mode_allows_empty_password(self, monkeypatch):
        monkeypatch.setenv("EB_DEV_MODE", "true")
        config = _safe_config(cognee=CogneeConfig(neo4j_password=""))
        _validate_startup_safety(config)


# ---------------------------------------------------------------------------
# A5 — dataset rename refusal
# ---------------------------------------------------------------------------


class TestDatasetLockStartupGuard:
    def test_no_data_dir_is_noop(self, tmp_path, monkeypatch):
        """When /var/lib/elephantbroker doesn't exist, the lock check no-ops gracefully."""
        # Point the data path at a non-existent location
        monkeypatch.setattr("elephantbroker.runtime.container._DATA_DIR_PATH", tmp_path / "missing")
        monkeypatch.setattr(
            "elephantbroker.runtime.container._DATASET_LOCK_FILE",
            tmp_path / "missing" / ".dataset_lock",
        )
        monkeypatch.delenv("EB_ALLOW_DATASET_CHANGE", raising=False)
        config = _safe_config(cognee=CogneeConfig(neo4j_password="x", default_dataset="custom"))
        _validate_startup_safety(config)  # no exception

    def test_first_boot_writes_lock_file(self, tmp_path, monkeypatch):
        data_dir = tmp_path / "var-lib"
        data_dir.mkdir()
        lock_file = data_dir / ".dataset_lock"
        monkeypatch.setattr("elephantbroker.runtime.container._DATA_DIR_PATH", data_dir)
        monkeypatch.setattr("elephantbroker.runtime.container._DATASET_LOCK_FILE", lock_file)
        monkeypatch.delenv("EB_ALLOW_DATASET_CHANGE", raising=False)

        config = _safe_config(cognee=CogneeConfig(neo4j_password="x", default_dataset="my-dataset"))
        _validate_startup_safety(config)
        assert lock_file.read_text() == "my-dataset"

    def test_matching_lock_file_passes(self, tmp_path, monkeypatch):
        data_dir = tmp_path / "var-lib"
        data_dir.mkdir()
        lock_file = data_dir / ".dataset_lock"
        lock_file.write_text("my-dataset")
        monkeypatch.setattr("elephantbroker.runtime.container._DATA_DIR_PATH", data_dir)
        monkeypatch.setattr("elephantbroker.runtime.container._DATASET_LOCK_FILE", lock_file)
        monkeypatch.delenv("EB_ALLOW_DATASET_CHANGE", raising=False)

        config = _safe_config(cognee=CogneeConfig(neo4j_password="x", default_dataset="my-dataset"))
        _validate_startup_safety(config)  # no exception

    def test_mismatched_lock_file_refuses_boot(self, tmp_path, monkeypatch):
        data_dir = tmp_path / "var-lib"
        data_dir.mkdir()
        lock_file = data_dir / ".dataset_lock"
        lock_file.write_text("old-dataset")
        monkeypatch.setattr("elephantbroker.runtime.container._DATA_DIR_PATH", data_dir)
        monkeypatch.setattr("elephantbroker.runtime.container._DATASET_LOCK_FILE", lock_file)
        monkeypatch.delenv("EB_ALLOW_DATASET_CHANGE", raising=False)

        config = _safe_config(cognee=CogneeConfig(neo4j_password="x", default_dataset="new-dataset"))
        with pytest.raises(UnsafeStartupConfigError, match="dataset"):
            _validate_startup_safety(config)

    def test_opt_out_allows_dataset_change(self, tmp_path, monkeypatch):
        data_dir = tmp_path / "var-lib"
        data_dir.mkdir()
        lock_file = data_dir / ".dataset_lock"
        lock_file.write_text("old-dataset")
        monkeypatch.setattr("elephantbroker.runtime.container._DATA_DIR_PATH", data_dir)
        monkeypatch.setattr("elephantbroker.runtime.container._DATASET_LOCK_FILE", lock_file)
        monkeypatch.setenv("EB_ALLOW_DATASET_CHANGE", "true")

        config = _safe_config(cognee=CogneeConfig(neo4j_password="x", default_dataset="new-dataset"))
        _validate_startup_safety(config)  # no exception
