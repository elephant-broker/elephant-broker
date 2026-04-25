"""Tests for the startup safety guards in RuntimeContainer.from_config (Bucket A — A3/A4/A5).

These guards refuse to boot the runtime when the operator left a safety-critical
default in place:

* A3 — gateway.gateway_id must not be empty or "local" unless EB_ALLOW_DEFAULT_GATEWAY_ID=true
* A4 — cognee.neo4j_password must not be empty unless EB_DEV_MODE=true
* A5 — dataset rename forbidden once /var/lib/elephantbroker/.dataset_lock exists
       unless EB_ALLOW_DATASET_CHANGE=true
* A6 — gateway.gateway_id must not contain Redis-key or SCAN-glob metacharacters
       (``:``, ``*``, ``?``, ``[``, ``]``). No env-var opt-out: these characters
       are operationally unsafe in all environments. (#1516 RESOLVED)

Each test below uses ``monkeypatch.delenv``/``setenv`` to control the relevant
opt-out env var locally and prove the guard fires (or short-circuits) as
expected. Tests elsewhere that need to bypass the guards opt in explicitly
via the ``allow_default_gateway`` fixture defined in ``tests/conftest.py``;
the previous global ``os.environ.setdefault`` opt-out was removed in
Bucket A-R2-Test (TODO-3-343) so the suite no longer masks the guards by
default.
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


# ---------------------------------------------------------------------------
# A6 — gateway_id must not contain Redis-key or scan-glob metacharacters
# (#1516 RESOLVED in-PR — bundled with TF-FN-017 L1)
# ---------------------------------------------------------------------------


class TestGatewayIdForbiddenCharsStartupGuard:
    """A6: startup-safety guard for `gateway_id` metacharacters.

    No env-var opt-out (unlike A3/A4/A5) — colons and glob metachars in a
    gateway_id are operationally unsafe in all environments:

    * ``:`` in gateway_id produces `eb:{gw}:...` Redis keys whose namespace
      is ambiguous with a nested gateway (e.g., `"gw:prod"` prefix
      `eb:gw:prod` overlaps `"gw"` key family `eb:gw:prod:...`).
    * ``*``, ``?``, ``[``, ``]`` in gateway_id propagate verbatim into
      `RedisKeyBuilder.ws_snapshot_scan_pattern()` /
      `guard_history_scan_pattern()` outputs. A SCAN with such a pattern
      would match OTHER gateways' keys, breaking multi-tenant isolation.

    The check lives in `_validate_startup_safety()` so it fires at config
    load, before any adapter is constructed. `RedisKeyBuilder` intentionally
    stays permissive — defense in depth: validate at config load, trust at
    use site. See `redis_keys.py:test_redis_key_builder_accepts_any_gateway_id_*`
    for the paired permissive-contract test.
    """

    def test_a6_rejects_gateway_id_with_colons(self, monkeypatch):
        """Colon in `gateway_id` must raise UnsafeStartupConfigError."""
        monkeypatch.delenv("EB_ALLOW_DEFAULT_GATEWAY_ID", raising=False)
        config = _safe_config(gateway=GatewayConfig(gateway_id="gw:prod"))
        with pytest.raises(UnsafeStartupConfigError, match=r"forbidden characters.*':'"):
            _validate_startup_safety(config)

    @pytest.mark.parametrize("bad_id", ["gw*", "gw?", "gw[abc]", "gw]"])
    def test_a6_rejects_gateway_id_with_glob_metacharacters(self, bad_id, monkeypatch):
        """Redis glob metacharacters (`*`, `?`, `[`, `]`) in `gateway_id` must
        raise UnsafeStartupConfigError. Parametrized over the four characters
        that Redis treats as glob metachars in SCAN patterns.
        """
        monkeypatch.delenv("EB_ALLOW_DEFAULT_GATEWAY_ID", raising=False)
        config = _safe_config(gateway=GatewayConfig(gateway_id=bad_id))
        with pytest.raises(UnsafeStartupConfigError, match="forbidden characters"):
            _validate_startup_safety(config)
