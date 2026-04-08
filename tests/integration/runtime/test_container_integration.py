"""Integration tests for RuntimeContainer with real infrastructure."""
from __future__ import annotations

import pytest

from elephantbroker.runtime.container import RuntimeContainer
from elephantbroker.schemas.config import ElephantBrokerConfig
from elephantbroker.schemas.tiers import BusinessTier


# R2 integration RED fix (cascade fallout from TODO-3-343 / Bucket A-R2-Test):
# These two test methods call RuntimeContainer.from_config() directly inside
# the test body (no fixture indirection), so each method takes a `monkeypatch`
# parameter individually and seeds EB_GATEWAY_ID before the load. Bucket
# A-R2-Test removed the global EB_ALLOW_DEFAULT_GATEWAY_ID opt-out from
# tests/conftest.py and scoped it to the unit-side test_container.py only;
# integration tests are now subject to the Bucket A startup safety check
# (R1 `d850186`) and need to set a real gateway_id. Same pattern as the I-R2
# fix to tests/integration/runtime/working_set/test_working_set_integration.py.
@pytest.mark.integration
class TestContainerIntegration:
    async def test_container_from_config_all_modules_live(self, monkeypatch):
        monkeypatch.setenv("EB_GATEWAY_ID", "test-container-int-gateway")
        config = ElephantBrokerConfig.load()
        container = await RuntimeContainer.from_config(config, BusinessTier.FULL)
        try:
            assert container.trace_ledger is not None
            assert container.profile_registry is not None
            assert container.actor_registry is not None
            assert container.memory_store is not None
        finally:
            await container.close()

    async def test_container_close_cleans_up_connections(self, monkeypatch):
        monkeypatch.setenv("EB_GATEWAY_ID", "test-container-int-gateway")
        config = ElephantBrokerConfig.load()
        container = await RuntimeContainer.from_config(config, BusinessTier.FULL)
        await container.close()
        # After close, graph driver should be None
        assert container.graph._driver is None
