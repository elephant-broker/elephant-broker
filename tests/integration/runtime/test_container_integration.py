"""Integration tests for RuntimeContainer with real infrastructure."""
from __future__ import annotations

import pytest

from elephantbroker.runtime.container import RuntimeContainer
from elephantbroker.schemas.config import ElephantBrokerConfig
from elephantbroker.schemas.tiers import BusinessTier


@pytest.mark.integration
class TestContainerIntegration:
    async def test_container_from_config_all_modules_live(self):
        config = ElephantBrokerConfig.load()
        container = await RuntimeContainer.from_config(config, BusinessTier.FULL)
        try:
            assert container.trace_ledger is not None
            assert container.profile_registry is not None
            assert container.actor_registry is not None
            assert container.memory_store is not None
        finally:
            await container.close()

    async def test_container_close_cleans_up_connections(self):
        config = ElephantBrokerConfig.load()
        container = await RuntimeContainer.from_config(config, BusinessTier.FULL)
        await container.close()
        # After close, graph driver should be None
        assert container.graph._driver is None
