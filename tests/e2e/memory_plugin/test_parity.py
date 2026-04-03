"""E2E parity tests using the gateway simulator.

These tests require a running ElephantBroker runtime with all backends.
Run with: pytest tests/e2e/ -v -m integration
"""
import pytest

from tests.e2e.gateway_simulator.simulator import OpenClawGatewaySimulator

BASE_URL = "http://localhost:8420"


@pytest.fixture
async def simulator():
    sim = OpenClawGatewaySimulator(BASE_URL)
    yield sim
    await sim.close()


@pytest.mark.integration
class TestMemoryPluginParity:
    async def test_tool_memory_store_persists(self, simulator):
        result = await simulator.simulate_tool_memory_store("Test fact for parity", "general")
        assert "id" in result

    async def test_session_lifecycle(self, simulator):
        await simulator.simulate_session_start()
        result = await simulator.simulate_session_end()
        assert "session_key" in result

    async def test_status_reports_correctly(self, simulator):
        r = await simulator.client.get("/memory/status")
        assert r.status_code == 200
