"""Adapter that runs scenarios through a real OpenClaw gateway."""
from __future__ import annotations

from tests.scenarios.base import Scenario
from tests.scenarios.openclaw_client import OpenClawClient
from tests.e2e.gateway_simulator.simulator import OpenClawGatewaySimulator


class LiveScenario(Scenario):
    """Scenario subclass that sends traffic through a real OpenClaw gateway.

    In live mode:
    - User messages go through OpenClawClient -> gateway WebSocket -> agent -> EB plugins
    - Trace verification still queries the EB runtime directly via simulator's httpx client
    - The simulator is used ONLY for trace inspection (not for sending traffic)
    """

    def __init__(self, gateway_url: str, gateway_token: str,
                 eb_runtime_url: str = "http://localhost:8420",
                 agent_id: str = "main", gateway_id: str = "local"):
        super().__init__(base_url=eb_runtime_url, gateway_id=gateway_id)
        self.oc_client = OpenClawClient(gateway_url, gateway_token, agent_id)
        self._session_key: str | None = None

    async def setup(self) -> None:
        await self.oc_client.connect()
        self._session_key = await self.oc_client.create_session()

    async def send_user_message(self, message: str) -> dict:
        """Send a message through the real OpenClaw gateway and wait for response."""
        return await self.oc_client.send_and_wait(self._session_key, message)

    async def get_conversation_history(self) -> list[dict]:
        """Get the conversation history from OpenClaw."""
        return await self.oc_client.get_history(self._session_key)

    async def teardown(self) -> None:
        if self._session_key:
            try:
                await self.oc_client.delete_session(self._session_key)
            except Exception:
                pass
        await self.oc_client.close()
