"""Phase 5 E2E lifecycle test — working set, session goals, procedures.

Exercises the full Phase 5 lifecycle via in-process ASGI transport (no external server needed):
  1. session_start
  2. ingest 3 turns
  3. create session goal + sub-goal
  4. list goals (verify hierarchy)
  5. build_working_set
  6. create procedure
  7. update goal status, record progress
  8. session_end (verify goals flushed)

Run with: pytest tests/e2e/gateway_simulator/test_phase5_lifecycle.py -v -m integration
Requires: Docker infrastructure (Neo4j, Qdrant, Redis) via run-integration-tests.sh
"""
from __future__ import annotations

import pytest
import pytest_asyncio

from elephantbroker.schemas.config import ElephantBrokerConfig


@pytest_asyncio.fixture(autouse=True)
async def reset_cognee_cache():
    """Clear Cognee's cached graph engine to avoid stale event loop errors."""
    try:
        from cognee.infrastructure.databases.graph.get_graph_engine import _create_graph_engine
        _create_graph_engine.cache_clear()
    except Exception:
        pass
    yield
    try:
        from cognee.infrastructure.databases.graph.get_graph_engine import _create_graph_engine
        _create_graph_engine.cache_clear()
    except Exception:
        pass


@pytest_asyncio.fixture
async def app():
    """Create a fully wired FastAPI app with real infrastructure (per test)."""
    from elephantbroker.api.app import create_app
    from elephantbroker.runtime.container import RuntimeContainer
    from elephantbroker.schemas.tiers import BusinessTier

    config = ElephantBrokerConfig.load()
    container = await RuntimeContainer.from_config(config, tier=BusinessTier.FULL)
    application = create_app(container)
    yield application
    try:
        await container.close()
    except Exception:
        pass


@pytest_asyncio.fixture
async def simulator(app):
    """Gateway simulator using in-process ASGI transport — no external server needed."""
    import httpx
    from tests.e2e.gateway_simulator.simulator import OpenClawGatewaySimulator

    transport = httpx.ASGITransport(app=app)
    sim = OpenClawGatewaySimulator.__new__(OpenClawGatewaySimulator)
    sim.client = httpx.AsyncClient(transport=transport, base_url="http://testserver", timeout=30.0)
    sim.session_key = "agent:main:main"
    import uuid
    sim.session_id = str(uuid.uuid4())
    yield sim
    await sim.client.aclose()


@pytest.mark.integration
class TestPhase5Lifecycle:
    """Full Phase 5 lifecycle: goals, working set, procedures, session end."""

    async def test_full_lifecycle(self, simulator):
        # ── Step 1: session_start ──
        await simulator.simulate_session_start()

        # ── Step 2: create session goal ──
        goal = await simulator.simulate_session_goals_create("Set up Python project")
        assert "id" in goal, f"Expected 'id' in goal response, got: {goal}"
        root_goal_id = goal["id"]

        # ── Step 3: create sub-goal ──
        sub_goal = await simulator.simulate_session_goals_create(
            "Configure testing framework",
            parent_goal_id=root_goal_id,
        )
        assert "id" in sub_goal
        sub_goal_id = sub_goal["id"]

        # ── Step 4: list goals — verify hierarchy ──
        goals_resp = await simulator.simulate_session_goals_list()
        assert "goals" in goals_resp
        goals = goals_resp["goals"]
        assert len(goals) >= 2
        goal_ids = {g["id"] for g in goals}
        assert root_goal_id in goal_ids
        assert sub_goal_id in goal_ids

        # ── Step 5: build_working_set ──
        ws = await simulator.simulate_build_working_set("Python project setup")
        assert "items" in ws or "session_id" in ws

        # ── Step 6: create procedure ──
        proc = await simulator.simulate_procedure_create(
            "Deploy Python Project",
            steps=[
                {"order": 0, "instruction": "Create virtual environment"},
                {"order": 1, "instruction": "Install dependencies"},
            ],
        )
        assert "id" in proc or "name" in proc

        # ── Step 7: update sub-goal status to completed ──
        updated = await simulator.simulate_session_goals_update_status(
            sub_goal_id, "completed", evidence="pytest configured in pyproject.toml",
        )
        assert updated.get("status") == "completed"

        # ── Step 8: record progress on root goal ──
        progress = await simulator.simulate_session_goals_progress(
            root_goal_id, evidence="Sub-task completed",
        )
        assert "id" in progress

        # ── Step 9: session_end ──
        end = await simulator.simulate_session_end()
        assert "session_key" in end
        assert "goals_flushed" in end
        assert isinstance(end["goals_flushed"], int)

    async def test_goal_blocker_lifecycle(self, simulator):
        """Test adding a blocker to a goal."""
        await simulator.simulate_session_start()

        goal = await simulator.simulate_session_goals_create("Goal with blocker")
        goal_id = goal["id"]

        blocked = await simulator.simulate_session_goals_add_blocker(
            goal_id, "Waiting for CI pipeline",
        )
        assert "blockers" in blocked
        assert "Waiting for CI pipeline" in blocked["blockers"]

        goals_resp = await simulator.simulate_session_goals_list()
        g = next(g for g in goals_resp["goals"] if g["id"] == goal_id)
        assert len(g["blockers"]) >= 1

        await simulator.simulate_session_end()

    async def test_procedure_activate_and_status(self, simulator):
        """Test procedure activation and session status tracking."""
        await simulator.simulate_session_start()

        proc = await simulator.simulate_procedure_create(
            "Review Code",
            steps=[
                {"order": 0, "instruction": "Read the diff"},
                {"order": 1, "instruction": "Check for regressions"},
            ],
        )
        proc_id = proc.get("id")
        assert proc_id is not None

        activation = await simulator.simulate_procedure_activate(proc_id)
        assert "execution_id" in activation

        status = await simulator.simulate_procedure_status()
        assert "procedures" in status

        await simulator.simulate_session_end()
