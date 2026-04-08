"""Phase 6 E2E lifecycle test — context engine bootstrap, ingest, assemble, compact, after-turn.

Exercises the full Phase 6 context lifecycle via in-process ASGI transport (no external server):
  1. bootstrap (initialize session context with profile)
  2. ingest (single message degraded mode + batch)
  3. assemble (build working set + system prompt overlay)
  4. build-overlay (Surface B)
  5. compact (compaction with force)
  6. after-turn (successful-use tracking)
  7. subagent spawn/ended/rollback
  8. dispose (cleanup)
  9. composite full-turn + multi-turn flows

Run with: pytest tests/e2e/gateway_simulator/test_phase6_lifecycle.py -v -m integration
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
async def app(monkeypatch):
    """Create a fully wired FastAPI app with real infrastructure (per test).

    R2 integration RED fix (cascade fallout from TODO-3-343 / Bucket A-R2-Test):
    Bucket A-R2-Test removed the global EB_ALLOW_DEFAULT_GATEWAY_ID opt-out
    from tests/conftest.py and scoped it to the unit-side test_container.py
    only. E2E fixtures call RuntimeContainer.from_config() directly without
    that scoping, and the Bucket A startup safety check (R1 `d850186`)
    correctly refuses to boot with empty gateway_id. Set a distinctive value
    here so any cross-test pollution surfaces as a visible mismatch instead of
    a silent collision. Same pattern as the I-R2 fix to
    tests/integration/runtime/working_set/test_working_set_integration.py.
    """
    from elephantbroker.api.app import create_app
    from elephantbroker.runtime.container import RuntimeContainer
    from elephantbroker.schemas.tiers import BusinessTier

    monkeypatch.setenv("EB_GATEWAY_ID", "test-phase6-gateway")
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
    import uuid
    from tests.e2e.gateway_simulator.simulator import OpenClawGatewaySimulator

    transport = httpx.ASGITransport(app=app)
    sim = OpenClawGatewaySimulator.__new__(OpenClawGatewaySimulator)
    sim.client = httpx.AsyncClient(transport=transport, base_url="http://testserver", timeout=30.0)
    sim.session_key = "agent:main:main"
    sim.session_id = str(uuid.uuid4())
    yield sim
    await sim.client.aclose()


# ---------------------------------------------------------------------------
# Bootstrap & Dispose
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestContextBootstrapAndDispose:
    """Bootstrap creates session context; dispose cleans it up."""

    @pytest.mark.asyncio
    async def test_bootstrap_default(self, simulator):
        result = await simulator.simulate_context_bootstrap()
        assert result.get("bootstrapped") is True

    @pytest.mark.asyncio
    async def test_bootstrap_with_research_profile(self, simulator):
        result = await simulator.simulate_context_bootstrap(profile_name="research")
        assert result.get("bootstrapped") is True

    @pytest.mark.asyncio
    async def test_bootstrap_with_prior_session(self, simulator):
        result = await simulator.simulate_context_bootstrap(prior_session_id="00000000-0000-0000-0000-000000000000")
        assert result.get("bootstrapped") is True

    @pytest.mark.asyncio
    async def test_dispose_after_bootstrap(self, simulator):
        await simulator.simulate_context_bootstrap()
        result = await simulator.simulate_context_dispose()
        assert result.get("disposed") is True

    @pytest.mark.asyncio
    async def test_dispose_idempotent(self, simulator):
        await simulator.simulate_context_bootstrap()
        await simulator.simulate_context_dispose()
        result = await simulator.simulate_context_dispose()
        assert result.get("disposed") is True


# ---------------------------------------------------------------------------
# Ingest
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestContextIngest:
    """Ingest via single message (degraded) and batch (primary)."""

    @pytest.mark.asyncio
    async def test_ingest_single_message_degraded(self, simulator):
        await simulator.simulate_context_bootstrap()
        result = await simulator.simulate_context_ingest(role="user", content="Hello")
        assert result.get("ingested") is True

    @pytest.mark.asyncio
    async def test_ingest_batch_returns_count(self, simulator):
        await simulator.simulate_context_bootstrap()
        result = await simulator.simulate_context_ingest_batch([
            {"role": "user", "content": "What is the API rate limit?"},
            {"role": "assistant", "content": "The rate limit is 200/min."},
            {"role": "user", "content": "Can we increase it?"},
        ])
        assert result.get("ingested_count", -1) >= 0

    @pytest.mark.asyncio
    async def test_ingest_batch_empty_messages(self, simulator):
        await simulator.simulate_context_bootstrap()
        result = await simulator.simulate_context_ingest_batch([])
        assert result.get("ingested_count", -1) >= 0

    @pytest.mark.asyncio
    async def test_ingest_heartbeat_processed_normally(self, simulator):
        """AD-27: heartbeat flag is informational, processing NOT short-circuited."""
        await simulator.simulate_context_bootstrap()
        result = await simulator.simulate_context_ingest_batch(
            [{"role": "user", "content": "Heartbeat check"}],
            is_heartbeat=True,
        )
        assert result.get("ingested_count", -1) >= 0


# ---------------------------------------------------------------------------
# Assemble
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestContextAssemble:
    """Assemble builds working set and returns context."""

    @pytest.mark.asyncio
    async def test_assemble_returns_structure(self, simulator):
        await simulator.simulate_context_bootstrap()
        await simulator.simulate_context_ingest_batch([
            {"role": "user", "content": "Fix the auth bug"},
        ])
        result = await simulator.simulate_context_assemble(
            messages=[{"role": "user", "content": "Fix the auth bug"}],
            query="Fix the auth bug",
        )
        assert "messages" in result or "estimated_tokens" in result

    @pytest.mark.asyncio
    async def test_assemble_with_token_budget(self, simulator):
        await simulator.simulate_context_bootstrap()
        result = await simulator.simulate_context_assemble(
            messages=[{"role": "user", "content": "test"}],
            query="test",
            token_budget=4000,
        )
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_assemble_auto_bootstraps(self, simulator):
        """Assemble WITHOUT prior bootstrap should auto-bootstrap (AD-28)."""
        result = await simulator.simulate_context_assemble(
            messages=[{"role": "user", "content": "test"}],
            query="test",
        )
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_build_overlay_after_assemble(self, simulator):
        await simulator.simulate_context_bootstrap()
        await simulator.simulate_context_assemble(
            messages=[{"role": "user", "content": "test"}],
            query="test",
        )
        overlay = await simulator.simulate_context_build_overlay()
        assert isinstance(overlay, dict)


# ---------------------------------------------------------------------------
# Compaction
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestContextCompaction:
    """Compaction reduces token count when triggered."""

    @pytest.mark.asyncio
    async def test_compact_noop_without_messages(self, simulator):
        await simulator.simulate_context_bootstrap()
        result = await simulator.simulate_context_compact()
        assert result.get("ok") is True

    @pytest.mark.asyncio
    async def test_compact_force(self, simulator):
        await simulator.simulate_context_bootstrap()
        await simulator.simulate_context_ingest_batch([
            {"role": "user", "content": "Deploy the service to production"},
            {"role": "assistant", "content": "Deploying now..."},
        ])
        result = await simulator.simulate_context_compact(force=True)
        assert result.get("ok") is True

    @pytest.mark.asyncio
    async def test_compact_returns_reason(self, simulator):
        await simulator.simulate_context_bootstrap()
        result = await simulator.simulate_context_compact()
        assert "reason" in result or "compacted" in result


# ---------------------------------------------------------------------------
# After Turn
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestContextAfterTurn:
    """After-turn tracks successful use and goal progress."""

    @pytest.mark.asyncio
    async def test_after_turn_returns_processed(self, simulator):
        await simulator.simulate_context_bootstrap()
        await simulator.simulate_context_ingest_batch([
            {"role": "user", "content": "Check the logs"},
        ])
        result = await simulator.simulate_context_after_turn(
            messages=[
                {"role": "user", "content": "Check the logs"},
                {"role": "assistant", "content": "The logs show no errors."},
            ],
            pre_prompt_message_count=0,
        )
        assert result.get("processed") is True

    @pytest.mark.asyncio
    async def test_after_turn_with_pre_prompt_count(self, simulator):
        await simulator.simulate_context_bootstrap()
        result = await simulator.simulate_context_after_turn(
            messages=[{"role": "assistant", "content": "Done."}],
            pre_prompt_message_count=2,
        )
        assert result.get("processed") is True

    @pytest.mark.asyncio
    async def test_after_turn_without_messages(self, simulator):
        await simulator.simulate_context_bootstrap()
        result = await simulator.simulate_context_after_turn()
        assert result.get("processed") is True


# ---------------------------------------------------------------------------
# Subagent
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestContextSubagent:
    """Subagent spawn/ended/rollback lifecycle."""

    @pytest.mark.asyncio
    async def test_subagent_spawn_returns_rollback_key(self, simulator):
        await simulator.simulate_context_bootstrap()
        result = await simulator.simulate_context_subagent_spawn(
            child_session_key="agent:worker:subagent:abc",
        )
        assert result.get("parent_session_key") == simulator.session_key
        assert result.get("child_session_key") == "agent:worker:subagent:abc"
        assert "rollback_key" in result

    @pytest.mark.asyncio
    async def test_subagent_ended_completed(self, simulator):
        await simulator.simulate_context_bootstrap()
        await simulator.simulate_context_subagent_spawn(
            child_session_key="agent:worker:subagent:abc",
        )
        result = await simulator.simulate_context_subagent_ended(
            child_session_key="agent:worker:subagent:abc",
        )
        assert result.get("acknowledged") is True

    @pytest.mark.asyncio
    async def test_subagent_ended_swept(self, simulator):
        await simulator.simulate_context_bootstrap()
        await simulator.simulate_context_subagent_spawn(
            child_session_key="agent:worker:subagent:xyz",
        )
        result = await simulator.simulate_context_subagent_ended(
            child_session_key="agent:worker:subagent:xyz",
            reason="swept",
        )
        assert result.get("acknowledged") is True

    @pytest.mark.asyncio
    async def test_subagent_rollback_cleans_up(self, simulator):
        await simulator.simulate_context_bootstrap()
        spawn_result = await simulator.simulate_context_subagent_spawn(
            child_session_key="agent:worker:subagent:fail",
        )
        result = await simulator.simulate_context_subagent_rollback(
            child_session_key="agent:worker:subagent:fail",
            rollback_key=spawn_result.get("rollback_key", ""),
        )
        assert result.get("rolled_back") is True


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestContextGetConfig:
    """Config endpoint returns assembly configuration."""

    @pytest.mark.asyncio
    async def test_get_config_returns_assembly_settings(self, simulator):
        result = await simulator.simulate_context_get_config()
        assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# Full Lifecycle Turn
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestFullLifecycleTurn:
    """Composite helpers exercising full turn flow."""

    @pytest.mark.asyncio
    async def test_single_full_turn(self, simulator):
        await simulator.simulate_context_bootstrap()
        result = await simulator.simulate_full_lifecycle_turn(
            user_msg="What database do we use?",
            assistant_msg="We use PostgreSQL 16.",
        )
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_multi_turn_3_turns(self, simulator):
        await simulator.simulate_context_bootstrap()
        results = await simulator.simulate_multi_turn_conversation(
            turns=[
                ("Set up the database", "Database configured."),
                ("Run the migrations", "Migrations complete."),
                ("Test the endpoints", "All tests passing."),
            ],
        )
        assert len(results) == 3

    @pytest.mark.asyncio
    async def test_multi_turn_with_compaction(self, simulator):
        await simulator.simulate_context_bootstrap()
        results = await simulator.simulate_multi_turn_conversation(
            turns=[
                ("First task", "Done."),
                ("Second task", "Done."),
                ("Third task", "Done."),
                ("Fourth task", "Done."),
            ],
            compact_after=2,
        )
        assert len(results) == 4


# ---------------------------------------------------------------------------
# Full Lifecycle with Goals
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestFullLifecycleWithGoals:
    """Context lifecycle interacting with session goals."""

    @pytest.mark.asyncio
    async def test_turn_with_active_goals(self, simulator):
        await simulator.simulate_context_bootstrap()
        await simulator.simulate_session_goals_create("Fix auth bug")
        result = await simulator.simulate_full_lifecycle_turn(
            user_msg="Working on the auth bug",
            assistant_msg="I found the issue in the middleware.",
        )
        assert isinstance(result, dict)

    @pytest.mark.asyncio
    async def test_multi_turn_goal_lifecycle(self, simulator):
        await simulator.simulate_context_bootstrap()
        goal = await simulator.simulate_session_goals_create("Deploy to staging")
        goal_id = goal.get("id") or goal.get("goal_id", "")
        await simulator.simulate_full_lifecycle_turn(
            user_msg="Start the deployment",
            assistant_msg="Deploying to staging now.",
        )
        if goal_id:
            await simulator.simulate_session_goals_progress(goal_id, "Deployment started")
        await simulator.simulate_full_lifecycle_turn(
            user_msg="Check deployment status",
            assistant_msg="Deployment successful.",
        )
        if goal_id:
            await simulator.simulate_session_goals_update_status(goal_id, "completed", "Deployed successfully")


# ---------------------------------------------------------------------------
# Complete End-to-End
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestFullLifecycleEndToEnd:
    """Full session lifecycle: start → bootstrap → ingest → assemble → compact → after_turn → dispose → end."""

    @pytest.mark.asyncio
    async def test_complete_session_lifecycle(self, simulator):
        """Exercise every lifecycle method in sequence. No content assertions — just no errors."""
        await simulator.simulate_session_start()
        await simulator.simulate_context_bootstrap()
        await simulator.simulate_context_ingest_batch([
            {"role": "user", "content": "Hello"},
            {"role": "assistant", "content": "Hi there!"},
        ])
        await simulator.simulate_context_ingest_batch([
            {"role": "user", "content": "What can you do?"},
            {"role": "assistant", "content": "I can help with code."},
        ])
        await simulator.simulate_context_ingest_batch([
            {"role": "user", "content": "Fix the bug"},
            {"role": "assistant", "content": "Fixed."},
        ])
        await simulator.simulate_context_assemble(
            messages=[{"role": "user", "content": "Fix the bug"}],
            query="Fix the bug",
        )
        await simulator.simulate_context_compact(force=True)
        await simulator.simulate_context_after_turn(
            messages=[
                {"role": "user", "content": "Fix the bug"},
                {"role": "assistant", "content": "Fixed."},
            ],
        )
        await simulator.simulate_context_dispose()
        await simulator.simulate_session_end()
