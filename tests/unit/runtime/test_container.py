"""Tests for RuntimeContainer."""
import logging
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elephantbroker.runtime.container import RuntimeContainer
from elephantbroker.schemas.config import ElephantBrokerConfig
from elephantbroker.schemas.tiers import BusinessTier

# Every test in this module constructs a container from a bare
# ``ElephantBrokerConfig()``, which has the empty ``gateway.gateway_id``
# and empty ``cognee.neo4j_password`` defaults. Bucket A's
# ``_validate_startup_safety`` refuses both unless the operator opts out
# via env var. The ``allow_default_gateway`` fixture (tests/conftest.py)
# sets ``EB_ALLOW_DEFAULT_GATEWAY_ID`` + ``EB_DEV_MODE`` +
# ``EB_ALLOW_DATASET_CHANGE`` for the duration of each test via
# ``monkeypatch``, so the opt-outs do not leak into adjacent tests that
# verify the guards fire (e.g. ``test_container_startup_safety.py``).
pytestmark = pytest.mark.usefixtures("allow_default_gateway")


@pytest.fixture(autouse=True)
def _mock_configure_cognee():
    """Mock configure_cognee so unit tests don't hit real Cognee SDK."""
    with patch("elephantbroker.runtime.container.configure_cognee", new_callable=AsyncMock):
        yield


class TestRuntimeContainer:
    async def test_all_modules_instantiated_full_tier(self):
        config = ElephantBrokerConfig()
        container = await RuntimeContainer.from_config(config, BusinessTier.FULL)
        assert container.trace_ledger is not None
        assert container.profile_registry is not None
        assert container.stats is not None
        assert container.scoring_tuner is not None
        assert container.actor_registry is not None
        assert container.goal_manager is not None
        assert container.memory_store is not None
        assert container.procedure_engine is not None
        assert container.evidence_engine is not None
        assert container.artifact_store is not None
        assert container.retrieval is not None
        assert container.rerank is not None
        assert container.working_set_manager is not None
        assert container.context_assembler is not None
        assert container.compaction_engine is not None
        assert container.guard_engine is not None
        assert container.consolidation is not None

    async def test_tier_memory_only_skips_context_modules(self):
        config = ElephantBrokerConfig()
        container = await RuntimeContainer.from_config(config, BusinessTier.MEMORY_ONLY)
        assert container.trace_ledger is not None
        assert container.memory_store is not None
        assert container.working_set_manager is None
        assert container.context_assembler is None
        assert container.compaction_engine is None
        assert container.guard_engine is None
        assert container.consolidation is None

    async def test_tier_context_only_skips_memory_store(self):
        config = ElephantBrokerConfig()
        container = await RuntimeContainer.from_config(config, BusinessTier.CONTEXT_ONLY)
        assert container.trace_ledger is not None
        assert container.memory_store is None
        assert container.artifact_store is None
        # WorkingSetManager is None because retrieval (its dependency) is not in CONTEXT_ONLY
        assert container.working_set_manager is None
        assert container.compaction_engine is not None

    async def test_full_tier_has_all_17_modules(self):
        config = ElephantBrokerConfig()
        container = await RuntimeContainer.from_config(config, BusinessTier.FULL)
        modules = [
            container.trace_ledger, container.profile_registry, container.stats,
            container.scoring_tuner, container.actor_registry, container.goal_manager,
            container.memory_store, container.procedure_engine, container.evidence_engine,
            container.artifact_store, container.retrieval, container.rerank,
            container.working_set_manager, container.context_assembler,
            container.compaction_engine, container.guard_engine, container.consolidation,
        ]
        assert all(m is not None for m in modules)
        assert len(modules) == 17

    async def test_close_shuts_down_adapters(self):
        config = ElephantBrokerConfig()
        container = await RuntimeContainer.from_config(config, BusinessTier.FULL)
        container.graph.close = AsyncMock()
        container.vector.close = AsyncMock()
        container.embeddings.close = AsyncMock()
        await container.close()
        container.graph.close.assert_called_once()
        container.vector.close.assert_called_once()
        container.embeddings.close.assert_called_once()

    async def test_container_from_config(self):
        config = ElephantBrokerConfig()
        container = await RuntimeContainer.from_config(config)
        assert container.config is config
        assert container.tier == BusinessTier.FULL

    async def test_modules_receive_correct_dependencies(self):
        config = ElephantBrokerConfig()
        container = await RuntimeContainer.from_config(config)
        assert container.scoring_tuner._profile_registry is container.profile_registry
        assert container.working_set_manager._retrieval is container.retrieval

    async def test_container_passes_dataset_name_to_modules(self):
        """All 6 modules receive gateway-scoped dataset name."""
        config = ElephantBrokerConfig()
        container = await RuntimeContainer.from_config(config, BusinessTier.FULL)
        expected = f"{config.gateway.gateway_id}__{config.cognee.default_dataset}"
        assert container.actor_registry._dataset_name == expected
        assert container.goal_manager._dataset_name == expected
        assert container.memory_store._dataset_name == expected
        assert container.procedure_engine._dataset_name == expected
        assert container.evidence_engine._dataset_name == expected
        assert container.artifact_store._dataset_name == expected

    async def test_container_default_dataset_from_config(self):
        """Changing config.cognee.default_dataset propagates to modules."""
        from elephantbroker.schemas.config import CogneeConfig
        config = ElephantBrokerConfig(cognee=CogneeConfig(default_dataset="custom_ds"))
        container = await RuntimeContainer.from_config(config, BusinessTier.FULL)
        assert container.actor_registry._dataset_name == f"{config.gateway.gateway_id}__custom_ds"

    async def test_llm_client_created(self):
        config = ElephantBrokerConfig()
        container = await RuntimeContainer.from_config(config, BusinessTier.FULL)
        assert container.llm_client is not None

    async def test_close_calls_llm_close(self):
        config = ElephantBrokerConfig()
        container = await RuntimeContainer.from_config(config, BusinessTier.FULL)
        container.graph.close = AsyncMock()
        container.vector.close = AsyncMock()
        container.embeddings.close = AsyncMock()
        container.llm_client.close = AsyncMock()
        await container.close()
        container.llm_client.close.assert_called_once()


class TestPhase5Wiring:
    """Tests for Phase 5 dependency wiring in RuntimeContainer."""

    async def test_working_set_manager_receives_all_dependencies(self):
        """WorkingSetManager should receive rerank, goal_manager, cached_embeddings, etc."""
        config = ElephantBrokerConfig()
        container = await RuntimeContainer.from_config(config, BusinessTier.FULL)
        wsm = container.working_set_manager
        assert wsm is not None
        assert wsm._rerank is container.rerank
        assert wsm._goal_manager is container.goal_manager
        assert wsm._procedure_engine is container.procedure_engine
        assert wsm._embeddings is container.cached_embeddings
        assert wsm._scoring_tuner is container.scoring_tuner
        assert wsm._profile_registry is container.profile_registry
        assert wsm._graph is container.graph
        assert wsm._redis is container.redis

    async def test_session_goal_store_created(self):
        """container.session_goal_store should be instantiated in FULL tier."""
        config = ElephantBrokerConfig()
        container = await RuntimeContainer.from_config(config, BusinessTier.FULL)
        assert container.session_goal_store is not None

    async def test_cached_embeddings_wraps_raw(self):
        """container.cached_embeddings should wrap container.embeddings."""
        config = ElephantBrokerConfig()
        container = await RuntimeContainer.from_config(config, BusinessTier.FULL)
        assert container.cached_embeddings is not None
        assert container.cached_embeddings._inner is container.embeddings

    async def test_rerank_receives_config(self):
        """RerankOrchestrator should receive reranker_config and scoring_config."""
        config = ElephantBrokerConfig()
        container = await RuntimeContainer.from_config(config, BusinessTier.FULL)
        rerank = container.rerank
        assert rerank is not None
        assert rerank._reranker_config is config.reranker
        assert rerank._scoring_config is config.scoring

    async def test_redis_attribute_exists(self):
        """container.redis attribute should exist (may be None if connection fails)."""
        config = ElephantBrokerConfig()
        container = await RuntimeContainer.from_config(config, BusinessTier.FULL)
        assert hasattr(container, "redis")

    async def test_close_handles_redis(self):
        """close() should not raise even when redis is present or absent."""
        config = ElephantBrokerConfig()
        container = await RuntimeContainer.from_config(config, BusinessTier.FULL)
        container.graph.close = AsyncMock()
        container.vector.close = AsyncMock()
        container.embeddings.close = AsyncMock()
        container.llm_client.close = AsyncMock()
        # Mock redis.aclose to verify it is called
        if container.redis:
            container.redis.aclose = AsyncMock()
        else:
            container.redis = AsyncMock()
            container.redis.aclose = AsyncMock()
        await container.close()
        container.redis.aclose.assert_called_once()

    async def test_setup_tracing_called_during_from_config(self):
        """Fix #29: setup_tracing() must be called during container init."""
        config = ElephantBrokerConfig()
        with patch("elephantbroker.runtime.container.setup_tracing") as mock_tracing:
            await RuntimeContainer.from_config(config, BusinessTier.FULL)
            mock_tracing.assert_called_once_with(config.infra, config.gateway.gateway_id)

    async def test_configure_cognee_failure_aborts_container(self):
        """G7 (TF-FN-005): configure_cognee() failures must propagate from RuntimeContainer.from_config.

        Pins the #1173 PROD-risk contract: configure_cognee is NOT wrapped in try/except.
        A bad-credentials / bad-config / network-unreachable error at boot must abort
        container construction so the operator sees the failure immediately, rather than
        silently producing a half-initialized container that fails later at random request
        paths.

        The module-level _mock_configure_cognee autouse fixture is overridden locally here
        by nesting a second patch with a side_effect -- inner patch wins for the duration
        of the with-block, then the outer patch is restored.
        """
        with patch(
            "elephantbroker.runtime.container.configure_cognee",
            new_callable=AsyncMock,
            side_effect=RuntimeError("bad credentials"),
        ):
            with pytest.raises(RuntimeError, match="bad credentials"):
                await RuntimeContainer.from_config(ElephantBrokerConfig(), BusinessTier.FULL)

    # ------------------------------------------------------------------
    # TF-FN-011 additions
    # ------------------------------------------------------------------

    async def test_close_emits_closing_adapter_log_per_adapter(self, caplog):
        """G1 (TF-FN-011): close() emits an INFO log line for each adapter it shuts down.

        Pins the F2 fix from Step 0 (commit 3526837) -- operators must be able to see
        teardown order and identify which adapter hung or failed during shutdown via
        the journal.
        """
        config = ElephantBrokerConfig()
        container = await RuntimeContainer.from_config(config, BusinessTier.FULL)
        container.graph.close = AsyncMock()
        container.vector.close = AsyncMock()
        container.embeddings.close = AsyncMock()
        container.llm_client.close = AsyncMock()
        if container.redis:
            container.redis.aclose = AsyncMock()
        with caplog.at_level(logging.INFO, logger="elephantbroker.runtime.container"):
            await container.close()
        for name in ["graph", "vector", "embeddings", "llm_client"]:
            assert f"Closing adapter: {name}" in caplog.text, f"Missing log for {name}"
        if container.redis:
            assert "Closing adapter: redis" in caplog.text

    async def test_close_unguarded_graph_failure_cascades(self):
        """G2-a: graph.close is unguarded; failure propagates by design (D13).

        Pins documented partial-guarding behavior (D13 lead decision: intentional fail-fast
        for critical infrastructure). If a future change wraps this in try/except, update
        this test, the TF-FN-011 plan, and reconcile with #226 wording.
        """
        config = ElephantBrokerConfig()
        container = await RuntimeContainer.from_config(config, BusinessTier.FULL)
        container.graph.close = AsyncMock(side_effect=RuntimeError("graph close failed"))
        with pytest.raises(RuntimeError, match="graph close failed"):
            await container.close()

    async def test_close_guarded_redis_failure_isolated(self):
        """G2-b: redis.aclose is guarded; its failure does not abort container teardown.

        Pins guarded path: best-effort cleanup for optional services (D13 design). Teardown
        continues past redis so downstream audit/store closes still run.
        """
        config = ElephantBrokerConfig()
        container = await RuntimeContainer.from_config(config, BusinessTier.FULL)
        # All unguarded closes succeed (so teardown reaches the guarded redis path)
        container.graph.close = AsyncMock()
        container.vector.close = AsyncMock()
        container.embeddings.close = AsyncMock()
        container.llm_client.close = AsyncMock()
        if (
            container.compaction_llm_client is not None
            and container.compaction_llm_client is not container.llm_client
        ):
            container.compaction_llm_client.close = AsyncMock()
        if container.procedure_audit:
            container.procedure_audit.close = AsyncMock()
        if container.session_goal_audit:
            container.session_goal_audit.close = AsyncMock()
        if container.org_override_store:
            container.org_override_store.close = AsyncMock()
        if container.authority_store:
            container.authority_store.close = AsyncMock()
        # Redis close fails -- guarded, so no exception leaks
        assert container.redis is not None, "test presupposes redis client was created"
        container.redis.aclose = AsyncMock(side_effect=RuntimeError("redis close failed"))
        # Must not raise
        await container.close()

    async def test_redis_init_failure_sets_none_with_warning(self, caplog):
        """G3 (#227): redis.asyncio.from_url failure sets c.redis = None and logs WARNING.

        Container continues construction -- dependent features (ingest_buffer, cached_embeddings
        cache path, session goal store) handle redis=None gracefully. Pins the resilience
        contract that Redis unavailability does not abort boot.
        """
        with patch("redis.asyncio.from_url", side_effect=Exception("redis unreachable")):
            with caplog.at_level(logging.WARNING, logger="elephantbroker.runtime.container"):
                container = await RuntimeContainer.from_config(
                    ElephantBrokerConfig(), BusinessTier.FULL,
                )
        assert container.redis is None
        assert "Redis client creation failed, continuing without" in caplog.text

    async def test_procedure_audit_init_db_failure_aborts_container(self):
        """G4 (#1171): ProcedureAuditStore.init_db() failure must abort container construction.

        This call is NOT in try/except by design -- audit storage is a safety-critical
        dependency for Phase 7 evidence/verification. A bad SQLite path, permission error,
        or corrupt DB must be signal-worthy at boot, not silently degraded.
        """
        with patch(
            "elephantbroker.runtime.audit.procedure_audit.ProcedureAuditStore.init_db",
            new_callable=AsyncMock,
            side_effect=Exception("sqlite fail"),
        ):
            with pytest.raises(Exception, match="sqlite fail"):
                await RuntimeContainer.from_config(ElephantBrokerConfig(), BusinessTier.FULL)

    async def test_close_does_not_close_trace_ledger_or_cached_embeddings(self):
        """G5 (#1181, #1182): close() does NOT call TraceLedger.close() or CachedEmbeddingService.close().

        Documents the intentional non-calls:
        - TraceLedger is in-memory, has no resources to close.
        - CachedEmbeddingService.close() internally delegates to its inner EmbeddingService's
          close(); the container already closes that inner directly via container.embeddings.
          Calling cached_embeddings.close() here would double-close the inner.
        """
        config = ElephantBrokerConfig()
        container = await RuntimeContainer.from_config(config, BusinessTier.FULL)
        container.graph.close = AsyncMock()
        container.vector.close = AsyncMock()
        container.embeddings.close = AsyncMock()
        container.llm_client.close = AsyncMock()
        if container.redis:
            container.redis.aclose = AsyncMock()
        # Attach close mocks so any call would be recorded
        container.trace_ledger.close = AsyncMock()
        container.cached_embeddings.close = AsyncMock()
        await container.close()
        assert container.trace_ledger.close.await_count == 0
        assert container.cached_embeddings.close.await_count == 0

    async def test_close_does_not_double_close_compaction_when_same_as_llm(self):
        """G6 (#1183): `is not` identity check prevents double-close when
        compaction_llm_client points to the same instance as llm_client.

        Pins the identity-not-equality guard: if compaction_llm_client is an alias for
        llm_client (shared client, default case), close() must run exactly once on that
        instance. A future change that relaxes this to `!=` could silently double-close,
        which some client implementations handle poorly (already-closed exceptions).
        """
        config = ElephantBrokerConfig()
        container = await RuntimeContainer.from_config(config, BusinessTier.FULL)
        container.graph.close = AsyncMock()
        container.vector.close = AsyncMock()
        container.embeddings.close = AsyncMock()
        container.llm_client.close = AsyncMock()
        if container.redis:
            container.redis.aclose = AsyncMock()
        # Point compaction_llm_client at the same instance as llm_client (aliased).
        container.compaction_llm_client = container.llm_client
        await container.close()
        assert container.llm_client.close.await_count == 1
