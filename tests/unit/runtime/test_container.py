"""Tests for RuntimeContainer."""
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
