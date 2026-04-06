"""Dependency injection container — wires all runtime modules."""
from __future__ import annotations

import logging

from elephantbroker.pipelines.artifact_ingest.pipeline import ArtifactIngestPipeline
from elephantbroker.pipelines.procedure_ingest.pipeline import ProcedureIngestPipeline
from elephantbroker.pipelines.turn_ingest.pipeline import TurnIngestPipeline
from elephantbroker.runtime.actors.registry import ActorRegistry
from elephantbroker.runtime.adapters.cognee.cached_embeddings import CachedEmbeddingService
from elephantbroker.runtime.adapters.cognee.config import configure_cognee
from elephantbroker.runtime.adapters.cognee.datasets import DatasetManager
from elephantbroker.runtime.adapters.cognee.embeddings import EmbeddingService
from elephantbroker.runtime.adapters.cognee.graph import GraphAdapter
from elephantbroker.runtime.adapters.cognee.pipeline_runner import PipelineRunner
from elephantbroker.runtime.adapters.cognee.vector import VectorAdapter
from elephantbroker.runtime.adapters.llm.client import LLMClient
from elephantbroker.runtime.artifacts.store import ToolArtifactStore
from elephantbroker.runtime.audit.procedure_audit import ProcedureAuditStore
from elephantbroker.runtime.audit.session_goal_audit import SessionGoalAuditStore
from elephantbroker.runtime.compaction.engine import CompactionEngine
from elephantbroker.runtime.consolidation.engine import ConsolidationEngine
from elephantbroker.runtime.context.assembler import ContextAssembler
from elephantbroker.runtime.context.lifecycle import ContextLifecycle
from elephantbroker.runtime.context.session_artifact_store import SessionArtifactStore
from elephantbroker.runtime.context.session_store import SessionContextStore
from elephantbroker.runtime.evidence.engine import EvidenceAndVerificationEngine
from elephantbroker.runtime.goals.manager import GoalManager
from elephantbroker.runtime.guards.engine import RedLineGuardEngine
from elephantbroker.runtime.memory.facade import MemoryStoreFacade
from elephantbroker.runtime.observability import register_verbose_level, setup_tracing
from elephantbroker.runtime.procedures.engine import ProcedureEngine
from elephantbroker.runtime.profiles.registry import ProfileRegistry
from elephantbroker.runtime.rerank.orchestrator import RerankOrchestrator
from elephantbroker.runtime.retrieval.orchestrator import RetrievalOrchestrator
from elephantbroker.runtime.stats.engine import StatsAndTelemetryEngine
from elephantbroker.runtime.metrics import MetricsContext
from elephantbroker.runtime.redis_keys import RedisKeyBuilder
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.runtime.working_set.goal_refinement import GoalRefinementTask
from elephantbroker.runtime.working_set.hint_processor import GoalHintProcessor
from elephantbroker.runtime.working_set.manager import WorkingSetManager
from elephantbroker.runtime.working_set.scoring_tuner import ScoringTuner
from elephantbroker.runtime.working_set.session_goals import SessionGoalStore
from elephantbroker.schemas.config import ElephantBrokerConfig
from elephantbroker.schemas.tiers import TIER_CAPABILITIES, BusinessTier

logger = logging.getLogger("elephantbroker.runtime.container")


def _enabled(tier: BusinessTier, interface_name: str) -> bool:
    return interface_name in TIER_CAPABILITIES[tier]


class RuntimeContainer:
    """Wires all runtime modules with their dependencies.

    Tier-aware: modules not in the active tier's capability set are ``None``.
    """

    def __init__(self) -> None:
        self.config: ElephantBrokerConfig | None = None
        self.tier: BusinessTier = BusinessTier.FULL

        # Adapters
        self.graph: GraphAdapter | None = None
        self.vector: VectorAdapter | None = None
        self.embeddings: EmbeddingService | None = None
        self.cached_embeddings: CachedEmbeddingService | None = None
        self.datasets: DatasetManager | None = None
        self.pipeline_runner: PipelineRunner | None = None

        # Shared infrastructure
        self.redis = None  # async Redis client (created in from_config)

        # Runtime modules (17)
        self.trace_ledger: TraceLedger | None = None
        self.profile_registry: ProfileRegistry | None = None
        self.stats: StatsAndTelemetryEngine | None = None
        self.scoring_tuner: ScoringTuner | None = None
        self.actor_registry: ActorRegistry | None = None
        self.goal_manager: GoalManager | None = None
        self.memory_store: MemoryStoreFacade | None = None
        self.procedure_engine: ProcedureEngine | None = None
        self.evidence_engine: EvidenceAndVerificationEngine | None = None
        self.artifact_store: ToolArtifactStore | None = None
        self.retrieval: RetrievalOrchestrator | None = None
        self.rerank: RerankOrchestrator | None = None
        self.working_set_manager: WorkingSetManager | None = None
        self.context_assembler: ContextAssembler | None = None
        self.compaction_engine: CompactionEngine | None = None
        self.guard_engine: RedLineGuardEngine | None = None
        self.consolidation: ConsolidationEngine | None = None

        # Phase 4: LLM + pipelines + buffer
        self.llm_client: LLMClient | None = None
        self.turn_ingest: TurnIngestPipeline | None = None
        self.artifact_ingest: ArtifactIngestPipeline | None = None
        self.procedure_ingest: ProcedureIngestPipeline | None = None
        self.ingest_buffer = None  # IngestBuffer (requires async Redis)

        # Phase 5: Session goals, refinement, audit
        self.session_goal_store: SessionGoalStore | None = None
        self.goal_refinement_task: GoalRefinementTask | None = None
        self.hint_processor: GoalHintProcessor | None = None
        self.procedure_audit: ProcedureAuditStore | None = None
        self.session_goal_audit: SessionGoalAuditStore | None = None

        # Phase 6: Context lifecycle
        self.context_lifecycle: ContextLifecycle | None = None
        self.session_context_store: SessionContextStore | None = None
        self.session_artifact_store: SessionArtifactStore | None = None
        self.compaction_llm_client: LLMClient | None = None

        # Phase 6.2: Async injection analyzer
        self.async_analyzer = None

        # Phase 7: Guard pipelines + HITL
        self.redline_refresh = None
        self.hitl_client = None

        # Phase 8: Org/team identity + authority
        self.org_override_store = None
        self.authority_store = None
        self._bootstrap_mode: bool | None = None  # None = not yet checked
        self._bootstrap_checked: bool = False

        # Gateway identity infrastructure
        self.redis_keys: RedisKeyBuilder | None = None
        self.metrics_ctx: MetricsContext | None = None
        # Phase prod: shared asyncpg pool for all audit stores
        self._pg_pool: object | None = None

    @classmethod
    async def from_config(
        cls,
        config: ElephantBrokerConfig,
        tier: BusinessTier = BusinessTier.FULL,
    ) -> RuntimeContainer:
        """Build container from config. Adapters are initialized, modules wired."""
        c = cls()
        c.config = config
        c.tier = tier

        # Register VERBOSE logging level and configure structured logging
        register_verbose_level()
        from elephantbroker.runtime.observability import setup_json_logging
        setup_json_logging(config.infra, config.gateway.gateway_id)

        # Configure Cognee SDK (graph/vector/LLM/embedding) before creating adapters
        await configure_cognee(config.cognee, config.llm)

        # --- Gateway identity ---
        gw_id = config.gateway.gateway_id
        c.redis_keys = RedisKeyBuilder(gw_id)
        c.metrics_ctx = MetricsContext(gw_id)

        # --- Shared asyncpg pool (for all audit/config stores) ---
        try:
            import asyncpg  # type: ignore[import-untyped]
            c._pg_pool = await asyncpg.create_pool(
                config.audit.postgres_dsn,
                min_size=2,
                max_size=10,
            )
            logger.info("PostgreSQL pool created: %s", config.audit.postgres_dsn)
        except Exception as exc:
            logger.warning("PostgreSQL pool creation failed — audit stores disabled: %s", exc)
            c._pg_pool = None

        # --- Shared infrastructure ---
        # Create async Redis client
        try:
            import redis.asyncio as aioredis
            c.redis = aioredis.from_url(
                config.infra.redis_url,
                decode_responses=True,
                # decode_responses=True: all values stored as JSON strings via json.dumps().
                # json.loads() works on both str and bytes, but returning str simplifies
                # SessionGoalStore, CachedEmbeddingService, and snapshot cache operations.
            )
        except Exception as exc:
            logger.warning("Redis client creation failed, continuing without: %s", exc)
            c.redis = None

        # --- Adapters ---
        c.graph = GraphAdapter(config.cognee)
        c.vector = VectorAdapter(config.cognee)
        c.embeddings = EmbeddingService(config.cognee)
        c.datasets = DatasetManager(config.cognee)
        c.pipeline_runner = PipelineRunner()

        # Phase 5: CachedEmbeddingService wrapping raw EmbeddingService
        c.cached_embeddings = CachedEmbeddingService(
            c.embeddings, redis=c.redis, config=config.embedding_cache,
            metrics=c.metrics_ctx,
        )

        # --- OTEL tracing ---
        setup_tracing(config.infra, gw_id)

        # --- Foundational (no adapter deps) ---
        # TraceLedger with optional OTEL log bridge (Phase 9)
        otel_logger = None
        try:
            from elephantbroker.runtime.observability import setup_otel_logging
            otel_logger = setup_otel_logging(config.infra, gw_id)
        except Exception:
            pass
        c.trace_ledger = TraceLedger(
            gateway_id=gw_id,
            otel_logger=otel_logger,
            config=getattr(config.infra, "trace", None),
        )

        if _enabled(tier, "IProfileRegistry"):
            c.profile_registry = ProfileRegistry(
                c.trace_ledger,
                cache_ttl_seconds=config.profile_cache.ttl_seconds,
                metrics=c.metrics_ctx,
            )
            # org_store wired later after Phase 8 stores are initialized

        if _enabled(tier, "IStatsAndTelemetryEngine"):
            c.stats = StatsAndTelemetryEngine(c.trace_ledger)

        # Phase 9: TuningDeltaStore + ScoringLedgerStore (PostgreSQL)
        c.tuning_delta_store = None
        c.scoring_ledger_store = None
        try:
            from elephantbroker.runtime.working_set.tuning_delta_store import TuningDeltaStore
            c.tuning_delta_store = TuningDeltaStore()
            await c.tuning_delta_store.init(c._pg_pool)
        except Exception:
            pass
        try:
            from elephantbroker.runtime.consolidation.scoring_ledger_store import ScoringLedgerStore
            c.scoring_ledger_store = ScoringLedgerStore()
            await c.scoring_ledger_store.init(c._pg_pool)
        except Exception:
            pass

        if _enabled(tier, "IScoringTuner") and c.profile_registry:
            c.scoring_tuner = ScoringTuner(c.trace_ledger, c.profile_registry, c.tuning_delta_store)

        # --- Adapter-dependent ---
        dataset_name = f"{gw_id}__{config.cognee.default_dataset}"
        if config.cognee.default_dataset != "elephantbroker":
            logger.warning(
                "EB_DEFAULT_DATASET is set to '%s' (default: 'elephantbroker'). "
                "Changing this will make ALL existing Cognee data (facts, goals, procedures) "
                "invisible to retrieval. Only change this for fresh deployments.",
                config.cognee.default_dataset,
            )

        if _enabled(tier, "IActorRegistry"):
            c.actor_registry = ActorRegistry(c.graph, c.trace_ledger, dataset_name=dataset_name, gateway_id=gw_id)

        if _enabled(tier, "IGoalManager"):
            c.goal_manager = GoalManager(c.graph, c.trace_ledger, dataset_name=dataset_name, gateway_id=gw_id)

        if _enabled(tier, "IMemoryStoreFacade"):
            c.memory_store = MemoryStoreFacade(
                c.graph, c.vector, c.embeddings, c.trace_ledger, dataset_name=dataset_name,
                gateway_id=gw_id, metrics=c.metrics_ctx,
            )

        if _enabled(tier, "IProcedureEngine"):
            c.procedure_engine = ProcedureEngine(
                c.graph, c.trace_ledger, dataset_name=dataset_name, gateway_id=gw_id,
                redis=c.redis, redis_keys=c.redis_keys,
                ttl_seconds=config.consolidation_min_retention_seconds,
            )

        if _enabled(tier, "IEvidenceAndVerificationEngine"):
            c.evidence_engine = EvidenceAndVerificationEngine(c.graph, c.trace_ledger, dataset_name=dataset_name, gateway_id=gw_id)

        if _enabled(tier, "IToolArtifactStore"):
            c.artifact_store = ToolArtifactStore(
                c.graph, c.vector, c.embeddings, c.trace_ledger, dataset_name=dataset_name, gateway_id=gw_id,
            )

        if _enabled(tier, "IRetrievalOrchestrator"):
            c.retrieval = RetrievalOrchestrator(
                c.vector, c.graph, c.embeddings, c.trace_ledger, dataset_name=dataset_name,
                gateway_id=gw_id,
            )

        if _enabled(tier, "IRerankOrchestrator"):
            c.rerank = RerankOrchestrator(
                c.trace_ledger,
                embedding_service=c.cached_embeddings,
                reranker_config=config.reranker,
                scoring_config=config.scoring,
                metrics=c.metrics_ctx,
            )

        # --- Modules that depend on other modules ---
        if _enabled(tier, "IWorkingSetManager") and c.retrieval:
            c.working_set_manager = WorkingSetManager(
                retrieval=c.retrieval,
                trace_ledger=c.trace_ledger,
                rerank=c.rerank,
                goal_manager=c.goal_manager,
                procedure_engine=c.procedure_engine,
                embedding_service=c.cached_embeddings,
                scoring_tuner=c.scoring_tuner,
                profile_registry=c.profile_registry,
                graph=c.graph,
                redis=c.redis,
                config=config,
                gateway_id=gw_id,
                redis_keys=c.redis_keys,
                metrics=c.metrics_ctx,
                scoring_ledger_store=c.scoring_ledger_store,
                session_goal_store=c.session_goal_store,
            )

        if _enabled(tier, "IContextAssembler") and c.working_set_manager:
            c.context_assembler = ContextAssembler(
                c.working_set_manager, c.trace_ledger,
                llm_client=None,  # set after LLMClient creation below
                config=config.context_assembly,
            )

        # --- Stubs (Phase 6: CompactionEngine is now full implementation) ---
        if _enabled(tier, "ICompactionEngine"):
            c.compaction_engine = CompactionEngine(
                c.trace_ledger,
                llm_client=None,  # set after LLMClient creation below
                redis=c.redis,
                config=config.context_assembly,
                gateway_id=gw_id,
                redis_keys=c.redis_keys,
                metrics=c.metrics_ctx,
                ttl_seconds=config.consolidation_min_retention_seconds,
            )

        if _enabled(tier, "IRedLineGuardEngine"):
            # Phase 7: Full guard engine with all dependencies
            from elephantbroker.runtime.guards.approval_queue import ApprovalQueue
            from elephantbroker.runtime.guards.autonomy import AutonomyClassifier, ToolDomainRegistry
            from elephantbroker.runtime.guards.hitl_client import HitlClient

            hitl_client = HitlClient(config=config.hitl, gateway_id=gw_id) if config.hitl.enabled else None
            c.hitl_client = hitl_client
            approval_queue = ApprovalQueue(redis=c.redis, redis_keys=c.redis_keys, config=config.hitl, trace_ledger=c.trace_ledger) if c.redis else None
            autonomy_classifier = AutonomyClassifier(
                tool_registry=ToolDomainRegistry(),
                redis=c.redis,
                redis_keys=c.redis_keys,
            )
            c.guard_engine = RedLineGuardEngine(
                trace_ledger=c.trace_ledger,
                embedding_service=c.cached_embeddings or c.embeddings,
                graph=c.graph,
                llm_client=None,  # set after LLMClient creation below
                profile_registry=c.profile_registry,
                redis=c.redis,
                config=config.guards,
                gateway_id=gw_id,
                redis_keys=c.redis_keys,
                metrics=c.metrics_ctx,
                hitl_client=hitl_client,
                approval_queue=approval_queue,
                autonomy_classifier=autonomy_classifier,
                session_goal_store=None,  # set after session_goal_store creation
            )

        if _enabled(tier, "IConsolidationEngine"):
            # Phase 9: Full ConsolidationEngine with all dependencies
            c.consolidation_report_store = None
            c.trace_query_client = None
            try:
                from elephantbroker.runtime.consolidation.report_store import ConsolidationReportStore
                c.consolidation_report_store = ConsolidationReportStore()
                await c.consolidation_report_store.init(c._pg_pool)
            except Exception:
                pass
            try:
                from elephantbroker.runtime.consolidation.otel_trace_query_client import OtelTraceQueryClient
                c.trace_query_client = OtelTraceQueryClient(
                    getattr(config.infra, "clickhouse", None),
                )
            except Exception:
                pass

            c.consolidation = ConsolidationEngine(
                trace_ledger=c.trace_ledger,
                graph=c.graph,
                vector=c.vector,
                memory_store=c.memory_store,
                embedding_service=c.cached_embeddings,
                profile_registry=c.profile_registry,
                scoring_tuner=c.scoring_tuner,
                evidence_engine=c.evidence_engine,
                procedure_engine=c.procedure_engine,
                session_artifact_store=getattr(c, "session_artifact_store", None),
                artifact_store=c.artifact_store,
                llm_client=getattr(c, "llm_client", None),
                redis=c.redis,
                redis_keys=c.redis_keys,
                metrics=c.metrics_ctx,
                config=config,
                report_store=c.consolidation_report_store,
                trace_query_client=c.trace_query_client,
                scoring_ledger_store=c.scoring_ledger_store,
                procedure_audit_store=getattr(c, "procedure_audit_store", None),
                session_goal_audit_store=getattr(c, "session_goal_audit_store", None),
                gateway_id=gw_id,
            )

        # --- Phase 4: LLM client + ingest pipelines ---
        c.llm_client = LLMClient(config.llm)

        # Phase 6: Wire LLM into assembler and compaction, create compaction LLM
        if (config.compaction_llm.endpoint == config.llm.endpoint
                and config.compaction_llm.api_key == config.llm.api_key):
            c.compaction_llm_client = c.llm_client
        else:
            from elephantbroker.schemas.config import LLMConfig as _LLMConfig
            c.compaction_llm_client = LLMClient(_LLMConfig(
                model=config.compaction_llm.model,
                endpoint=config.compaction_llm.endpoint,
                api_key=config.compaction_llm.api_key,
            ))

        if c.context_assembler:
            c.context_assembler._llm_client = c.llm_client
        if c.compaction_engine:
            c.compaction_engine._llm = c.compaction_llm_client
        if c.guard_engine:
            c.guard_engine._llm = c.llm_client

        # IngestBuffer with shared Redis client (resolves Phase 4 tech debt)
        if c.redis:
            from elephantbroker.pipelines.turn_ingest.buffer import IngestBuffer
            c.ingest_buffer = IngestBuffer(redis=c.redis, config=config.llm, redis_keys=c.redis_keys)
        else:
            c.ingest_buffer = None

        # --- Phase 5: Session goals, refinement, audit (created before pipelines so they can be injected) ---
        c.session_goal_store = SessionGoalStore(
            redis=c.redis,
            config=config.scoring,
            trace_ledger=c.trace_ledger,
            graph=c.graph,
            dataset_name=dataset_name,
            gateway_id=gw_id,
            redis_keys=c.redis_keys,
            metrics=c.metrics_ctx,
        )

        # Phase 7: Wire session_goal_store into guard + procedure engines
        if c.guard_engine:
            c.guard_engine._goals = c.session_goal_store
        if c.procedure_engine:
            c.procedure_engine._session_goal_store = c.session_goal_store
        if c.procedure_engine and c.evidence_engine:
            c.procedure_engine._evidence_engine = c.evidence_engine

        # Phase 7: Redline index refresh pipeline (§7.7)
        from elephantbroker.pipelines.redline_index_refresh.pipeline import RedlineIndexRefreshPipeline
        c.redline_refresh = RedlineIndexRefreshPipeline(
            guard_engine=c.guard_engine,
            graph=c.graph,
            profile_registry=c.profile_registry,
            pipeline_runner=c.pipeline_runner,
            trace_ledger=c.trace_ledger,
        ) if c.guard_engine else None

        # Phase 6.2: Async injection analyzer (AD-24)
        if config.async_analysis.enabled and c.cached_embeddings and c.redis:
            from elephantbroker.runtime.context.async_analyzer import AsyncInjectionAnalyzer
            c.async_analyzer = AsyncInjectionAnalyzer(
                embeddings=c.cached_embeddings,
                redis=c.redis,
                redis_keys=c.redis_keys,
                config=config.async_analysis,
                gateway_id=gw_id,
                metrics=c.metrics_ctx,
            )

        c.goal_refinement_task = GoalRefinementTask(
            llm_client=c.llm_client,
            config=config.goal_refinement,
            trace_ledger=c.trace_ledger,
            metrics=c.metrics_ctx,
            gateway_id=gw_id,
        )

        c.hint_processor = GoalHintProcessor(
            session_goal_store=c.session_goal_store,
            goal_refinement_task=c.goal_refinement_task,
            config=config.goal_refinement,
            trace_ledger=c.trace_ledger,
            metrics=c.metrics_ctx,
            gateway_id=gw_id,
        )

        if c.memory_store:
            c.turn_ingest = TurnIngestPipeline(
                memory_facade=c.memory_store,
                actor_registry=c.actor_registry,
                embedding_service=c.embeddings,
                llm_client=c.llm_client,
                trace_ledger=c.trace_ledger,
                config=config.llm,
                profile_registry=c.profile_registry,
                buffer=c.ingest_buffer,
                graph=c.graph,
                session_goal_store=c.session_goal_store,
                hint_processor=c.hint_processor,
                goal_manager=c.goal_manager,
                goal_injection_config=config.goal_injection,
                gateway_id=gw_id,
                metrics=c.metrics_ctx,
                org_id=config.gateway.org_id or "",
                dataset_name=dataset_name,
            )

        if c.artifact_store and c.memory_store:
            c.artifact_ingest = ArtifactIngestPipeline(
                artifact_store=c.artifact_store,
                memory_facade=c.memory_store,
                llm_client=c.llm_client,
                trace_ledger=c.trace_ledger,
                config=config.llm,
                gateway_id=gw_id,
            )

        c.procedure_ingest = ProcedureIngestPipeline(
            graph=c.graph,
            trace_ledger=c.trace_ledger,
            dataset_name=dataset_name,
            gateway_id=gw_id,
        )

        # Audit stores (PostgreSQL)
        c.procedure_audit = ProcedureAuditStore(
            enabled=config.audit.procedure_audit_enabled,
        )
        await c.procedure_audit.init(c._pg_pool)

        c.session_goal_audit = SessionGoalAuditStore(
            enabled=config.audit.session_goal_audit_enabled,
        )
        await c.session_goal_audit.init(c._pg_pool)

        # --- Phase 8: Org override + authority stores (PostgreSQL) ---
        from elephantbroker.runtime.profiles.org_override_store import OrgOverrideStore
        from elephantbroker.runtime.profiles.authority_store import AuthorityRuleStore
        c.org_override_store = OrgOverrideStore()
        await c.org_override_store.init(c._pg_pool)
        c.authority_store = AuthorityRuleStore()
        await c.authority_store.init(c._pg_pool)

        # Bootstrap detection is LAZY — checked on first admin API request
        # via GET /admin/bootstrap-status. This avoids opening a Neo4j
        # connection during from_config() which can cause event loop binding
        # issues in test environments. The _bootstrap_mode flag starts as
        # None (unchecked) and is resolved on first access.
        c._bootstrap_mode = None  # None = not yet checked; True/False after check
        c._bootstrap_checked = False

        # Wire org_store into ProfileRegistry (created earlier without it)
        if c.profile_registry and c.org_override_store:
            c.profile_registry._org_store = c.org_override_store

        # --- Phase 6: Context lifecycle stores + orchestrator ---
        c.session_context_store = SessionContextStore(
            redis=c.redis, config=config, redis_keys=c.redis_keys, gateway_id=gw_id,
        )
        c.session_artifact_store = SessionArtifactStore(
            redis=c.redis, config=config, redis_keys=c.redis_keys,
            artifact_store=c.artifact_store, trace_ledger=c.trace_ledger, gateway_id=gw_id,
        )
        c.context_lifecycle = ContextLifecycle(
            working_set_manager=c.working_set_manager,
            context_assembler=c.context_assembler,
            compaction_engine=c.compaction_engine,
            guard_engine=c.guard_engine,
            memory_store=c.memory_store,
            turn_ingest=c.turn_ingest,
            artifact_ingest=c.artifact_ingest,
            session_goal_store=c.session_goal_store,
            hint_processor=c.hint_processor,
            actor_registry=c.actor_registry,
            profile_registry=c.profile_registry,
            trace_ledger=c.trace_ledger,
            llm_client=c.llm_client,
            redis=c.redis,
            config=config,
            gateway_id=gw_id,
            redis_keys=c.redis_keys,
            metrics=c.metrics_ctx,
            session_context_store=c.session_context_store,
            session_artifact_store=c.session_artifact_store,
            procedure_engine=c.procedure_engine,
            async_analyzer=c.async_analyzer,
            successful_use_task=getattr(c, "successful_use_task", None),
            blocker_extraction_task=getattr(c, "blocker_extraction_task", None),
        )

        # Phase 9: RT-1/RT-2 task instances (conditional on config)
        c.successful_use_task = None
        c.blocker_extraction_task = None
        if config.successful_use.enabled and c.memory_store:
            try:
                from elephantbroker.runtime.consolidation.successful_use_task import SuccessfulUseReasoningTask
                c.successful_use_task = SuccessfulUseReasoningTask(config.successful_use, c.memory_store)
                c.context_lifecycle._successful_use_task = c.successful_use_task
            except Exception:
                pass
        if config.blocker_extraction.enabled and c.session_goal_store:
            try:
                from elephantbroker.runtime.consolidation.blocker_extraction_task import BlockerExtractionTask
                c.blocker_extraction_task = BlockerExtractionTask(config.blocker_extraction, c.session_goal_store)
                c.context_lifecycle._blocker_extraction_task = c.blocker_extraction_task
            except Exception:
                pass

        return c

    async def check_bootstrap_mode(self) -> bool:
        """Lazy bootstrap detection — queries graph on first call, caches result."""
        if self._bootstrap_checked:
            return self._bootstrap_mode or False
        self._bootstrap_checked = True
        try:
            if self.graph:
                result = await self.graph.query_cypher(
                    "MATCH (a:ActorDataPoint) RETURN count(a) AS c"
                )
                self._bootstrap_mode = (result[0]["c"] == 0) if result else False
            else:
                self._bootstrap_mode = False
        except Exception:
            self._bootstrap_mode = False
        if self.metrics_ctx:
            self.metrics_ctx.set_bootstrap_mode(self._bootstrap_mode)
        return self._bootstrap_mode

    async def close(self) -> None:
        """Shut down all adapter connections."""
        if self.graph:
            await self.graph.close()
        if self.vector:
            await self.vector.close()
        if self.embeddings:
            await self.embeddings.close()
        if self.llm_client:
            await self.llm_client.close()
        if self.compaction_llm_client and self.compaction_llm_client is not self.llm_client:
            await self.compaction_llm_client.close()
        # Phase 5 cleanup
        if self.redis:
            try:
                await self.redis.aclose()
            except Exception:
                pass
        if self.rerank:
            try:
                await self.rerank.close()
            except Exception:
                pass
        if self.procedure_audit:
            await self.procedure_audit.close()
        if self.session_goal_audit:
            await self.session_goal_audit.close()
        # Phase 8 cleanup
        if self.org_override_store:
            await self.org_override_store.close()
        if self.authority_store:
            await self.authority_store.close()
        # Phase 7 cleanup
        if self.hitl_client:
            try:
                await self.hitl_client.close()
            except Exception:
                pass
        # Phase 9 cleanup
        for store_attr in ("tuning_delta_store", "scoring_ledger_store", "consolidation_report_store"):
            store = getattr(self, store_attr, None)
            if store:
                try:
                    await store.close()
                except Exception:
                    pass
        trace_qc = getattr(self, "trace_query_client", None)
        if trace_qc:
            try:
                trace_qc.close()
            except Exception:
                pass
        # Close shared PostgreSQL pool
        if self._pg_pool:
            try:
                await self._pg_pool.close()
            except Exception:
                pass
