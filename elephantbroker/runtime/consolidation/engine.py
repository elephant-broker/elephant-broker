"""ConsolidationEngine — 9-stage 'sleep' pipeline for memory consolidation.

Orchestrates all 9 stages, manages Redis locking, stores reports, emits
trace events and metrics. Scoped per (org_id, gateway_id) call — one
engine instance serves all gateways via the API route.

AD-1: Immediate mutations per stage (crash-safe).
AD-5: Single orchestrator, stage classes as internal dependencies.
AD-10: Redis distributed lock (one run per gateway at a time).
AD-11: Active session protection (skip recent facts).
"""
from __future__ import annotations

import json
import logging
import time
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from elephantbroker.runtime.interfaces.consolidation import IConsolidationEngine
from elephantbroker.runtime.observability import GatewayLoggerAdapter, traced
from elephantbroker.schemas.consolidation import (
    ConsolidationContext,
    ConsolidationReport,
    ConsolidationSummary,
    StageResult,
)
from elephantbroker.schemas.trace import TraceEvent, TraceEventType

if TYPE_CHECKING:
    from elephantbroker.runtime.adapters.cognee.cached_embeddings import CachedEmbeddingService
    from elephantbroker.runtime.adapters.cognee.graph import GraphAdapter
    from elephantbroker.runtime.adapters.cognee.vector import VectorAdapter
    from elephantbroker.runtime.adapters.llm.client import LLMClient
    from elephantbroker.runtime.audit.procedure_audit import ProcedureAuditStore
    from elephantbroker.runtime.audit.session_goal_audit import SessionGoalAuditStore
    from elephantbroker.runtime.consolidation.otel_trace_query_client import OtelTraceQueryClient
    from elephantbroker.runtime.consolidation.report_store import ConsolidationReportStore
    from elephantbroker.runtime.consolidation.scoring_ledger_store import ScoringLedgerStore
    from elephantbroker.runtime.context.session_artifact_store import SessionArtifactStore
    from elephantbroker.runtime.interfaces.artifact_store import IToolArtifactStore
    from elephantbroker.runtime.interfaces.evidence_engine import IEvidenceAndVerificationEngine
    from elephantbroker.runtime.interfaces.memory_store import IMemoryStoreFacade
    from elephantbroker.runtime.interfaces.procedure_engine import IProcedureEngine
    from elephantbroker.runtime.interfaces.profile_registry import IProfileRegistry
    from elephantbroker.runtime.interfaces.scoring_tuner import IScoringTuner
    from elephantbroker.runtime.interfaces.trace_ledger import ITraceLedger
    from elephantbroker.runtime.metrics import MetricsContext
    from elephantbroker.runtime.redis_keys import RedisKeyBuilder
    from elephantbroker.schemas.config import ElephantBrokerConfig

_STAGE_NAMES = {
    1: "cluster_duplicates",
    2: "canonicalize",
    3: "strengthen",
    4: "decay",
    5: "prune_autorecall",
    6: "promote",
    7: "refine_procedures",
    8: "verification_gaps",
    9: "recompute_salience",
}


class ConsolidationAlreadyRunningError(Exception):
    """Raised when a consolidation run is already in progress for this gateway."""

    def __init__(self, gateway_id: str) -> None:
        super().__init__(f"Consolidation already running for gateway {gateway_id}")
        self.gateway_id = gateway_id


class ConsolidationEngine(IConsolidationEngine):
    """9-stage consolidation pipeline orchestrator."""

    def __init__(
        self,
        trace_ledger: ITraceLedger,
        graph: GraphAdapter | None = None,
        vector: VectorAdapter | None = None,
        memory_store: IMemoryStoreFacade | None = None,
        embedding_service: CachedEmbeddingService | None = None,
        profile_registry: IProfileRegistry | None = None,
        scoring_tuner: IScoringTuner | None = None,
        evidence_engine: IEvidenceAndVerificationEngine | None = None,
        procedure_engine: IProcedureEngine | None = None,
        session_artifact_store: SessionArtifactStore | None = None,
        artifact_store: IToolArtifactStore | None = None,
        llm_client: LLMClient | None = None,
        redis: Any = None,
        redis_keys: RedisKeyBuilder | None = None,
        metrics: MetricsContext | None = None,
        config: ElephantBrokerConfig | None = None,
        report_store: ConsolidationReportStore | None = None,
        trace_query_client: OtelTraceQueryClient | None = None,
        scoring_ledger_store: ScoringLedgerStore | None = None,
        procedure_audit_store: ProcedureAuditStore | None = None,
        session_goal_audit_store: SessionGoalAuditStore | None = None,
        gateway_id: str = "",
        **kwargs,
    ) -> None:
        self._trace = trace_ledger
        self._graph = graph
        self._vector = vector
        self._memory = memory_store
        self._embeddings = embedding_service
        self._profiles = profile_registry
        self._tuner = scoring_tuner
        self._evidence = evidence_engine
        self._procedures = procedure_engine
        self._session_artifacts = session_artifact_store
        self._artifacts = artifact_store
        self._llm = llm_client
        self._redis = redis
        self._keys = redis_keys
        self._metrics = metrics
        self._config = config
        self._report_store = report_store
        self._trace_query = trace_query_client
        self._scoring_ledger = scoring_ledger_store
        self._proc_audit = procedure_audit_store
        self._goal_audit = session_goal_audit_store
        self._gateway_id = gateway_id
        self._log = GatewayLoggerAdapter(
            logging.getLogger("elephantbroker.runtime.consolidation.engine"),
            {"gateway_id": gateway_id},
        )

        # Create stage instances (internal — not in DI)
        self._stages = self._build_stages()

    def _build_stages(self) -> dict[int, Any]:
        """Create stage instances from constructor dependencies."""
        from elephantbroker.schemas.consolidation import ConsolidationConfig

        consolidation_cfg = ConsolidationConfig()
        if self._config:
            # ConsolidationConfig may be attached to ElephantBrokerConfig
            consolidation_cfg = getattr(self._config, "consolidation", None) or ConsolidationConfig()
            if not isinstance(consolidation_cfg, ConsolidationConfig):
                consolidation_cfg = ConsolidationConfig()

        stages: dict[int, Any] = {}

        try:
            from elephantbroker.runtime.consolidation.stages.cluster_duplicates import ClusterDuplicatesStage
            if self._embeddings:
                stages[1] = ClusterDuplicatesStage(self._embeddings, consolidation_cfg)
        except Exception:
            pass

        try:
            from elephantbroker.runtime.consolidation.stages.canonicalize import CanonicalizationStage
            if self._graph and self._vector:
                stages[2] = CanonicalizationStage(
                    self._graph, self._vector, self._llm, self._embeddings, consolidation_cfg,
                    trace_ledger=self._trace, metrics=self._metrics,
                )
        except Exception:
            pass

        try:
            from elephantbroker.runtime.consolidation.stages.strengthen import StrengthenStage
            stages[3] = StrengthenStage(consolidation_cfg)
        except Exception:
            pass

        try:
            from elephantbroker.runtime.consolidation.stages.decay import DecayStage
            stages[4] = DecayStage(consolidation_cfg)
        except Exception:
            pass

        try:
            from elephantbroker.runtime.consolidation.stages.prune_autorecall import PruneAutorecallStage
            stages[5] = PruneAutorecallStage(consolidation_cfg)
        except Exception:
            pass

        try:
            from elephantbroker.runtime.consolidation.stages.promote import PromoteStage
            if self._graph:
                stages[6] = PromoteStage(
                    self._graph, self._session_artifacts, self._artifacts, consolidation_cfg,
                )
        except Exception:
            pass

        try:
            from elephantbroker.runtime.consolidation.stages.refine_procedures import RefineProceduresStage
            stages[7] = RefineProceduresStage(
                self._llm, self._trace_query, self._proc_audit, consolidation_cfg,
            )
        except Exception:
            pass

        try:
            from elephantbroker.runtime.consolidation.stages.verification_gaps import VerificationGapsStage
            if self._evidence and self._procedures and self._graph:
                stages[8] = VerificationGapsStage(self._evidence, self._procedures, self._graph)
        except Exception:
            pass

        try:
            from elephantbroker.runtime.consolidation.stages.recompute_salience import RecomputeSalienceStage
            stages[9] = RecomputeSalienceStage(consolidation_cfg)
        except Exception:
            pass

        return stages

    @traced
    async def run_consolidation(
        self, org_id: str, gateway_id: str, profile_id: str | None = None,
    ) -> ConsolidationReport:
        """Run the full 9-stage consolidation pipeline.

        Scoped to (org_id, gateway_id) pair. Protected by Redis distributed lock.
        """
        report = ConsolidationReport(
            org_id=org_id, gateway_id=gateway_id, profile_id=profile_id,
        )
        t0 = time.monotonic()
        lock = None

        try:
            # 1. Acquire Redis lock (AD-10)
            if self._redis and self._keys:
                lock_key = self._keys.consolidation_lock()
                lock = self._redis.lock(lock_key, timeout=3600)
                acquired = await lock.acquire(blocking=False)
                if not acquired:
                    raise ConsolidationAlreadyRunningError(gateway_id)

                # 2. Write running status
                status_key = self._keys.consolidation_status()
                await self._redis.set(
                    status_key,
                    json.dumps({"running": True, "started_at": report.started_at.isoformat(), "current_stage": 0}),
                )

            # 3. Emit CONSOLIDATION_STARTED
            await self._trace.append_event(TraceEvent(
                event_type=TraceEventType.CONSOLIDATION_STARTED,
                payload={"org_id": org_id, "gateway_id": gateway_id, "profile_id": profile_id},
            ))

            # 4. Resolve profile
            profile = None
            if self._profiles and profile_id:
                try:
                    profile = await self._profiles.resolve_profile(profile_id, org_id=org_id)
                except Exception:
                    self._log.warning("Profile resolution failed for %s", profile_id)

            # 5. Load facts (paginated Cypher with AD-11 protection)
            facts = await self._load_facts(gateway_id)

            # 6. Build context
            from elephantbroker.schemas.consolidation import ConsolidationConfig
            consolidation_cfg = getattr(self._config, "consolidation", None) or ConsolidationConfig()
            llm_cap = (
                consolidation_cfg.llm_calls_per_run_cap
                if isinstance(consolidation_cfg, ConsolidationConfig)
                else 50
            )

            context = ConsolidationContext(
                org_id=org_id,
                gateway_id=gateway_id,
                profile_id=profile_id,
                resolved_profile=profile,
                scoring_ledger_store=self._scoring_ledger,
                facts=facts,
                total_facts_loaded=len(facts),
                llm_calls_cap=llm_cap,
            )

            # 7. Run stages 1-9 sequentially
            for stage_num in range(1, 10):
                stage_result = await self._run_single_stage(stage_num, gateway_id, context, profile, report)
                context.stage_results.append(stage_result)
                report.stage_results.append(stage_result)

                # Update Redis status
                if self._redis and self._keys:
                    try:
                        status_key = self._keys.consolidation_status()
                        await self._redis.set(
                            status_key,
                            json.dumps({
                                "running": True,
                                "started_at": report.started_at.isoformat(),
                                "current_stage": stage_num,
                            }),
                        )
                    except Exception:
                        pass

            # Build summary from stage results
            report.summary = self._build_summary(report.stage_results)
            report.status = "completed"
            report.completed_at = datetime.now(UTC)

            # 15. Store report
            if self._report_store:
                try:
                    await self._report_store.save_report(report)
                except Exception:
                    self._log.warning("Failed to save consolidation report")

            # 16. Cleanup (TD-8)
            await self._run_cleanup()

            # Metrics
            duration = time.monotonic() - t0
            if self._metrics:
                self._metrics.inc_consolidation_run("completed")
                self._metrics.observe_consolidation_duration(duration)

        except ConsolidationAlreadyRunningError:
            raise
        except Exception as exc:
            report.status = "failed"
            report.error = str(exc)
            report.completed_at = datetime.now(UTC)
            self._log.error("Consolidation failed: %s", exc, exc_info=True)
            if self._metrics:
                self._metrics.inc_consolidation_run("failed")
            if self._report_store:
                try:
                    await self._report_store.save_report(report)
                except Exception:
                    pass
        finally:
            # 18. Release Redis lock + clear status
            if lock:
                try:
                    await lock.release()
                except Exception:
                    pass
            if self._redis and self._keys:
                try:
                    status_key = self._keys.consolidation_status()
                    await self._redis.set(
                        status_key,
                        json.dumps({"running": False, "last_run_at": datetime.now(UTC).isoformat()}),
                    )
                except Exception:
                    pass

            # 17. Emit CONSOLIDATION_COMPLETED
            await self._trace.append_event(TraceEvent(
                event_type=TraceEventType.CONSOLIDATION_COMPLETED,
                payload={
                    "org_id": report.org_id, "gateway_id": report.gateway_id,
                    "report_id": report.id, "status": report.status,
                    "summary": report.summary.model_dump() if report.summary else {},
                },
            ))

        return report

    async def _run_single_stage(
        self, stage_num: int, gateway_id: str,
        context: ConsolidationContext, profile: Any,
        report: ConsolidationReport,
    ) -> StageResult:
        """Run a single stage with timing, metrics, and error handling."""
        stage_name = _STAGE_NAMES.get(stage_num, f"stage_{stage_num}")
        t0 = time.monotonic()
        result = StageResult(stage=stage_num, name=stage_name)

        stage = self._stages.get(stage_num)
        if not stage:
            result.details = {"skipped": True, "reason": "stage not available"}
            return result

        try:
            if stage_num == 1:
                clusters = await stage.run(context.facts, gateway_id)
                context.clusters = clusters
                result.items_processed = len(context.facts)
                result.items_affected = sum(len(c.fact_ids) for c in clusters)

            elif stage_num == 2:
                canon_results = await stage.run(context.clusters, context.facts, gateway_id, context)
                if isinstance(canon_results, tuple):
                    canon_results, reloaded_facts = canon_results
                    context.facts = reloaded_facts
                result.items_processed = len(context.clusters)
                result.items_affected = len(canon_results) if isinstance(canon_results, list) else 0
                result.llm_calls_made = context.llm_calls_used

            elif stage_num == 3:
                strengthen_results = await stage.run(context.facts, gateway_id)
                await self._apply_fact_upserts(context.facts, gateway_id)
                result.items_processed = len(context.facts)
                result.items_affected = sum(1 for r in strengthen_results if r.boosted)

            elif stage_num == 4:
                decay_results = await stage.run(context.facts, profile, gateway_id)
                # Archive facts below threshold (AD-3)
                for dr in decay_results:
                    if dr.archived:
                        await self._archive_fact(dr.fact_id, gateway_id)
                await self._apply_fact_upserts(context.facts, gateway_id)
                result.items_processed = len(context.facts)
                result.items_affected = len(decay_results)
                result.details["archived"] = sum(1 for r in decay_results if r.archived)

            elif stage_num == 5:
                blacklisted = await stage.run(context.facts, gateway_id)
                await self._apply_fact_upserts(context.facts, gateway_id)
                result.items_processed = len(context.facts)
                result.items_affected = len(blacklisted)

            elif stage_num == 6:
                promote_results = await stage.run(context.facts, gateway_id, context)
                await self._apply_fact_upserts(context.facts, gateway_id)
                result.items_processed = len(context.facts)
                result.items_affected = len(promote_results)

            elif stage_num == 7:
                suggestions = await stage.run(gateway_id, context)
                result.items_affected = len(suggestions)
                result.llm_calls_made = context.llm_calls_used
                # Store suggestions + metrics
                if self._report_store:
                    for s in suggestions:
                        try:
                            await self._report_store.save_suggestion(s.model_dump(mode="json"))
                        except Exception:
                            pass
                if self._metrics and suggestions:
                    self._metrics.inc_consolidation_suggestion("pending")

            elif stage_num == 8:
                gaps = await stage.run(gateway_id)
                result.items_affected = len(gaps)

            elif stage_num == 9:
                await self._run_stage_9(context, profile, gateway_id, result)

        except Exception as exc:
            result.details["error"] = str(exc)
            self._log.warning("Stage %d (%s) failed: %s", stage_num, stage_name, exc, exc_info=True)
            if report.status != "failed":
                report.status = "partial"

        result.duration_ms = int((time.monotonic() - t0) * 1000)

        # Emit stage completed trace event
        await self._trace.append_event(TraceEvent(
            event_type=TraceEventType.CONSOLIDATION_STAGE_COMPLETED,
            payload={
                "org_id": context.org_id, "gateway_id": gateway_id,
                "stage": stage_num, "name": stage_name,
                "items_processed": result.items_processed,
                "items_affected": result.items_affected,
                "duration_ms": result.duration_ms,
                "llm_calls": result.llm_calls_made,
            },
        ))

        # Metrics
        if self._metrics:
            self._metrics.observe_stage_duration(stage_name, result.duration_ms / 1000)
            if result.llm_calls_made > 0:
                for _ in range(result.llm_calls_made):
                    self._metrics.inc_consolidation_llm(stage_name)
            if result.items_processed > 0:
                self._metrics.inc_facts_processed(stage_name, result.items_processed)
            if result.items_affected > 0:
                self._metrics.inc_facts_affected(stage_name, "affected", result.items_affected)

        return result

    async def _run_stage_9(
        self, context: ConsolidationContext, profile: Any,
        gateway_id: str, result: StageResult,
    ) -> None:
        """Stage 9: Recompute Salience — correlate scoring with outcome."""
        stage = self._stages.get(9)
        if not stage or not self._scoring_ledger:
            return

        # Load scoring ledger entries
        ledger_entries = await self._scoring_ledger.query_for_correlation(gateway_id)
        if not ledger_entries:
            result.details["skipped"] = "no scoring ledger data"
            return

        # Load current use counts from graph (BS-8)
        fact_ids = list({e.get("fact_id", "") for e in ledger_entries})
        current_counts = await self._load_current_use_counts(fact_ids, gateway_id)

        # Get base weights
        weights = profile.scoring_weights if profile else None
        if not weights:
            from elephantbroker.schemas.working_set import ScoringWeights
            weights = ScoringWeights()

        # Get previous deltas for EMA
        previous_deltas = {}
        if self._tuner and hasattr(self._tuner, "_delta_store") and self._tuner._delta_store:
            profile_id = context.profile_id or "coding"
            previous_deltas = await self._tuner._delta_store.get_deltas(
                profile_id, context.org_id, gateway_id,
            )

        deltas = await stage.run(ledger_entries, current_counts, weights, previous_deltas)
        result.items_processed = len(ledger_entries)
        result.items_affected = len(deltas)

        # Apply deltas via ScoringTuner (AD-7: after org overrides)
        if deltas and self._tuner:
            profile_id = context.profile_id or "coding"
            await self._tuner.apply_feedback(
                profile_id, deltas,
                org_id=context.org_id, gateway_id=gateway_id,
            )
            if self._metrics:
                for d in deltas:
                    self._metrics.inc_tuner_adjustment(d.dimension.value)
                    self._metrics.observe_tuner_magnitude(d.dimension.value, abs(d.delta))

    async def _load_facts(self, gateway_id: str) -> list:
        """Load non-archived facts with active session protection (AD-2, AD-11)."""
        if not self._graph:
            return []

        from elephantbroker.runtime.adapters.cognee.datapoints import FactDataPoint
        from elephantbroker.schemas.consolidation import ConsolidationConfig

        cfg = getattr(self._config, "consolidation", None) or ConsolidationConfig()
        batch_size = cfg.batch_size if isinstance(cfg, ConsolidationConfig) else 500
        protection_hours = cfg.active_session_protection_hours if isinstance(cfg, ConsolidationConfig) else 1.0

        cutoff = datetime.now(UTC) - timedelta(hours=protection_hours)
        cutoff_ms = int(cutoff.timestamp() * 1000)

        all_facts = []
        offset = 0

        while True:
            try:
                records = await self._graph.query_cypher(
                    "MATCH (f:FactDataPoint) "
                    "WHERE f.gateway_id = $gw "
                    "AND (f.archived IS NULL OR f.archived = false) "
                    "AND (f.eb_last_used_at IS NULL OR f.eb_last_used_at < $cutoff) "
                    "RETURN properties(f) AS props "
                    "ORDER BY f.eb_created_at "
                    "SKIP $offset LIMIT $batch_size",
                    {"gw": gateway_id, "cutoff": cutoff_ms, "offset": offset, "batch_size": batch_size},
                )
            except Exception:
                self._log.warning("Fact loading failed at offset %d", offset, exc_info=True)
                break

            if not records:
                break

            for row in records:
                props = row.get("props", {})
                try:
                    from elephantbroker.runtime.graph_utils import clean_graph_props
                    props = clean_graph_props(props)
                    dp = FactDataPoint(**props)
                    all_facts.append(dp.to_schema())
                except Exception:
                    continue

            if len(records) < batch_size:
                break
            offset += batch_size

        self._log.info("Loaded %d facts for consolidation (gateway=%s)", len(all_facts), gateway_id)
        return all_facts

    async def _load_current_use_counts(
        self, fact_ids: list[str], gateway_id: str,
    ) -> dict[str, int]:
        """Batch load current successful_use_count from graph (BS-8)."""
        if not self._graph or not fact_ids:
            return {}
        try:
            records = await self._graph.query_cypher(
                "MATCH (f:FactDataPoint) "
                "WHERE f.eb_id IN $ids AND f.gateway_id = $gw "
                "RETURN f.eb_id AS fid, f.successful_use_count AS suc",
                {"ids": fact_ids, "gw": gateway_id},
            )
            return {r["fid"]: r.get("suc", 0) for r in records}
        except Exception:
            self._log.warning("Failed to load current use counts")
            return {}

    async def _apply_fact_upserts(self, facts: list, gateway_id: str) -> None:
        """Batch upsert modified facts via add_data_points()."""
        try:
            from cognee.tasks.storage import add_data_points

            from elephantbroker.runtime.adapters.cognee.datapoints import FactDataPoint

            dps = [FactDataPoint.from_schema(f) for f in facts if hasattr(f, "id")]
            if dps:
                await add_data_points(dps)
        except Exception:
            self._log.warning("Fact batch upsert failed", exc_info=True)

    async def _archive_fact(self, fact_id: str, gateway_id: str) -> None:
        """Archive a fact: delete Qdrant embedding (AD-3)."""
        if self._vector:
            try:
                await self._vector.delete_embedding("FactDataPoint_text", fact_id)
            except Exception:
                self._log.debug("Qdrant delete failed for %s", fact_id)

    async def _run_cleanup(self) -> None:
        """Cleanup old data from all SQLite stores (TD-8)."""
        retention_days = 90
        if self._config and hasattr(self._config, "audit"):
            retention_days = getattr(self._config.audit, "retention_days", 90)

        retention_seconds = retention_days * 86400

        for store_attr, method_name, arg in [
            ("_scoring_ledger", "cleanup_old", retention_seconds),
            ("_report_store", "cleanup_old", retention_days),
            ("_proc_audit", "cleanup_old", retention_days),
            ("_goal_audit", "cleanup_old", retention_days),
        ]:
            store = getattr(self, store_attr, None)
            if store and hasattr(store, method_name):
                try:
                    await getattr(store, method_name)(arg)
                except Exception:
                    self._log.debug("Cleanup failed for %s", store_attr)

    def _build_summary(self, stages: list[StageResult]) -> ConsolidationSummary:
        """Build summary from stage results."""
        summary = ConsolidationSummary()
        for sr in stages:
            if sr.stage == 2:
                summary.duplicates_merged = sr.items_affected
            elif sr.stage == 3:
                summary.facts_strengthened = sr.items_affected
            elif sr.stage == 4:
                summary.facts_decayed = sr.items_affected
                summary.facts_archived = sr.details.get("archived", 0)
            elif sr.stage == 5:
                summary.autorecall_blacklisted = sr.items_affected
            elif sr.stage == 6:
                summary.episodic_promoted = sr.items_affected
            elif sr.stage == 7:
                summary.procedures_suggested = sr.items_affected
            elif sr.stage == 8:
                summary.verification_gaps_found = sr.items_affected
        return summary

    async def run_stage(
        self, stage_num: int, org_id: str, gateway_id: str,
        context: ConsolidationContext,
    ) -> StageResult:
        """Run a single stage for testing/debugging."""
        report = ConsolidationReport(org_id=org_id, gateway_id=gateway_id)
        profile = context.resolved_profile
        return await self._run_single_stage(stage_num, gateway_id, context, profile, report)

    async def get_consolidation_report(self, report_id: str) -> ConsolidationReport | None:
        """Retrieve a previous report from SQLite."""
        if self._report_store:
            return await self._report_store.get_report(report_id)
        return None
