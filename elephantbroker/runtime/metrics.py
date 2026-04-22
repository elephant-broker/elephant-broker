"""Prometheus metrics definitions, safe helpers, and MetricsContext."""
from __future__ import annotations

import logging

logger = logging.getLogger("elephantbroker.runtime.metrics")

METRICS_AVAILABLE = False

try:
    from prometheus_client import Counter, Gauge, Histogram

    eb_memory_store_total = Counter("eb_memory_store_total", "Total memory operations", ["gateway_id", "operation", "status"])
    eb_memory_store_duration = Histogram("eb_memory_store_duration_seconds", "Op latency", ["gateway_id", "operation"])
    eb_facts_stored_total = Counter("eb_facts_stored_total", "Facts stored", ["gateway_id", "memory_class", "profile_name"])
    eb_facts_superseded_total = Counter("eb_facts_superseded_total", "Facts superseded", ["gateway_id", "profile_name"])
    eb_dedup_checks_total = Counter("eb_dedup_checks_total", "Dedup outcomes", ["gateway_id", "result"])
    eb_retrieval_total = Counter("eb_retrieval_total", "Retrieval ops", ["gateway_id", "auto_recall", "profile_name"])
    eb_retrieval_duration = Histogram("eb_retrieval_duration_seconds", "Retrieval latency", ["gateway_id", "auto_recall", "profile_name"])
    eb_pipeline_runs_total = Counter("eb_pipeline_runs_total", "Pipeline runs", ["gateway_id", "pipeline", "status"])
    eb_pipeline_duration = Histogram("eb_pipeline_duration_seconds", "Pipeline latency", ["gateway_id", "pipeline"])
    eb_llm_calls_total = Counter("eb_llm_calls_total", "LLM calls", ["gateway_id", "operation", "status", "model"])
    eb_llm_duration = Histogram("eb_llm_duration_seconds", "LLM latency", ["gateway_id", "operation", "model"])
    eb_llm_tokens_used = Counter("eb_llm_tokens_used", "Token consumption", ["gateway_id", "direction", "model"])
    eb_ingest_buffer_flushes_total = Counter("eb_ingest_buffer_flushes_total", "Buffer flushes", ["gateway_id", "trigger"])
    eb_ingest_gate_skips_total = Counter("eb_ingest_gate_skips_total", "Ingest gate skips (FULL mode)", ["gateway_id", "reason"])
    # TODO-6-302 (cluster C-boundary-source): surface the P4 hybrid-A+C
    # response-boundary decision as a Prometheus counter. Values bounded to
    # {empty, plugin, derived} — stable cardinality. Operators can alert on
    # `source="derived"` to catch OpenClaw silently stopping to emit
    # prePromptMessageCount.
    eb_after_turn_boundary_source_total = Counter(
        "eb_after_turn_boundary_source_total",
        "P4 response-boundary source per after_turn",
        ["gateway_id", "source"],
    )
    # TODO-6-105 / TODO-6-306 (cluster C-response-delta-no-user): surface
    # the `_extract_response_delta` no-user-role fallback branch. When the
    # tail-walker finds no role="user" message, the whole envelope is
    # returned as "response delta" — bounded blast radius (downstream
    # scanners filter to role="assistant") but silent until now. Operators
    # can alert on `rate(...) > 0` to catch malformed/heartbeat/subagent
    # envelopes reaching after_turn.
    eb_response_delta_no_user_total = Counter(
        "eb_response_delta_no_user_total",
        "P4 _extract_response_delta no-user-role fallback fires per after_turn",
        ["gateway_id"],
    )
    eb_session_active = Gauge("eb_session_active", "Active sessions", ["gateway_id", "profile_name"])
    eb_edges_created_total = Counter("eb_edges_created_total", "Graph edges created", ["gateway_id", "edge_type"])
    eb_edges_failed_total = Counter("eb_edges_failed_total", "Failed edges", ["gateway_id", "edge_type"])
    eb_cognify_runs_total = Counter("eb_cognify_runs_total", "Cognify runs", ["gateway_id", "status"])
    eb_cognify_duration = Histogram("eb_cognify_duration_seconds", "Cognify latency", ["gateway_id"])
    eb_gdpr_deletes_total = Counter("eb_gdpr_deletes_total", "GDPR deletions", ["gateway_id"])
    eb_backend_health = Gauge("eb_backend_health", "Backend health 1=ok 0=down", ["gateway_id", "component"])
    eb_degraded_operations_total = Counter("eb_degraded_operations_total", "Degraded ops", ["gateway_id", "component", "operation"])
    eb_cognee_data_id_capture_failures_total = Counter(
        "eb_cognee_data_id_capture_failures_total",
        "Times cognee.add() returned a shape the facade could not extract a data_id from — the fact is stored with cognee_data_id=None and the TD-50 delete cascade will skip Cognee cleanup",
        ["gateway_id", "operation"],
    )
    eb_recent_facts_scrubbed_total = Counter(
        "eb_recent_facts_scrubbed_total",
        "recent_facts GDPR buffer scrub outcomes on delete (TF-ER-003 Tier A). status=scrubbed when the deleted fact was removed from the extraction-context window, noop when the fact was not present, failure when Redis raised.",
        ["gateway_id", "status"],
    )
    eb_fact_delete_cascade_failures_total = Counter(
        "eb_fact_delete_cascade_failures_total",
        "TD-50 cascade step failures. step=graph|vector|cognee_data identifies which layer threw; operation=delete|update|canonicalize identifies the parent op so dashboards can split delete-path cascade failures from update-path (superseded-doc cleanup after text change, TODO-5-110) and consolidation canonicalize-path (superseded-member cleanup, TODO-5-901). The EB-layer operation continues on each failure (best-effort cascade) so a step-level counter increment is compatible with an eventually-emitted trace whose cascade_status marks that step as failed.",
        ["gateway_id", "step", "operation"],
    )
    eb_memory_search_stage_failures_total = Counter(
        "eb_memory_search_stage_failures_total",
        "Search stage failures across memory read paths. stage label carries the failing source — for MemoryStoreFacade.search() Stage 1: semantic; for the 5-source RetrievalOrchestrator (TODO-5-508): structural|keyword|vector|graph|artifact. exception_type carries the Python exception class name. Search downgrades to partial results (still returns a list) rather than crashing; this counter makes the per-source failure visible.",
        ["gateway_id", "stage", "exception_type"],
    )

    # Phase 5 metrics
    eb_working_set_builds_total = Counter("eb_working_set_builds_total", "Working set builds", ["gateway_id", "profile_name", "status"])
    eb_working_set_build_duration = Histogram("eb_working_set_build_duration_seconds", "Build latency", ["gateway_id", "profile_name"])
    eb_working_set_candidates = Histogram("eb_working_set_candidates", "Candidates generated", ["gateway_id", "source_type"])
    eb_working_set_selected = Histogram("eb_working_set_selected", "Items selected", ["gateway_id"])
    eb_working_set_tokens_used = Histogram("eb_working_set_tokens_used", "Tokens used per build", ["gateway_id"])
    eb_working_set_must_inject_total = Counter("eb_working_set_must_inject_total", "Must-inject items", ["gateway_id"])
    eb_rerank_calls_total = Counter("eb_rerank_calls_total", "Rerank calls", ["gateway_id", "status"])
    eb_rerank_duration = Histogram("eb_rerank_duration_seconds", "Rerank latency", ["gateway_id", "stage"])
    eb_rerank_fallbacks_total = Counter("eb_rerank_fallbacks_total", "Rerank fallbacks", ["gateway_id"])
    eb_rerank_candidates_in = Histogram("eb_rerank_candidates_in", "Candidates submitted to reranker", ["gateway_id"])
    eb_rerank_candidates_out = Histogram("eb_rerank_candidates_out", "Candidates returned from reranker", ["gateway_id"])
    eb_embedding_cache_total = Counter("eb_embedding_cache_total", "Embedding cache hits/misses", ["gateway_id", "result"])
    eb_embedding_cache_batch_size = Histogram("eb_embedding_cache_batch_size", "Batch sizes", ["gateway_id"])
    eb_embedding_cache_latency = Histogram("eb_embedding_cache_latency_seconds", "Cache operation latency", ["gateway_id", "operation"])
    eb_goal_hints_total = Counter("eb_goal_hints_total", "Goal hints processed", ["gateway_id", "hint_type"])
    eb_goal_refinement_calls_total = Counter("eb_goal_refinement_calls_total", "Goal refinement LLM calls", ["gateway_id"])
    eb_goal_refinement_duration = Histogram("eb_goal_refinement_duration_seconds", "Refinement latency", ["gateway_id"])
    eb_session_goals_count = Gauge("eb_session_goals_count", "Active session goals", ["gateway_id"])
    eb_session_goals_flushed_total = Counter("eb_session_goals_flushed_total", "Goals flushed to Cognee", ["gateway_id"])
    eb_subgoals_created_total = Counter("eb_subgoals_created_total", "Sub-goals created", ["gateway_id"])
    eb_subgoals_dedup_skipped_total = Counter("eb_subgoals_dedup_skipped_total", "Sub-goals skipped (dedup)", ["gateway_id"])
    eb_session_goals_tool_calls_total = Counter("eb_session_goals_tool_calls_total", "Session goals tool calls", ["gateway_id", "tool"])
    eb_procedure_qualified_total = Counter("eb_procedure_qualified_total", "Procedures qualified for context", ["gateway_id"])
    eb_procedure_activated_total = Counter("eb_procedure_activated_total", "Procedures activated", ["gateway_id"])
    eb_procedure_step_completed_total = Counter("eb_procedure_step_completed_total", "Procedure steps completed", ["gateway_id"])
    eb_procedure_proof_submitted_total = Counter("eb_procedure_proof_submitted_total", "Proofs submitted", ["gateway_id", "proof_type"])
    eb_procedure_completed_total = Counter("eb_procedure_completed_total", "Procedures completed", ["gateway_id"])
    eb_procedure_tool_calls_total = Counter("eb_procedure_tool_calls_total", "Procedure tool calls", ["gateway_id", "tool"])

    # Phase 6 metrics
    eb_compaction_triggered_total = Counter("eb_compaction_triggered_total", "Compaction triggers", ["gateway_id", "cadence", "trigger"])
    eb_compaction_tokens = Histogram("eb_compaction_tokens", "Compaction tokens", ["gateway_id", "phase"])
    eb_compaction_classification_total = Counter("eb_compaction_classification_total", "Message classifications", ["gateway_id", "classification"])
    eb_compaction_llm_calls_total = Counter("eb_compaction_llm_calls_total", "Compaction LLM calls", ["gateway_id"])
    eb_assembly_tokens_used = Histogram("eb_assembly_tokens_used", "Assembly tokens", ["gateway_id", "profile_name"])
    eb_assembly_block_tokens = Histogram("eb_assembly_block_tokens", "Block tokens", ["gateway_id", "block"])
    eb_budget_resolution_tokens = Histogram("eb_budget_resolution_tokens", "Budget resolution", ["gateway_id", "source"])
    eb_tool_replacements_total = Counter("eb_tool_replacements_total", "Tool replacements", ["gateway_id", "tool_name"])
    eb_tool_tokens_saved_total = Counter("eb_tool_tokens_saved_total", "Tool tokens saved", ["gateway_id"])
    # `source_type` label union-semantics (T-3 compromise):
    # For FACT rows (the majority) this label holds the retrieval-PATH value
    # (structural / keyword / vector / graph) — preserves pre-T-3 dashboard
    # cardinality so existing alerts/panels keep working.
    # For NON-FACT rows (artifact / goal / persistent_goal / procedure) this
    # label holds the DataPoint-TYPE value (the row HAS no retrieval_source;
    # it was produced by the scoring pipeline, not a retrieval orchestrator).
    # A clean T-3-aligned split would add a second `retrieval_source` label
    # and have this label hold only DataPoint types — deferred because that
    # would break every operator dashboard/alert keyed on the current union.
    # See lifecycle.py:968-972 + manager.py:163-164 for the stamp sites.
    # The same union-semantics applies to `eb_working_set_candidates` above
    # (line 81), which is fed by the manager.py stamp site.
    eb_injection_referenced_total = Counter("eb_injection_referenced_total", "Items referenced", ["gateway_id", "category", "memory_class", "source_type"])
    eb_injection_ignored_total = Counter("eb_injection_ignored_total", "Items ignored", ["gateway_id", "category", "memory_class", "source_type"])
    eb_subagent_spawns_total = Counter("eb_subagent_spawns_total", "Subagent spawns", ["gateway_id"])
    eb_subagent_packet_tokens = Histogram("eb_subagent_packet_tokens", "Subagent packet tokens", ["gateway_id"])
    eb_lifecycle_calls_total = Counter("eb_lifecycle_calls_total", "Lifecycle calls", ["gateway_id", "method", "profile_name"])
    eb_lifecycle_duration = Histogram("eb_lifecycle_duration_seconds", "Lifecycle latency", ["gateway_id", "method", "profile_name"])
    eb_lifecycle_errors_total = Counter("eb_lifecycle_errors_total", "Lifecycle errors", ["gateway_id", "method", "error_type"])
    eb_successful_use_updates_total = Counter("eb_successful_use_updates_total", "Successful use updates", ["gateway_id", "method"])
    eb_successful_use_jaccard = Histogram("eb_successful_use_jaccard_score", "Jaccard scores", ["gateway_id"])
    eb_context_window_reported_total = Counter("eb_context_window_reported_total", "Context window reports", ["gateway_id", "provider", "model"])
    eb_token_usage_input = Histogram("eb_token_usage_input_tokens", "Token usage input", ["gateway_id"])
    eb_token_usage_output = Histogram("eb_token_usage_output_tokens", "Token usage output", ["gateway_id"])
    eb_fact_attribution_total = Counter("eb_fact_attribution_total", "Fact attributions", ["gateway_id", "role"])

    # Phase 7 metrics — Guard pipeline
    eb_guard_checks_total = Counter("eb_guard_checks_total", "Guard checks", ["gateway_id", "outcome"])
    eb_guard_check_duration = Histogram("eb_guard_check_duration_seconds", "Guard check latency", ["gateway_id"])
    eb_guard_layer_triggers = Counter("eb_guard_layer_triggers_total", "Guard layer triggers", ["gateway_id", "layer"])
    eb_guard_near_misses = Counter("eb_guard_near_misses_total", "Near misses", ["gateway_id"])
    eb_guard_reinjections = Counter("eb_guard_reinjections_total", "Constraint reinjections", ["gateway_id"])
    eb_guard_llm_escalations = Counter("eb_guard_llm_escalations_total", "LLM escalations", ["gateway_id"])
    eb_guard_near_miss_escalations = Counter("eb_guard_near_miss_escalations_total", "Near-miss LLM escalations", ["gateway_id"])
    eb_hitl_retry_exhausted = Counter("eb_hitl_retry_exhausted_total", "HITL webhook retry exhaustions", ["gateway_id"])
    eb_autonomy_classifications = Counter("eb_autonomy_classifications_total", "Domain classifications", ["gateway_id", "domain", "level"])
    eb_autonomy_domain_tier = Counter("eb_autonomy_domain_tier_total", "Classification tier used", ["gateway_id", "tier"])
    eb_autonomy_hard_stops = Counter("eb_autonomy_hard_stops_total", "Hard stops", ["gateway_id", "domain"])
    eb_approval_requests = Counter("eb_approval_requests_total", "Approval requests", ["gateway_id", "domain"])
    eb_guard_bm25_score = Histogram("eb_guard_bm25_score_max", "Top BM25 scores", ["gateway_id"])
    eb_guard_semantic_score = Histogram("eb_guard_semantic_score_max", "Top semantic scores", ["gateway_id"])
    eb_guard_bm25_short_circuit = Counter("eb_guard_bm25_short_circuit_total", "BM25 definitive (skipped embedding)", ["gateway_id"])
    eb_verification_runs = Counter("eb_verification_runs_total", "Verification pipeline runs", ["gateway_id", "result"])
    eb_completion_checks = Counter("eb_completion_checks_total", "Completion gate checks", ["gateway_id", "result"])

    # Amendment 6.2: Async injection analysis metrics
    eb_async_analysis_calls = Counter("eb_async_analysis_calls_total", "Async analyzer invocations", ["gateway_id"])
    eb_async_analysis_matches = Counter("eb_async_analysis_matches_total", "Items exceeding similarity threshold", ["gateway_id"])
    eb_async_analysis_similarity = Histogram("eb_async_analysis_similarity_max", "Max similarity per item", ["gateway_id"])
    eb_async_analysis_items = Histogram("eb_async_analysis_items_processed", "Items processed per batch", ["gateway_id"])

    # --- Phase 9: Consolidation ---
    eb_consolidation_runs_total = Counter("eb_consolidation_runs_total", "Consolidation runs", ["gateway_id", "status"])
    eb_consolidation_duration = Histogram("eb_consolidation_duration_seconds", "Total consolidation duration", ["gateway_id"])
    eb_consolidation_stage_duration = Histogram("eb_consolidation_stage_duration_seconds", "Per-stage duration", ["gateway_id", "stage"])
    eb_consolidation_facts_processed = Counter("eb_consolidation_facts_processed_total", "Facts processed per stage", ["gateway_id", "stage"])
    eb_consolidation_facts_affected = Counter("eb_consolidation_facts_affected_total", "Facts affected", ["gateway_id", "stage", "action"])
    eb_consolidation_llm_calls = Counter("eb_consolidation_llm_calls_total", "LLM calls in consolidation", ["gateway_id", "stage"])
    eb_consolidation_suggestions = Counter("eb_consolidation_suggestions_total", "Procedure suggestions", ["gateway_id", "status"])
    eb_scoring_tuner_adjustments = Counter("eb_scoring_tuner_adjustments_total", "Weight adjustments applied", ["gateway_id", "dimension"])
    eb_scoring_tuner_magnitude = Histogram("eb_scoring_tuner_adjustment_magnitude", "Delta magnitude", ["gateway_id", "dimension"])

    eb_goal_create_total = Counter(
        "eb_goal_create_total",
        "Persistent goals created",
        ["gateway_id"],
    )

    eb_session_boundary_total = Counter(
        "eb_session_boundary_total",
        "Session boundary events (start/end)",
        ["gateway_id", "action"],
    )

    eb_session_ttl_touch_total = Counter(
        "eb_session_ttl_touch_total",
        "Session TTL refresh operations",
        ["gateway_id"],
    )
    eb_session_ttl_touch_keys = Histogram(
        "eb_session_ttl_touch_keys",
        "Keys refreshed per touch operation",
        ["gateway_id"],
        buckets=[0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
    )

    # Phase 8: Profile, authority, admin, org/team metrics
    eb_profile_resolve_total = Counter(
        "eb_profile_resolve_total", "Profile resolutions",
        ["gateway_id", "profile_name", "has_org_override"],
    )
    eb_profile_resolve_duration = Histogram(
        "eb_profile_resolve_duration_seconds", "Profile resolution latency", ["gateway_id"],
    )
    eb_profile_cache_total = Counter(
        "eb_profile_cache_total", "Profile cache ops", ["gateway_id", "result"],
    )
    eb_authority_checks_total = Counter(
        "eb_authority_checks_total", "Authority checks", ["gateway_id", "action", "result"],
    )
    eb_admin_ops_total = Counter(
        "eb_admin_ops_total", "Admin API ops", ["gateway_id", "operation", "status"],
    )
    eb_org_team_edges_total = Counter(
        "eb_org_team_edges_total", "Org/team edge ops", ["gateway_id", "edge_type", "operation"],
    )
    eb_goal_scope_filter_total = Counter(
        "eb_goal_scope_filter_total", "Goal scope filter", ["gateway_id", "scope"],
    )
    eb_goal_scope_filter_duration = Histogram(
        "eb_goal_scope_filter_duration_seconds", "Goal scope filter latency", ["gateway_id"],
    )
    eb_handle_resolution_total = Counter(
        "eb_handle_resolution_total", "Handle lookups", ["gateway_id", "result"],
    )
    eb_actor_merge_total = Counter(
        "eb_actor_merge_total", "Actor merges", ["gateway_id", "status"],
    )
    eb_bootstrap_mode_active = Gauge(
        "eb_bootstrap_mode_active", "Bootstrap mode", ["gateway_id"],
    )

    METRICS_AVAILABLE = True
except ImportError:
    logger.warning("prometheus_client not available, metrics disabled")


# --- Safe helpers (backward-compat: gateway_id defaults to "") ---

def inc_store(operation: str, status: str, gateway_id: str = "") -> None:
    if METRICS_AVAILABLE:
        eb_memory_store_total.labels(gateway_id=gateway_id, operation=operation, status=status).inc()


def inc_dedup(result: str, gateway_id: str = "") -> None:
    if METRICS_AVAILABLE:
        eb_dedup_checks_total.labels(gateway_id=gateway_id, result=result).inc()


def inc_edge(edge_type: str, success: bool, gateway_id: str = "") -> None:
    if METRICS_AVAILABLE:
        if success:
            eb_edges_created_total.labels(gateway_id=gateway_id, edge_type=edge_type).inc()
        else:
            eb_edges_failed_total.labels(gateway_id=gateway_id, edge_type=edge_type).inc()


def inc_gdpr_delete(gateway_id: str = "") -> None:
    if METRICS_AVAILABLE:
        eb_gdpr_deletes_total.labels(gateway_id=gateway_id).inc()


def inc_pipeline(pipeline: str, status: str, gateway_id: str = "") -> None:
    if METRICS_AVAILABLE:
        eb_pipeline_runs_total.labels(gateway_id=gateway_id, pipeline=pipeline, status=status).inc()


def inc_cognify(status: str, gateway_id: str = "") -> None:
    if METRICS_AVAILABLE:
        eb_cognify_runs_total.labels(gateway_id=gateway_id, status=status).inc()


def inc_buffer_flush(trigger: str, gateway_id: str = "") -> None:
    if METRICS_AVAILABLE:
        eb_ingest_buffer_flushes_total.labels(gateway_id=gateway_id, trigger=trigger).inc()


def inc_ingest_gate_skip(reason: str, gateway_id: str = "") -> None:
    if METRICS_AVAILABLE:
        eb_ingest_gate_skips_total.labels(gateway_id=gateway_id, reason=reason).inc()


def inc_after_turn_boundary_source(source: str, gateway_id: str = "") -> None:
    """Increment the P4 hybrid-A+C boundary-source counter.

    ``source`` is bounded to ``{"empty", "plugin", "derived"}`` by the
    lifecycle code that calls this (see lifecycle.py:~884-892), so label
    cardinality is stable at 3 per gateway_id.
    """
    if METRICS_AVAILABLE:
        eb_after_turn_boundary_source_total.labels(
            gateway_id=gateway_id, source=source,
        ).inc()


def inc_response_delta_no_user(gateway_id: str = "") -> None:
    """Increment the P4 _extract_response_delta no-user-role fallback counter.

    Fires when the tail-walker in ``ContextLifecycle._extract_response_delta``
    cannot find a ``role=="user"`` message in the envelope — the whole list
    is returned as response delta (defensive fallback). Single bounded label
    (``gateway_id``); the counter is a pure incident signal.
    """
    if METRICS_AVAILABLE:
        eb_response_delta_no_user_total.labels(gateway_id=gateway_id).inc()


def inc_cognee_capture_failure(operation: str, gateway_id: str = "") -> None:
    if METRICS_AVAILABLE:
        eb_cognee_data_id_capture_failures_total.labels(
            gateway_id=gateway_id, operation=operation,
        ).inc()


def inc_recent_facts_scrubbed(status: str, gateway_id: str = "") -> None:
    if METRICS_AVAILABLE:
        eb_recent_facts_scrubbed_total.labels(
            gateway_id=gateway_id, status=status,
        ).inc()


def inc_fact_delete_cascade_failure(
    step: str, operation: str = "delete", gateway_id: str = "",
) -> None:
    if METRICS_AVAILABLE:
        eb_fact_delete_cascade_failures_total.labels(
            gateway_id=gateway_id, step=step, operation=operation,
        ).inc()


def inc_search_stage_failure(stage: str, exception_type: str, gateway_id: str = "") -> None:
    if METRICS_AVAILABLE:
        eb_memory_search_stage_failures_total.labels(
            gateway_id=gateway_id, stage=stage, exception_type=exception_type,
        ).inc()


# --- Phase 5 safe helpers ---

def inc_working_set_build(profile_name: str, status: str = "ok", gateway_id: str = "") -> None:
    if METRICS_AVAILABLE:
        eb_working_set_builds_total.labels(gateway_id=gateway_id, profile_name=profile_name, status=status).inc()


def inc_rerank_call(status: str, gateway_id: str = "") -> None:
    if METRICS_AVAILABLE:
        eb_rerank_calls_total.labels(gateway_id=gateway_id, status=status).inc()


def inc_rerank_fallback(gateway_id: str = "") -> None:
    if METRICS_AVAILABLE:
        eb_rerank_fallbacks_total.labels(gateway_id=gateway_id).inc()


def inc_embedding_cache(result: str, gateway_id: str = "") -> None:
    if METRICS_AVAILABLE:
        eb_embedding_cache_total.labels(gateway_id=gateway_id, result=result).inc()


def inc_goal_hint(hint_type: str, gateway_id: str = "") -> None:
    if METRICS_AVAILABLE:
        eb_goal_hints_total.labels(gateway_id=gateway_id, hint_type=hint_type).inc()


def inc_procedure_qualified(gateway_id: str = "") -> None:
    if METRICS_AVAILABLE:
        eb_procedure_qualified_total.labels(gateway_id=gateway_id).inc()


def inc_procedure_activated(gateway_id: str = "") -> None:
    if METRICS_AVAILABLE:
        eb_procedure_activated_total.labels(gateway_id=gateway_id).inc()


def inc_procedure_step_completed(gateway_id: str = "") -> None:
    if METRICS_AVAILABLE:
        eb_procedure_step_completed_total.labels(gateway_id=gateway_id).inc()


def inc_procedure_proof(proof_type: str, gateway_id: str = "") -> None:
    if METRICS_AVAILABLE:
        eb_procedure_proof_submitted_total.labels(gateway_id=gateway_id, proof_type=proof_type).inc()


# ---------------------------------------------------------------------------
# MetricsContext — gateway-aware wrapper that auto-injects gateway_id
# ---------------------------------------------------------------------------

class MetricsContext:
    """Scoped metrics helper that auto-injects gateway_id on every call."""

    def __init__(self, gateway_id: str) -> None:
        self._gw = gateway_id

    def inc_store(self, operation: str, status: str) -> None:
        inc_store(operation, status, gateway_id=self._gw)

    def inc_dedup(self, result: str) -> None:
        inc_dedup(result, gateway_id=self._gw)

    def inc_edge(self, edge_type: str, success: bool) -> None:
        inc_edge(edge_type, success, gateway_id=self._gw)

    def inc_gdpr_delete(self) -> None:
        inc_gdpr_delete(gateway_id=self._gw)

    def inc_pipeline(self, pipeline: str, status: str) -> None:
        inc_pipeline(pipeline, status, gateway_id=self._gw)

    def inc_cognify(self, status: str) -> None:
        inc_cognify(status, gateway_id=self._gw)

    def inc_buffer_flush(self, trigger: str) -> None:
        inc_buffer_flush(trigger, gateway_id=self._gw)

    def inc_ingest_gate_skip(self, reason: str) -> None:
        inc_ingest_gate_skip(reason, gateway_id=self._gw)

    def inc_after_turn_boundary_source(self, source: str) -> None:
        inc_after_turn_boundary_source(source, gateway_id=self._gw)

    def inc_response_delta_no_user_boundary(self) -> None:
        inc_response_delta_no_user(gateway_id=self._gw)

    def inc_cognee_capture_failure(self, operation: str) -> None:
        inc_cognee_capture_failure(operation, gateway_id=self._gw)

    def inc_recent_facts_scrubbed(self, status: str) -> None:
        inc_recent_facts_scrubbed(status, gateway_id=self._gw)

    def inc_fact_delete_cascade_failure(
        self, step: str, operation: str = "delete",
    ) -> None:
        inc_fact_delete_cascade_failure(
            step, operation=operation, gateway_id=self._gw,
        )

    def inc_search_stage_failure(self, stage: str, exception_type: str) -> None:
        inc_search_stage_failure(stage, exception_type, gateway_id=self._gw)

    def inc_working_set_build(self, profile_name: str, status: str = "ok") -> None:
        inc_working_set_build(profile_name, status, gateway_id=self._gw)

    def inc_rerank_call(self, status: str) -> None:
        inc_rerank_call(status, gateway_id=self._gw)

    def inc_rerank_fallback(self) -> None:
        inc_rerank_fallback(gateway_id=self._gw)

    def inc_embedding_cache(self, result: str) -> None:
        inc_embedding_cache(result, gateway_id=self._gw)

    def inc_goal_hint(self, hint_type: str) -> None:
        inc_goal_hint(hint_type, gateway_id=self._gw)

    def inc_procedure_qualified(self) -> None:
        inc_procedure_qualified(gateway_id=self._gw)

    def inc_procedure_activated(self) -> None:
        inc_procedure_activated(gateway_id=self._gw)

    def inc_procedure_step_completed(self) -> None:
        inc_procedure_step_completed(gateway_id=self._gw)

    def inc_procedure_proof(self, proof_type: str) -> None:
        inc_procedure_proof(proof_type, gateway_id=self._gw)

    def observe_store_duration(self, operation: str, duration: float) -> None:
        if METRICS_AVAILABLE:
            eb_memory_store_duration.labels(gateway_id=self._gw, operation=operation).observe(duration)

    def inc_facts_stored(self, memory_class: str, profile_name: str) -> None:
        if METRICS_AVAILABLE:
            eb_facts_stored_total.labels(gateway_id=self._gw, memory_class=memory_class, profile_name=profile_name).inc()

    def inc_facts_superseded(self, profile_name: str) -> None:
        if METRICS_AVAILABLE:
            eb_facts_superseded_total.labels(gateway_id=self._gw, profile_name=profile_name).inc()

    def inc_retrieval(self, auto_recall: str, profile_name: str) -> None:
        if METRICS_AVAILABLE:
            eb_retrieval_total.labels(gateway_id=self._gw, auto_recall=auto_recall, profile_name=profile_name).inc()

    def observe_retrieval_duration(self, auto_recall: str, profile_name: str, duration: float) -> None:
        if METRICS_AVAILABLE:
            eb_retrieval_duration.labels(gateway_id=self._gw, auto_recall=auto_recall, profile_name=profile_name).observe(duration)

    def observe_pipeline_duration(self, pipeline: str, duration: float) -> None:
        if METRICS_AVAILABLE:
            eb_pipeline_duration.labels(gateway_id=self._gw, pipeline=pipeline).observe(duration)

    def inc_llm_call(self, operation: str, status: str, model: str) -> None:
        if METRICS_AVAILABLE:
            eb_llm_calls_total.labels(gateway_id=self._gw, operation=operation, status=status, model=model).inc()

    def observe_llm_duration(self, operation: str, model: str, duration: float) -> None:
        if METRICS_AVAILABLE:
            eb_llm_duration.labels(gateway_id=self._gw, operation=operation, model=model).observe(duration)

    def inc_llm_tokens(self, direction: str, model: str, count: int) -> None:
        if METRICS_AVAILABLE:
            eb_llm_tokens_used.labels(gateway_id=self._gw, direction=direction, model=model).inc(count)

    def set_session_active(self, profile_name: str, value: int) -> None:
        if METRICS_AVAILABLE:
            eb_session_active.labels(gateway_id=self._gw, profile_name=profile_name).set(value)

    def observe_working_set_duration(self, profile_name: str, duration: float) -> None:
        if METRICS_AVAILABLE:
            eb_working_set_build_duration.labels(gateway_id=self._gw, profile_name=profile_name).observe(duration)

    def observe_candidates(self, source_type: str, count: int) -> None:
        if METRICS_AVAILABLE:
            eb_working_set_candidates.labels(gateway_id=self._gw, source_type=source_type).observe(count)

    def observe_selected(self, count: int) -> None:
        if METRICS_AVAILABLE:
            eb_working_set_selected.labels(gateway_id=self._gw).observe(count)

    def observe_tokens_used(self, count: int) -> None:
        if METRICS_AVAILABLE:
            eb_working_set_tokens_used.labels(gateway_id=self._gw).observe(count)

    def inc_must_inject(self) -> None:
        if METRICS_AVAILABLE:
            eb_working_set_must_inject_total.labels(gateway_id=self._gw).inc()

    def observe_rerank_candidates_in(self, count: int) -> None:
        if METRICS_AVAILABLE:
            eb_rerank_candidates_in.labels(gateway_id=self._gw).observe(count)

    def observe_rerank_candidates_out(self, count: int) -> None:
        if METRICS_AVAILABLE:
            eb_rerank_candidates_out.labels(gateway_id=self._gw).observe(count)

    def observe_rerank_duration(self, stage: str, duration: float) -> None:
        if METRICS_AVAILABLE:
            eb_rerank_duration.labels(gateway_id=self._gw, stage=stage).observe(duration)

    def observe_embedding_cache_batch(self, size: int) -> None:
        if METRICS_AVAILABLE:
            eb_embedding_cache_batch_size.labels(gateway_id=self._gw).observe(size)

    def observe_embedding_cache_latency(self, operation: str, duration: float) -> None:
        if METRICS_AVAILABLE:
            eb_embedding_cache_latency.labels(gateway_id=self._gw, operation=operation).observe(duration)

    def inc_goal_refinement_call(self) -> None:
        if METRICS_AVAILABLE:
            eb_goal_refinement_calls_total.labels(gateway_id=self._gw).inc()

    def observe_goal_refinement_duration(self, duration: float) -> None:
        if METRICS_AVAILABLE:
            eb_goal_refinement_duration.labels(gateway_id=self._gw).observe(duration)

    def set_session_goals_count(self, value: int) -> None:
        if METRICS_AVAILABLE:
            eb_session_goals_count.labels(gateway_id=self._gw).set(value)

    def inc_session_goals_flushed(self) -> None:
        if METRICS_AVAILABLE:
            eb_session_goals_flushed_total.labels(gateway_id=self._gw).inc()

    def inc_subgoals_created(self) -> None:
        if METRICS_AVAILABLE:
            eb_subgoals_created_total.labels(gateway_id=self._gw).inc()

    def inc_subgoals_dedup_skipped(self) -> None:
        if METRICS_AVAILABLE:
            eb_subgoals_dedup_skipped_total.labels(gateway_id=self._gw).inc()

    def inc_session_goals_tool(self, tool: str) -> None:
        if METRICS_AVAILABLE:
            eb_session_goals_tool_calls_total.labels(gateway_id=self._gw, tool=tool).inc()

    def inc_procedure_completed(self) -> None:
        if METRICS_AVAILABLE:
            eb_procedure_completed_total.labels(gateway_id=self._gw).inc()

    def inc_procedure_tool(self, tool: str) -> None:
        if METRICS_AVAILABLE:
            eb_procedure_tool_calls_total.labels(gateway_id=self._gw, tool=tool).inc()

    def set_backend_health(self, component: str, value: int) -> None:
        if METRICS_AVAILABLE:
            eb_backend_health.labels(gateway_id=self._gw, component=component).set(value)

    def inc_degraded_op(self, component: str, operation: str) -> None:
        if METRICS_AVAILABLE:
            eb_degraded_operations_total.labels(gateway_id=self._gw, component=component, operation=operation).inc()

    def inc_cognify_duration(self, duration: float) -> None:
        if METRICS_AVAILABLE:
            eb_cognify_duration.labels(gateway_id=self._gw).observe(duration)

    # Phase 6 metrics methods

    def inc_compaction_triggered(self, cadence: str, trigger: str) -> None:
        if METRICS_AVAILABLE:
            eb_compaction_triggered_total.labels(gateway_id=self._gw, cadence=cadence, trigger=trigger).inc()

    def observe_compaction_tokens(self, phase: str, value: int) -> None:
        if METRICS_AVAILABLE:
            eb_compaction_tokens.labels(gateway_id=self._gw, phase=phase).observe(value)

    def inc_compaction_classification(self, classification: str) -> None:
        if METRICS_AVAILABLE:
            eb_compaction_classification_total.labels(gateway_id=self._gw, classification=classification).inc()

    def inc_compaction_llm_call(self) -> None:
        if METRICS_AVAILABLE:
            eb_compaction_llm_calls_total.labels(gateway_id=self._gw).inc()

    def observe_assembly_tokens(self, profile_name: str, value: int) -> None:
        if METRICS_AVAILABLE:
            eb_assembly_tokens_used.labels(gateway_id=self._gw, profile_name=profile_name).observe(value)

    def observe_block_tokens(self, block: str, value: int) -> None:
        if METRICS_AVAILABLE:
            eb_assembly_block_tokens.labels(gateway_id=self._gw, block=block).observe(value)

    def observe_budget_resolution(self, source: str, value: int) -> None:
        if METRICS_AVAILABLE:
            eb_budget_resolution_tokens.labels(gateway_id=self._gw, source=source).observe(value)

    def inc_tool_replacement(self, tool_name: str) -> None:
        if METRICS_AVAILABLE:
            eb_tool_replacements_total.labels(gateway_id=self._gw, tool_name=tool_name).inc()

    def inc_tool_tokens_saved(self, count: int = 1) -> None:
        if METRICS_AVAILABLE:
            eb_tool_tokens_saved_total.labels(gateway_id=self._gw).inc(count)

    def inc_injection_referenced(self, category: str, memory_class: str, source_type: str) -> None:
        if METRICS_AVAILABLE:
            eb_injection_referenced_total.labels(gateway_id=self._gw, category=category, memory_class=memory_class, source_type=source_type).inc()

    def inc_injection_ignored(self, category: str, memory_class: str, source_type: str) -> None:
        if METRICS_AVAILABLE:
            eb_injection_ignored_total.labels(gateway_id=self._gw, category=category, memory_class=memory_class, source_type=source_type).inc()

    def inc_subagent_spawn(self) -> None:
        if METRICS_AVAILABLE:
            eb_subagent_spawns_total.labels(gateway_id=self._gw).inc()

    def observe_subagent_packet_tokens(self, value: int) -> None:
        if METRICS_AVAILABLE:
            eb_subagent_packet_tokens.labels(gateway_id=self._gw).observe(value)

    def inc_lifecycle_call(self, method: str, profile_name: str) -> None:
        if METRICS_AVAILABLE:
            eb_lifecycle_calls_total.labels(gateway_id=self._gw, method=method, profile_name=profile_name).inc()

    def observe_lifecycle_duration(self, method: str, profile_name: str, duration: float) -> None:
        if METRICS_AVAILABLE:
            eb_lifecycle_duration.labels(gateway_id=self._gw, method=method, profile_name=profile_name).observe(duration)

    def inc_lifecycle_error(self, method: str, error_type: str) -> None:
        if METRICS_AVAILABLE:
            eb_lifecycle_errors_total.labels(gateway_id=self._gw, method=method, error_type=error_type).inc()

    def inc_successful_use_update(self, method: str) -> None:
        if METRICS_AVAILABLE:
            eb_successful_use_updates_total.labels(gateway_id=self._gw, method=method).inc()

    def observe_successful_use_jaccard(self, score: float) -> None:
        if METRICS_AVAILABLE:
            eb_successful_use_jaccard.labels(gateway_id=self._gw).observe(score)

    def inc_context_window_reported(self, provider: str, model: str) -> None:
        if METRICS_AVAILABLE:
            eb_context_window_reported_total.labels(gateway_id=self._gw, provider=provider, model=model).inc()

    def observe_token_usage(self, input_tokens: int, output_tokens: int) -> None:
        if METRICS_AVAILABLE:
            eb_token_usage_input.labels(gateway_id=self._gw).observe(input_tokens)
            eb_token_usage_output.labels(gateway_id=self._gw).observe(output_tokens)

    def inc_fact_attribution(self, role: str) -> None:
        if METRICS_AVAILABLE:
            eb_fact_attribution_total.labels(gateway_id=self._gw, role=role).inc()

    # --- Amendment 6.2: Async injection analysis ---

    def inc_async_analysis_call(self) -> None:
        try:
            eb_async_analysis_calls.labels(gateway_id=self._gw).inc()
        except Exception:
            pass

    def inc_async_analysis_match(self) -> None:
        try:
            eb_async_analysis_matches.labels(gateway_id=self._gw).inc()
        except Exception:
            pass

    def observe_async_analysis_similarity(self, value: float) -> None:
        try:
            eb_async_analysis_similarity.labels(gateway_id=self._gw).observe(value)
        except Exception:
            pass

    def observe_async_analysis_items(self, count: int) -> None:
        try:
            eb_async_analysis_items.labels(gateway_id=self._gw).observe(count)
        except Exception:
            pass

    def inc_goal_create(self) -> None:
        if METRICS_AVAILABLE:
            eb_goal_create_total.labels(gateway_id=self._gw).inc()

    def inc_session_boundary(self, action: str) -> None:
        if METRICS_AVAILABLE:
            eb_session_boundary_total.labels(gateway_id=self._gw, action=action).inc()

    def inc_session_ttl_touch(self) -> None:
        try:
            eb_session_ttl_touch_total.labels(gateway_id=self._gw).inc()
        except Exception:
            pass

    def observe_session_ttl_touch_keys(self, count: int) -> None:
        try:
            eb_session_ttl_touch_keys.labels(gateway_id=self._gw).observe(count)
        except Exception:
            pass

    # Phase 7 metrics methods

    def inc_guard_check(self, outcome: str) -> None:
        if METRICS_AVAILABLE:
            eb_guard_checks_total.labels(gateway_id=self._gw, outcome=outcome).inc()

    def observe_guard_latency(self, duration: float) -> None:
        if METRICS_AVAILABLE:
            eb_guard_check_duration.labels(gateway_id=self._gw).observe(duration)

    def inc_guard_layer_triggered(self, layer: str) -> None:
        if METRICS_AVAILABLE:
            eb_guard_layer_triggers.labels(gateway_id=self._gw, layer=layer).inc()

    def inc_guard_near_miss(self) -> None:
        if METRICS_AVAILABLE:
            eb_guard_near_misses.labels(gateway_id=self._gw).inc()

    def inc_guard_reinjection(self) -> None:
        if METRICS_AVAILABLE:
            eb_guard_reinjections.labels(gateway_id=self._gw).inc()

    def inc_guard_llm_escalation(self) -> None:
        if METRICS_AVAILABLE:
            eb_guard_llm_escalations.labels(gateway_id=self._gw).inc()

    def inc_guard_near_miss_escalation(self) -> None:
        if METRICS_AVAILABLE:
            eb_guard_near_miss_escalations.labels(gateway_id=self._gw).inc()

    def inc_hitl_retry_exhausted(self) -> None:
        if METRICS_AVAILABLE:
            eb_hitl_retry_exhausted.labels(gateway_id=self._gw).inc()

    def inc_autonomy_classification(self, domain: str, level: str) -> None:
        if METRICS_AVAILABLE:
            eb_autonomy_classifications.labels(gateway_id=self._gw, domain=domain, level=level).inc()

    def inc_autonomy_domain_tier(self, tier: str) -> None:
        if METRICS_AVAILABLE:
            eb_autonomy_domain_tier.labels(gateway_id=self._gw, tier=tier).inc()

    def inc_autonomy_hard_stop(self, domain: str) -> None:
        if METRICS_AVAILABLE:
            eb_autonomy_hard_stops.labels(gateway_id=self._gw, domain=domain).inc()

    def inc_approval_requested(self, domain: str) -> None:
        if METRICS_AVAILABLE:
            eb_approval_requests.labels(gateway_id=self._gw, domain=domain).inc()

    def observe_guard_bm25_score(self, score: float) -> None:
        if METRICS_AVAILABLE:
            eb_guard_bm25_score.labels(gateway_id=self._gw).observe(score)

    def observe_guard_semantic_score(self, score: float) -> None:
        if METRICS_AVAILABLE:
            eb_guard_semantic_score.labels(gateway_id=self._gw).observe(score)

    def inc_guard_bm25_short_circuit(self) -> None:
        if METRICS_AVAILABLE:
            eb_guard_bm25_short_circuit.labels(gateway_id=self._gw).inc()

    def inc_verification_check(self, result: str) -> None:
        if METRICS_AVAILABLE:
            eb_verification_runs.labels(gateway_id=self._gw, result=result).inc()

    def inc_completion_gate_check(self, result: str) -> None:
        if METRICS_AVAILABLE:
            eb_completion_checks.labels(gateway_id=self._gw, result=result).inc()

    # --- Phase 8: Profile, authority, admin metrics ---

    def inc_profile_resolve(self, profile_name: str, has_org_override: bool) -> None:
        if METRICS_AVAILABLE:
            eb_profile_resolve_total.labels(gateway_id=self._gw, profile_name=profile_name, has_org_override=str(has_org_override)).inc()

    def observe_profile_resolve_duration(self, seconds: float) -> None:
        if METRICS_AVAILABLE:
            eb_profile_resolve_duration.labels(gateway_id=self._gw).observe(seconds)

    def inc_profile_cache(self, result: str) -> None:
        if METRICS_AVAILABLE:
            eb_profile_cache_total.labels(gateway_id=self._gw, result=result).inc()

    def inc_authority_check(self, action: str, result: str) -> None:
        if METRICS_AVAILABLE:
            eb_authority_checks_total.labels(gateway_id=self._gw, action=action, result=result).inc()

    def inc_admin_op(self, operation: str, status: str) -> None:
        if METRICS_AVAILABLE:
            eb_admin_ops_total.labels(gateway_id=self._gw, operation=operation, status=status).inc()

    def inc_org_team_edge(self, edge_type: str, operation: str) -> None:
        if METRICS_AVAILABLE:
            eb_org_team_edges_total.labels(gateway_id=self._gw, edge_type=edge_type, operation=operation).inc()

    def inc_goal_scope_filter(self, scope: str) -> None:
        if METRICS_AVAILABLE:
            eb_goal_scope_filter_total.labels(gateway_id=self._gw, scope=scope).inc()

    def observe_goal_scope_filter_duration(self, seconds: float) -> None:
        if METRICS_AVAILABLE:
            eb_goal_scope_filter_duration.labels(gateway_id=self._gw).observe(seconds)

    def inc_handle_resolution(self, result: str) -> None:
        if METRICS_AVAILABLE:
            eb_handle_resolution_total.labels(gateway_id=self._gw, result=result).inc()

    def inc_actor_merge(self, status: str) -> None:
        if METRICS_AVAILABLE:
            eb_actor_merge_total.labels(gateway_id=self._gw, status=status).inc()

    def set_bootstrap_mode(self, active: bool) -> None:
        if METRICS_AVAILABLE:
            eb_bootstrap_mode_active.labels(gateway_id=self._gw).set(1 if active else 0)

    # Phase 9 consolidation metrics

    def inc_consolidation_run(self, status: str) -> None:
        if METRICS_AVAILABLE:
            eb_consolidation_runs_total.labels(gateway_id=self._gw, status=status).inc()

    def observe_consolidation_duration(self, duration: float) -> None:
        if METRICS_AVAILABLE:
            eb_consolidation_duration.labels(gateway_id=self._gw).observe(duration)

    def observe_stage_duration(self, stage: str, duration: float) -> None:
        if METRICS_AVAILABLE:
            eb_consolidation_stage_duration.labels(gateway_id=self._gw, stage=stage).observe(duration)

    def inc_facts_processed(self, stage: str, count: int = 1) -> None:
        if METRICS_AVAILABLE:
            eb_consolidation_facts_processed.labels(gateway_id=self._gw, stage=stage).inc(count)

    def inc_facts_affected(self, stage: str, action: str, count: int = 1) -> None:
        if METRICS_AVAILABLE:
            eb_consolidation_facts_affected.labels(gateway_id=self._gw, stage=stage, action=action).inc(count)

    def inc_consolidation_llm(self, stage: str) -> None:
        if METRICS_AVAILABLE:
            eb_consolidation_llm_calls.labels(gateway_id=self._gw, stage=stage).inc()

    def inc_consolidation_suggestion(self, status: str) -> None:
        if METRICS_AVAILABLE:
            eb_consolidation_suggestions.labels(gateway_id=self._gw, status=status).inc()

    def inc_tuner_adjustment(self, dimension: str) -> None:
        if METRICS_AVAILABLE:
            eb_scoring_tuner_adjustments.labels(gateway_id=self._gw, dimension=dimension).inc()

    def observe_tuner_magnitude(self, dimension: str, magnitude: float) -> None:
        if METRICS_AVAILABLE:
            eb_scoring_tuner_magnitude.labels(gateway_id=self._gw, dimension=dimension).observe(magnitude)
