"""Tests for interface contracts — verifies all ABCs are properly defined."""
import abc
import inspect
import typing

from elephantbroker.runtime.interfaces import (
    actor_registry,
    artifact_store,
    compaction_engine,
    consolidation,
    context_assembler,
    evidence_engine,
    goal_manager,
    guard_engine,
    ingest_buffer,
    memory_store,
    procedure_engine,
    profile_registry,
    rerank,
    retrieval,
    scoring_tuner,
    stats,
    trace_ledger,
    working_set,
)

ALL_INTERFACE_MODULES = [
    actor_registry,
    goal_manager,
    memory_store,
    working_set,
    context_assembler,
    compaction_engine,
    procedure_engine,
    evidence_engine,
    guard_engine,
    artifact_store,
    retrieval,
    rerank,
    stats,
    consolidation,
    profile_registry,
    trace_ledger,
    scoring_tuner,
    ingest_buffer,
]

EXPECTED_METHODS = {
    "IActorRegistry": ["resolve_actor", "register_actor", "get_authority_chain", "get_relationships"],
    "IGoalManager": ["set_goal", "resolve_active_goals", "get_goal_hierarchy", "update_goal_status"],
    "IMemoryStoreFacade": ["store", "search", "promote_scope", "promote_class", "decay", "get_by_id", "update", "delete", "get_by_scope"],
    "IWorkingSetManager": ["build_working_set", "get_working_set"],
    "IContextAssembler": ["assemble", "build_system_overlay", "build_subagent_packet", "assemble_from_snapshot", "build_system_overlay_from_items", "build_subagent_packet_from_context"],
    "ICompactionEngine": ["compact", "get_compact_state", "merge_overlapping", "compact_with_context", "get_session_compact_state"],
    "IProcedureEngine": ["store_procedure", "activate", "check_step", "validate_completion", "get_active_execution_ids"],
    "IEvidenceAndVerificationEngine": [
        "record_claim", "attach_evidence", "verify",
        "get_verification_state", "get_claim_verification",
        "check_completion_requirements", "reject",
        "get_claims_for_procedure",
    ],
    "IRedLineGuardEngine": ["preflight_check", "reinject_constraints", "get_guard_history", "load_session_rules", "unload_session"],
    "IToolArtifactStore": ["store_artifact", "search_artifacts", "get_by_hash"],
    "IRetrievalOrchestrator": ["retrieve_candidates", "get_exact_hits", "get_semantic_hits"],
    "IRerankOrchestrator": ["rerank", "cheap_prune", "cross_encoder_rerank", "merge_duplicates", "dedup_safe"],
    "IStatsAndTelemetryEngine": ["record_injection", "record_use", "get_stats_by_profile"],
    "IConsolidationEngine": ["run_consolidation", "get_consolidation_report", "run_stage"],
    "IProfileRegistry": ["resolve_profile", "get_effective_policy", "get_scoring_weights"],
    "ITraceLedger": ["append_event", "query_trace", "get_evidence_chain"],
    "IScoringTuner": ["get_weights", "apply_feedback", "run_tuning_cycle"],
    "IIngestBuffer": [
        "add_messages", "flush", "force_flush", "check_timeout_flush",
        "load_recent_facts", "update_recent_facts", "scrub_fact_from_recent",
    ],
}


def _get_interface_classes():
    """Extract all ABC classes from interface modules."""
    classes = []
    for mod in ALL_INTERFACE_MODULES:
        for name, obj in inspect.getmembers(mod, inspect.isclass):
            if name.startswith("I") and issubclass(obj, abc.ABC) and obj is not abc.ABC:
                classes.append((name, obj))
    return classes


class TestInterfaceCompleteness:
    def test_all_18_interfaces_exist(self):
        classes = _get_interface_classes()
        assert len(classes) == 18

    def test_all_interfaces_are_abstract(self):
        for name, cls in _get_interface_classes():
            abstract_methods = set()
            for method_name, method in inspect.getmembers(cls, predicate=inspect.isfunction):
                if getattr(method, "__isabstractmethod__", False):
                    abstract_methods.add(method_name)
            assert len(abstract_methods) > 0, f"{name} has no abstract methods"

    def test_all_interfaces_have_typed_signatures(self):
        for name, cls in _get_interface_classes():
            for method_name, method in inspect.getmembers(cls, predicate=inspect.isfunction):
                if method_name.startswith("_"):
                    continue
                hints = typing.get_type_hints(method)
                assert "return" in hints, f"{name}.{method_name} missing return type"
                for param, hint in hints.items():
                    assert hint is not typing.Any, f"{name}.{method_name} param '{param}' uses Any"

    def test_interface_method_counts_match_spec(self):
        for name, cls in _get_interface_classes():
            if name not in EXPECTED_METHODS:
                continue
            public_methods = [
                m for m, _ in inspect.getmembers(cls, predicate=inspect.isfunction)
                if not m.startswith("_")
            ]
            expected = EXPECTED_METHODS[name]
            assert set(public_methods) == set(expected), (
                f"{name}: expected {expected}, got {public_methods}"
            )
