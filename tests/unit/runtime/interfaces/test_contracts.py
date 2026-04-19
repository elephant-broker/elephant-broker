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
    scrub_buffer,
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


class TestScrubBufferProtocol:
    """TODO-5-316: IScrubBuffer is a narrowed structural Protocol for
    facade.delete()'s recent-facts scrub — the only call site that needed
    IIngestBuffer's 7-method ABC for 1 method. These tests pin:
      1. IScrubBuffer is a runtime-checkable Protocol (not an ABC).
      2. Exposes exactly one method: scrub_fact_from_recent.
      3. Concrete IngestBuffer satisfies it structurally without inheriting.
      4. MemoryStoreFacade's constructor type-narrows to IScrubBuffer.
    """

    def test_iscrub_buffer_is_runtime_checkable_protocol(self):
        from elephantbroker.runtime.interfaces.scrub_buffer import IScrubBuffer

        assert getattr(IScrubBuffer, "_is_protocol", False), (
            "IScrubBuffer must be a typing.Protocol, not an ABC"
        )
        # @runtime_checkable enables isinstance() against duck-typed instances.
        # Verified directly: a minimal object satisfying the protocol passes
        # the isinstance check without inheriting from IScrubBuffer.

        class _Duck:
            async def scrub_fact_from_recent(self, session_key: str, fact_id: str) -> int:
                return 0

        assert isinstance(_Duck(), IScrubBuffer), (
            "IScrubBuffer must be @runtime_checkable so isinstance() accepts "
            "structural conformance"
        )

    def test_iscrub_buffer_exposes_only_scrub_method(self):
        from elephantbroker.runtime.interfaces.scrub_buffer import IScrubBuffer

        public = [
            m for m, _ in inspect.getmembers(IScrubBuffer, predicate=inspect.isfunction)
            if not m.startswith("_")
        ]
        assert public == ["scrub_fact_from_recent"], (
            f"IScrubBuffer must expose only scrub_fact_from_recent, got {public}"
        )

    def test_iscrub_buffer_method_signature_matches_iingestbuffer(self):
        from elephantbroker.runtime.interfaces.ingest_buffer import IIngestBuffer
        from elephantbroker.runtime.interfaces.scrub_buffer import IScrubBuffer

        proto_sig = inspect.signature(IScrubBuffer.scrub_fact_from_recent)
        abc_sig = inspect.signature(IIngestBuffer.scrub_fact_from_recent)
        assert proto_sig.parameters == abc_sig.parameters, (
            "IScrubBuffer.scrub_fact_from_recent must mirror IIngestBuffer's "
            "signature so IngestBuffer satisfies both contracts"
        )

    def test_concrete_ingest_buffer_satisfies_scrub_buffer(self):
        """Structural conformance check — IngestBuffer inherits IIngestBuffer
        (nominal) and must ALSO match IScrubBuffer (structural) so the
        facade's narrowed type parameter accepts the existing instance."""
        from elephantbroker.pipelines.turn_ingest.buffer import IngestBuffer
        from elephantbroker.runtime.interfaces.scrub_buffer import IScrubBuffer

        assert hasattr(IngestBuffer, "scrub_fact_from_recent")
        assert callable(IngestBuffer.scrub_fact_from_recent)
        assert issubclass(IngestBuffer, IScrubBuffer), (
            "IngestBuffer must structurally satisfy IScrubBuffer "
            "(runtime_checkable Protocol accepts duck-typing)"
        )

    def test_memory_facade_constructor_narrows_to_scrub_buffer(self):
        """TODO-5-316: facade constructor param is typed `IScrubBuffer | None`
        — pins the interface-segregation win at a static-audit surface so a
        future regression that re-widens it to IIngestBuffer is caught."""
        from elephantbroker.runtime.interfaces.scrub_buffer import IScrubBuffer
        from elephantbroker.runtime.memory.facade import MemoryStoreFacade

        hints = typing.get_type_hints(MemoryStoreFacade.__init__)
        assert "ingest_buffer" in hints
        ingest_hint = hints["ingest_buffer"]
        # `IScrubBuffer | None` is Union[IScrubBuffer, None] at runtime.
        assert IScrubBuffer in typing.get_args(ingest_hint), (
            f"ingest_buffer must be typed IScrubBuffer | None, got {ingest_hint}"
        )


class TestCascadeStatusLiteral:
    """TODO-5-410: cascade_cognee_data returns a Literal-typed status alias
    so mypy/pyright catch typo'd comparisons at the call sites. The thin
    wrappers (facade._cascade_cognee_data, canonicalize._cascade_superseded_data_id)
    must propagate the same alias — discarding it or widening to `str`
    defeats the static check."""

    def test_cascade_status_is_literal_of_five_values(self):
        from elephantbroker.runtime.memory.cascade_helper import CascadeStatus

        args = typing.get_args(CascadeStatus)
        assert set(args) == {
            "ok", "ok_idempotent", "failed",
            "skipped_no_dataset", "skipped_bad_data_id",
        }, f"CascadeStatus values drifted: {args}"

    def test_facade_cascade_wrapper_returns_cascade_status(self):
        from elephantbroker.runtime.memory.cascade_helper import CascadeStatus
        from elephantbroker.runtime.memory.facade import MemoryStoreFacade

        hints = typing.get_type_hints(MemoryStoreFacade._cascade_cognee_data)
        assert hints["return"] is CascadeStatus, (
            f"_cascade_cognee_data return must be CascadeStatus, got {hints['return']}"
        )

    def test_canonicalize_cascade_wrapper_returns_cascade_status(self):
        from elephantbroker.runtime.consolidation.stages.canonicalize import (
            CanonicalizationStage,
        )
        from elephantbroker.runtime.memory.cascade_helper import CascadeStatus

        # canonicalize.py imports CascadeStatus under TYPE_CHECKING (runtime
        # import avoided to keep the consolidation module's cascade-helper
        # coupling type-only). `get_type_hints` needs the symbol in localns
        # to resolve the forward reference.
        hints = typing.get_type_hints(
            CanonicalizationStage._cascade_superseded_data_id,
            localns={"CascadeStatus": CascadeStatus},
        )
        assert hints["return"] is CascadeStatus, (
            f"_cascade_superseded_data_id return must be CascadeStatus, "
            f"got {hints['return']}"
        )
