"""Human-readable descriptions for all TraceEventType values."""

TRACE_EVENT_DESCRIPTIONS: dict[str, str] = {
    "input_received": "User or agent message received for processing",
    "retrieval_performed": "Memory search executed (auto-recall or explicit)",
    "retrieval_source_result": "Individual retrieval source returned results (structural/keyword/semantic/graph/artifact)",
    "tool_invoked": "Agent invoked a tool (memory_search, memory_store, etc.)",
    "artifact_created": "Tool artifact stored (code, file, URL, etc.)",
    "claim_made": "Agent made a verifiable claim",
    "claim_verified": "Claim verification completed (accepted/rejected/pending)",
    "procedure_step_passed": "Procedure step completed successfully",
    "procedure_step_failed": "Procedure step failed validation",
    "guard_triggered": "Red-line guard triggered — action blocked or requires approval",
    "compaction_action": "Context compaction performed (rule-classify or LLM-summarize)",
    "subagent_spawned": "Child agent spawned from parent session",
    "subagent_ended": "Child agent completed and results returned to parent",
    "context_assembled": "Context window assembled with token budget",
    "scoring_completed": "Working set scoring completed — payload contains per-item dimensions",
    "fact_extracted": "Fact extracted from conversation by LLM pipeline",
    "fact_superseded": "Existing fact superseded by newer extraction",
    "memory_class_assigned": "Fact classified into memory class (EPISODIC/SEMANTIC/PROCEDURAL/POLICY)",
    "dedup_triggered": "Duplicate detection triggered during fact storage",
    "session_boundary": "Session ended — buffer flushed, goals persisted",
    "ingest_buffer_flush": "Ingest buffer flushed to pipeline",
    "gdpr_delete": "GDPR deletion performed on fact/actor data",
    "cognee_cognify_completed": "Cognee cognify pipeline completed (chunking, entity extraction, triplet embedding)",
    "degraded_operation": "Operation ran in degraded mode due to backend failure — check payload.error",
    # Phase 7
    "guard_passed": "Guard check passed — no constraint violation detected",
    "guard_near_miss": "Guard check passed but was close to threshold — logged for review",
    "constraint_reinjected": "Red-line constraint reinjected into system prompt",
    "procedure_completion_checked": "Procedure completion validation ran (all steps + proofs checked)",
    # Phase 6
    "bootstrap_completed": "Session bootstrap completed — context initialized",
    "after_turn_completed": (
        "After-turn processing completed (successful-use tracking, cleanup). "
        "Payload fields: turn_count (int), updated_count (int — items whose "
        "successful_use_count was incremented this turn), response_messages "
        "(int — count of assistant messages in the delta), total_messages "
        "(int — size of the full messages envelope received), boundary_source "
        "('plugin' | 'derived' | 'empty' — how the response-message boundary "
        "was resolved; 'derived' indicates plugin stopped emitting "
        "prePromptMessageCount and the tail-walker fallback ran), "
        "snapshot_available (bool), signals_summary (dict[item_id, "
        "scanner_method]). See docs/CONFIGURATION.md §3 for observability "
        "semantics."
    ),
    "token_usage_reported": "Token usage reported by agent (input/output tokens)",
    "context_window_reported": "Context window size reported by agent",
    "successful_use_tracked": "Fact successful-use tracking updated (S1 quote, S2 tool, S6 ignored)",
    "subagent_parent_mapped": "Parent-child session mapping created for subagent",
    # Phase 8
    "profile_resolved": "Profile resolved for session (base + named + org override)",
    "org_created": "Organization entity created in graph",
    "team_created": "Team entity created in graph",
    "member_added": "Actor added as member of organization or team",
    "member_removed": "Actor removed from organization or team membership",
    "actor_merged": "Two actor records merged into one",
    "authority_check_failed": "Authority rule check failed — insufficient privileges",
    "handle_resolved": "Platform-qualified handle resolved to actor",
    "persistent_goal_created": "Persistent goal created with scope (GLOBAL/ORGANIZATION/TEAM/ACTOR)",
    "bootstrap_org_created": "Organization bootstrapped during first-run initialization",
    # Phase 5 (session goal lifecycle)
    "session_goal_created": "Session goal created — new goal tracked for the session",
    "session_goal_updated": "Session goal updated (status/priority/target change)",
    "session_goal_blocker_added": "Session goal blocker recorded — reason why the goal is stalled",
    "session_goal_progress": "Session goal progress note — incremental progress recorded",
    # Phase 9
    "consolidation_started": "Consolidation (sleep) pipeline started for gateway",
    "consolidation_stage_completed": "Single consolidation stage completed (1 of 9)",
    "consolidation_completed": "Full consolidation pipeline completed (all 9 stages)",
}
