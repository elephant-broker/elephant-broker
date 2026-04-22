"""Profile preset definitions — BASE + 5 named profiles from arch spec §10.2.

Extracted from the Phase 3 inline ``_PRESETS`` dict in ``registry.py``.
Each named profile sets ``extends="base"`` for the inheritance engine.
All values are the canonical spec values verified against §10.2.
"""
from __future__ import annotations

from elephantbroker.schemas.guards import AutonomyLevel, AutonomyPolicy
from elephantbroker.schemas.profile import (
    AssemblyPlacementPolicy,
    AutorecallPolicy,
    Budgets,
    CompactionPolicy,
    GraphMode,
    GuardPolicy,
    IsolationLevel,
    IsolationScope,
    ProfilePolicy,
    RetrievalPolicy,
    VerificationPolicy,
)
from elephantbroker.schemas.working_set import ScoringWeights

# ---------------------------------------------------------------------------
# BASE_PROFILE — root of inheritance chain, all Pydantic defaults
# ---------------------------------------------------------------------------

BASE_PROFILE = ProfilePolicy(
    id="base",
    name="Base Profile",
    extends=None,
    graph_mode=GraphMode.HYBRID,
    budgets=Budgets(),
    scoring_weights=ScoringWeights(),
    compaction=CompactionPolicy(),
    autorecall=AutorecallPolicy(),
    retrieval=RetrievalPolicy(),
    verification=VerificationPolicy(),
    guards=GuardPolicy(),
    session_data_ttl_seconds=86400,
    assembly_placement=AssemblyPlacementPolicy(),
)

# ---------------------------------------------------------------------------
# CODING — local graph, aggressive compaction, high turn/recency relevance
# ---------------------------------------------------------------------------

CODING_PROFILE = ProfilePolicy(
    id="coding",
    name="Coding",
    extends="base",
    graph_mode=GraphMode.LOCAL,
    scoring_weights=ScoringWeights(
        turn_relevance=1.5,
        session_goal_relevance=1.2,
        global_goal_relevance=0.3,
        recency=1.2,
        successful_use_prior=0.8,
        confidence=0.3,
        evidence_strength=0.2,
        novelty=0.6,
        redundancy_penalty=-0.8,
        contradiction_penalty=-1.0,
        cost_penalty=-0.4,
        recency_half_life_hours=24.0,
        evidence_refs_for_max_score=2,
        redundancy_similarity_threshold=0.85,
        contradiction_similarity_threshold=0.9,
        contradiction_confidence_gap=0.3,
    ),
    budgets=Budgets(max_prompt_tokens=8000),
    compaction=CompactionPolicy(cadence="aggressive"),
    retrieval=RetrievalPolicy(
        structural_weight=0.5,
        keyword_weight=0.4,
        vector_weight=0.3,
        graph_expansion_enabled=True,
        graph_expansion_weight=0.1,
        graph_mode=GraphMode.LOCAL,
        graph_max_depth=1,
        isolation_level=IsolationLevel.LOOSE,
        isolation_scope=IsolationScope.SESSION_KEY,
    ),
    autorecall=AutorecallPolicy(
        retrieval=RetrievalPolicy(
            structural_weight=0.7,
            keyword_weight=0.3,
            vector_enabled=False,
            graph_expansion_enabled=False,
            root_top_k=15,
        ),
        extraction_focus=[
            "code decisions", "architecture choices", "technical preferences", "tool configs", "error patterns",
        ],
        custom_categories=["code_decision", "architecture", "debugging", "tooling"],
        superseded_confidence_factor=0.1,
    ),
    session_data_ttl_seconds=86400,
    guards=GuardPolicy(
        preflight_check_strictness="medium",
        autonomy=AutonomyPolicy(
            default_level=AutonomyLevel.INFORM,
            domain_levels={
                "financial": AutonomyLevel.HARD_STOP,
                "data_access": AutonomyLevel.APPROVE_FIRST,
                "communication": AutonomyLevel.INFORM,
                "code_change": AutonomyLevel.AUTONOMOUS,
                "scope_change": AutonomyLevel.INFORM,
                "resource": AutonomyLevel.AUTONOMOUS,
                "info_share": AutonomyLevel.INFORM,
                "delegation": AutonomyLevel.AUTONOMOUS,
                "record_mutation": AutonomyLevel.AUTONOMOUS,
            },
        ),
    ),
    assembly_placement=AssemblyPlacementPolicy(
        goal_injection_cadence="smart", goal_reminder_interval=5,
        keep_last_n_tool_outputs=2, replace_tool_outputs=True,
    ),
)

# ---------------------------------------------------------------------------
# RESEARCH — global graph, minimal compaction, high evidence/confidence
# ---------------------------------------------------------------------------

RESEARCH_PROFILE = ProfilePolicy(
    id="research",
    name="Research",
    extends="base",
    graph_mode=GraphMode.HYBRID,
    scoring_weights=ScoringWeights(
        turn_relevance=0.8,
        session_goal_relevance=1.0,
        global_goal_relevance=0.8,
        recency=0.5,
        successful_use_prior=0.6,
        confidence=0.8,
        evidence_strength=0.9,
        novelty=0.7,
        redundancy_penalty=-0.5,
        contradiction_penalty=-1.0,
        cost_penalty=-0.2,
        recency_half_life_hours=168.0,
        evidence_refs_for_max_score=5,
        redundancy_similarity_threshold=0.80,
        contradiction_similarity_threshold=0.85,
        contradiction_confidence_gap=0.25,
    ),
    budgets=Budgets(max_prompt_tokens=12000),
    compaction=CompactionPolicy(cadence="minimal"),
    retrieval=RetrievalPolicy(
        structural_weight=0.3,
        keyword_weight=0.2,
        vector_weight=0.5,
        graph_expansion_weight=0.3,
        graph_mode=GraphMode.GLOBAL,
        graph_max_depth=3,
        isolation_level=IsolationLevel.NONE,
        isolation_scope=IsolationScope.GLOBAL,
        structural_fetch_k=15,
        vector_fetch_k=25,
        artifact_fetch_k=15,
    ),
    autorecall=AutorecallPolicy(
        retrieval=RetrievalPolicy(
            structural_weight=0.3,
            keyword_weight=0.2,
            vector_weight=0.5,
            graph_expansion_weight=0.3,
            graph_mode=GraphMode.GLOBAL,
            graph_max_depth=3,
            isolation_level=IsolationLevel.NONE,
            isolation_scope=IsolationScope.GLOBAL,
            structural_fetch_k=15,
            vector_fetch_k=25,
            artifact_fetch_k=15,
        ),
        extraction_focus=["findings", "hypotheses", "methodology", "data sources", "citations"],
        custom_categories=["hypothesis", "finding", "methodology", "citation"],
        superseded_confidence_factor=0.5,
    ),
    session_data_ttl_seconds=259200,
    guards=GuardPolicy(
        preflight_check_strictness="loose",
        autonomy=AutonomyPolicy(
            default_level=AutonomyLevel.INFORM,
            domain_levels={
                "financial": AutonomyLevel.APPROVE_FIRST,
                "data_access": AutonomyLevel.APPROVE_FIRST,
                "communication": AutonomyLevel.INFORM,
                "code_change": AutonomyLevel.INFORM,
                "scope_change": AutonomyLevel.INFORM,
                "resource": AutonomyLevel.AUTONOMOUS,
                "info_share": AutonomyLevel.INFORM,
                "delegation": AutonomyLevel.INFORM,
                "record_mutation": AutonomyLevel.INFORM,
            },
        ),
    ),
    assembly_placement=AssemblyPlacementPolicy(
        goal_injection_cadence="smart", goal_reminder_interval=10,
        keep_last_n_tool_outputs=0, replace_tool_outputs=False,
    ),
    # T-2: successful_use_thresholds intentionally left unset — Option C reset
    # means all 5 presets inherit module defaults (0.15/0.3/0.15/0.15/3).
    # Per-profile differentiation was removed after Q-2 live verification
    # showed speculative overrides blocked realistic signal strengths.
)

# ---------------------------------------------------------------------------
# MANAGERIAL — goal-focused, strict guards, always-inject goals
# ---------------------------------------------------------------------------

MANAGERIAL_PROFILE = ProfilePolicy(
    id="managerial",
    name="Managerial",
    extends="base",
    graph_mode=GraphMode.HYBRID,
    scoring_weights=ScoringWeights(
        turn_relevance=0.7,
        session_goal_relevance=1.5,
        global_goal_relevance=1.0,
        recency=0.6,
        successful_use_prior=0.5,
        confidence=0.5,
        evidence_strength=0.7,
        novelty=0.4,
        redundancy_penalty=-0.9,
        contradiction_penalty=-1.0,
        cost_penalty=-0.5,
        recency_half_life_hours=72.0,
        evidence_refs_for_max_score=3,
        redundancy_similarity_threshold=0.90,
        contradiction_similarity_threshold=0.9,
        contradiction_confidence_gap=0.3,
    ),
    budgets=Budgets(max_prompt_tokens=8000),
    compaction=CompactionPolicy(cadence="aggressive"),
    retrieval=RetrievalPolicy(
        keyword_weight=0.2,
        vector_weight=0.3,
        graph_expansion_weight=0.4,
        graph_mode=GraphMode.HYBRID,
        graph_max_depth=2,
        isolation_level=IsolationLevel.LOOSE,
    ),
    autorecall=AutorecallPolicy(
        retrieval=RetrievalPolicy(
            structural_weight=0.5,
            graph_expansion_weight=0.3,
            keyword_enabled=False,
            root_top_k=20,
        ),
        extraction_focus=["decisions", "delegations", "deadlines", "blockers", "team dynamics"],
        custom_categories=["delegation", "deadline", "blocker", "team_dynamic"],
        superseded_confidence_factor=0.3,
    ),
    session_data_ttl_seconds=172800,
    guards=GuardPolicy(
        preflight_check_strictness="strict",
        autonomy=AutonomyPolicy(
            default_level=AutonomyLevel.APPROVE_FIRST,
            domain_levels={
                "financial": AutonomyLevel.APPROVE_FIRST,
                "data_access": AutonomyLevel.APPROVE_FIRST,
                "communication": AutonomyLevel.APPROVE_FIRST,
                "code_change": AutonomyLevel.HARD_STOP,
                "scope_change": AutonomyLevel.AUTONOMOUS,
                "resource": AutonomyLevel.INFORM,
                "info_share": AutonomyLevel.APPROVE_FIRST,
                "delegation": AutonomyLevel.AUTONOMOUS,
                "record_mutation": AutonomyLevel.INFORM,
            },
        ),
    ),
    assembly_placement=AssemblyPlacementPolicy(
        goal_injection_cadence="always",
        keep_last_n_tool_outputs=1, replace_tool_outputs=True,
    ),
    # T-2: successful_use_thresholds intentionally left unset — Option C reset
    # means all 5 presets inherit module defaults (0.15/0.3/0.15/0.15/3).
    # Per-profile differentiation was removed after Q-2 live verification
    # showed speculative overrides blocked realistic signal strengths.
)

# ---------------------------------------------------------------------------
# WORKER — task-focused, short recency half-life, local graph
# ---------------------------------------------------------------------------

WORKER_PROFILE = ProfilePolicy(
    id="worker",
    name="Worker",
    extends="base",
    graph_mode=GraphMode.LOCAL,
    scoring_weights=ScoringWeights(
        turn_relevance=1.3,
        session_goal_relevance=1.4,
        global_goal_relevance=0.6,
        recency=1.3,
        successful_use_prior=0.7,
        confidence=0.4,
        evidence_strength=0.3,
        novelty=0.5,
        redundancy_penalty=-0.7,
        contradiction_penalty=-1.0,
        cost_penalty=-0.4,
        recency_half_life_hours=12.0,
        evidence_refs_for_max_score=2,
        redundancy_similarity_threshold=0.85,
        contradiction_similarity_threshold=0.9,
        contradiction_confidence_gap=0.3,
    ),
    budgets=Budgets(max_prompt_tokens=6000),
    compaction=CompactionPolicy(cadence="balanced"),
    retrieval=RetrievalPolicy(
        structural_weight=0.5,
        keyword_weight=0.4,
        vector_weight=0.3,
        graph_expansion_weight=0.1,
        graph_mode=GraphMode.LOCAL,
        graph_max_depth=1,
        isolation_level=IsolationLevel.LOOSE,
    ),
    autorecall=AutorecallPolicy(
        retrieval=RetrievalPolicy(
            structural_weight=0.7,
            keyword_weight=0.3,
            vector_enabled=False,
            graph_expansion_enabled=False,
            root_top_k=15,
        ),
        extraction_focus=["task instructions", "tool outputs", "progress updates", "blockers"],
        superseded_confidence_factor=0.2,
    ),
    session_data_ttl_seconds=86400,
    guards=GuardPolicy(
        preflight_check_strictness="medium",
        autonomy=AutonomyPolicy(
            default_level=AutonomyLevel.INFORM,
            domain_levels={
                "financial": AutonomyLevel.HARD_STOP,
                "data_access": AutonomyLevel.APPROVE_FIRST,
                "communication": AutonomyLevel.INFORM,
                "code_change": AutonomyLevel.AUTONOMOUS,
                "scope_change": AutonomyLevel.APPROVE_FIRST,
                "resource": AutonomyLevel.AUTONOMOUS,
                "info_share": AutonomyLevel.INFORM,
                "delegation": AutonomyLevel.INFORM,
                "record_mutation": AutonomyLevel.AUTONOMOUS,
            },
        ),
    ),
    assembly_placement=AssemblyPlacementPolicy(
        goal_injection_cadence="smart", goal_reminder_interval=3,
        keep_last_n_tool_outputs=1, replace_tool_outputs=True,
    ),
)

# ---------------------------------------------------------------------------
# PERSONAL_ASSISTANT — long TTL, strict isolation, high use-prior
# ---------------------------------------------------------------------------

PERSONAL_ASSISTANT_PROFILE = ProfilePolicy(
    id="personal_assistant",
    name="Personal Assistant",
    extends="base",
    graph_mode=GraphMode.HYBRID,
    scoring_weights=ScoringWeights(
        turn_relevance=1.0,
        session_goal_relevance=0.8,
        global_goal_relevance=0.4,
        recency=0.9,
        successful_use_prior=0.9,
        confidence=0.3,
        evidence_strength=0.2,
        novelty=0.5,
        redundancy_penalty=-0.6,
        contradiction_penalty=-1.0,
        cost_penalty=-0.3,
        recency_half_life_hours=720.0,
        evidence_refs_for_max_score=3,
        redundancy_similarity_threshold=0.85,
        contradiction_similarity_threshold=0.9,
        contradiction_confidence_gap=0.35,
    ),
    budgets=Budgets(max_prompt_tokens=8000),
    compaction=CompactionPolicy(cadence="balanced"),
    retrieval=RetrievalPolicy(
        structural_weight=0.3,
        keyword_weight=0.3,
        vector_weight=0.5,
        graph_expansion_weight=0.2,
        graph_mode=GraphMode.HYBRID,
        graph_max_depth=2,
        structural_fetch_k=15,
        isolation_level=IsolationLevel.STRICT,
    ),
    autorecall=AutorecallPolicy(
        retrieval=RetrievalPolicy(
            vector_weight=0.6,
            structural_weight=0.4,
            keyword_enabled=False,
            graph_expansion_enabled=False,
            root_top_k=15,
        ),
        extraction_focus=["preferences", "habits", "schedules", "relationships", "reminders"],
        superseded_confidence_factor=0.4,
    ),
    session_data_ttl_seconds=604800,
    guards=GuardPolicy(
        preflight_check_strictness="strict",
        autonomy=AutonomyPolicy(
            default_level=AutonomyLevel.INFORM,
            domain_levels={
                "financial": AutonomyLevel.HARD_STOP,
                "data_access": AutonomyLevel.HARD_STOP,
                "communication": AutonomyLevel.APPROVE_FIRST,
                "code_change": AutonomyLevel.APPROVE_FIRST,
                "scope_change": AutonomyLevel.INFORM,
                "resource": AutonomyLevel.INFORM,
                "info_share": AutonomyLevel.APPROVE_FIRST,
                "delegation": AutonomyLevel.INFORM,
                "record_mutation": AutonomyLevel.INFORM,
            },
        ),
    ),
    assembly_placement=AssemblyPlacementPolicy(
        goal_injection_cadence="smart", goal_reminder_interval=8,
        keep_last_n_tool_outputs=1, replace_tool_outputs=True,
        system_context_blockers=False,
    ),
    # T-2: successful_use_thresholds intentionally left unset — Option C reset
    # means all 5 presets inherit module defaults (0.15/0.3/0.15/0.15/3).
    # Per-profile differentiation was removed after Q-2 live verification
    # showed speculative overrides blocked realistic signal strengths.
)

# ---------------------------------------------------------------------------
# Exported preset registry
# ---------------------------------------------------------------------------

PROFILE_PRESETS: dict[str, ProfilePolicy] = {
    "base": BASE_PROFILE,
    "coding": CODING_PROFILE,
    "research": RESEARCH_PROFILE,
    "managerial": MANAGERIAL_PROFILE,
    "worker": WORKER_PROFILE,
    "personal_assistant": PERSONAL_ASSISTANT_PROFILE,
}
