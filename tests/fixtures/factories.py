"""Factory functions for test data."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any

from elephantbroker.schemas.actor import ActorRef, ActorType
from elephantbroker.schemas.artifact import ToolArtifact
from elephantbroker.schemas.config import (
    AuditConfig,
    CompactionLLMConfig,
    ConflictDetectionConfig,
    ContextAssemblyConfig,
    EmbeddingCacheConfig,
    ElephantBrokerConfig,
    GoalInjectionConfig,
    GoalRefinementConfig,
    LLMConfig,
    ProcedureCandidateConfig,
    ScoringConfig,
    SuccessfulUseConfig,
    VerificationMultipliers,
)
from elephantbroker.schemas.evidence import (
    ClaimRecord,
    EvidenceRef,
    VerificationState,
    VerificationSummary,
)
from elephantbroker.schemas.guards import (
    ApprovalRequest,
    ApprovalStatus,
    AutonomyPolicy,
    GuardActionType,
    GuardCheckInput,
    GuardEvent,
    GuardOutcome,
    GuardResult,
    StaticRule,
    StaticRulePatternType,
    StructuralValidatorSpec,
)
from elephantbroker.schemas.fact import FactAssertion, FactCategory
from elephantbroker.schemas.goal import GoalState
from elephantbroker.schemas.pipeline import ArtifactInput, TurnInput
from elephantbroker.schemas.procedure import (
    ProcedureDefinition,
    ProcedureExecution,
    ProcedureStep,
)
from elephantbroker.schemas.profile import (
    ProfilePolicy,
    RetrievalPolicy,
)
from elephantbroker.schemas.trace import TraceEvent, TraceEventType
from elephantbroker.schemas.working_set import (
    ScoringContext,
    ScoringWeights,
    WorkingSetItem,
    WorkingSetSnapshot,
)


def make_actor_ref(**overrides: Any) -> ActorRef:
    defaults: dict[str, Any] = {"type": ActorType.WORKER_AGENT, "display_name": "test-actor"}
    return ActorRef(**(defaults | overrides))


def make_goal_state(**overrides: Any) -> GoalState:
    defaults: dict[str, Any] = {"title": "Test goal"}
    return GoalState(**(defaults | overrides))


def make_fact_assertion(**overrides: Any) -> FactAssertion:
    defaults: dict[str, Any] = {"text": "Test fact", "category": "general"}
    return FactAssertion(**(defaults | overrides))


def make_claim_record(**overrides: Any) -> ClaimRecord:
    defaults: dict[str, Any] = {"claim_text": "Test claim"}
    return ClaimRecord(**(defaults | overrides))


def make_evidence_ref(**overrides: Any) -> EvidenceRef:
    defaults: dict[str, Any] = {"type": "tool_output", "ref_value": "test-ref"}
    return EvidenceRef(**(defaults | overrides))


def make_verification_state(**overrides: Any) -> VerificationState:
    defaults: dict[str, Any] = {"claim_id": uuid.uuid4()}
    return VerificationState(**(defaults | overrides))


def make_verification_summary(**overrides: Any) -> VerificationSummary:
    return VerificationSummary(**overrides)


def make_procedure_definition(**overrides: Any) -> ProcedureDefinition:
    # #1146 RESOLVED (R2-P2.1): default to is_manual_only=True so tests
    # that don't care about activation_modes get a valid procedure without
    # every call site having to opt into the flag. Tests that specifically
    # want to exercise auto-triggered procedures pass activation_modes=...
    # AND override is_manual_only=False.
    defaults: dict[str, Any] = {"name": "Test procedure", "is_manual_only": True}
    return ProcedureDefinition(**(defaults | overrides))


def make_procedure_step(**overrides: Any) -> ProcedureStep:
    defaults: dict[str, Any] = {"order": 0, "instruction": "Do the thing"}
    return ProcedureStep(**(defaults | overrides))


def make_procedure_execution(**overrides: Any) -> ProcedureExecution:
    defaults: dict[str, Any] = {"procedure_id": uuid.uuid4()}
    return ProcedureExecution(**(defaults | overrides))


def make_trace_event(**overrides: Any) -> TraceEvent:
    defaults: dict[str, Any] = {"event_type": TraceEventType.INPUT_RECEIVED}
    return TraceEvent(**(defaults | overrides))


def make_working_set_item(**overrides: Any) -> WorkingSetItem:
    defaults: dict[str, Any] = {
        "id": str(uuid.uuid4()),
        "source_type": "fact",
        "source_id": uuid.uuid4(),
        "text": "Test item",
    }
    return WorkingSetItem(**(defaults | overrides))


def make_working_set_snapshot(**overrides: Any) -> WorkingSetSnapshot:
    defaults: dict[str, Any] = {"session_id": uuid.uuid4(), "token_budget": 4000}
    return WorkingSetSnapshot(**(defaults | overrides))


def make_scoring_weights(**overrides: Any) -> ScoringWeights:
    return ScoringWeights(**overrides)


def make_profile_policy(**overrides: Any) -> ProfilePolicy:
    defaults: dict[str, Any] = {"id": "test", "name": "Test Profile"}
    return ProfilePolicy(**(defaults | overrides))


def make_tool_artifact(**overrides: Any) -> ToolArtifact:
    defaults: dict[str, Any] = {"tool_name": "test-artifact", "content": "test"}
    return ToolArtifact(**(defaults | overrides))


def make_config(**overrides: Any) -> ElephantBrokerConfig:
    return ElephantBrokerConfig(**overrides)


def make_cognee_config(**overrides: Any):
    from elephantbroker.schemas.config import CogneeConfig
    return CogneeConfig(**overrides)


def make_fact_datapoint(**overrides: Any):
    from elephantbroker.runtime.adapters.cognee.datapoints import FactDataPoint
    defaults: dict[str, Any] = {"text": "Test fact", "category": "general", "eb_id": str(uuid.uuid4())}
    return FactDataPoint(**(defaults | overrides))


def make_actor_datapoint(**overrides: Any):
    from elephantbroker.runtime.adapters.cognee.datapoints import ActorDataPoint
    defaults: dict[str, Any] = {"display_name": "test-actor", "actor_type": "worker_agent", "eb_id": str(uuid.uuid4())}
    return ActorDataPoint(**(defaults | overrides))


def make_retrieval_policy(**overrides: Any) -> RetrievalPolicy:
    return RetrievalPolicy(**overrides)


def make_turn_input(**overrides: Any) -> TurnInput:
    defaults: dict[str, Any] = {"session_key": "agent:main:main"}
    return TurnInput(**(defaults | overrides))


def make_artifact_input(**overrides: Any) -> ArtifactInput:
    defaults: dict[str, Any] = {"tool_name": "test-tool", "tool_output": "test output"}
    return ArtifactInput(**(defaults | overrides))


def make_llm_config(**overrides: Any) -> LLMConfig:
    return LLMConfig(**overrides)


# --- Phase 5 factory additions ---


def make_retrieval_candidate(**overrides: Any):
    from elephantbroker.runtime.interfaces.retrieval import RetrievalCandidate
    fact = overrides.pop("fact", None) or make_fact_assertion()
    defaults: dict[str, Any] = {"fact": fact, "source": "structural", "score": 0.8}
    return RetrievalCandidate(**(defaults | overrides))


def make_scoring_context(**overrides: Any) -> ScoringContext:
    defaults: dict[str, Any] = {
        "turn_text": "test query",
        "turn_embedding": [0.1] * 10,
        "token_budget": 8000,
    }
    return ScoringContext(**(defaults | overrides))


def make_scoring_config(**overrides: Any) -> ScoringConfig:
    return ScoringConfig(**overrides)


def make_verification_multipliers(**overrides: Any) -> VerificationMultipliers:
    return VerificationMultipliers(**overrides)


def make_conflict_detection_config(**overrides: Any) -> ConflictDetectionConfig:
    return ConflictDetectionConfig(**overrides)


def make_embedding_cache_config(**overrides: Any) -> EmbeddingCacheConfig:
    return EmbeddingCacheConfig(**overrides)


def make_goal_injection_config(**overrides: Any) -> GoalInjectionConfig:
    return GoalInjectionConfig(**overrides)


def make_goal_refinement_config(**overrides: Any) -> GoalRefinementConfig:
    return GoalRefinementConfig(**overrides)


def make_procedure_candidate_config(**overrides: Any) -> ProcedureCandidateConfig:
    return ProcedureCandidateConfig(**overrides)


def make_audit_config(**overrides: Any) -> AuditConfig:
    return AuditConfig(**overrides)


# --- Phase 6 factory additions ---


def make_bootstrap_params(**overrides: Any):
    from elephantbroker.schemas.context import BootstrapParams
    defaults: dict[str, Any] = {"session_key": "agent:main:main", "session_id": str(uuid.uuid4())}
    return BootstrapParams(**(defaults | overrides))


def make_ingest_batch_params(**overrides: Any):
    from elephantbroker.schemas.context import AgentMessage, IngestBatchParams
    defaults: dict[str, Any] = {
        "session_id": str(uuid.uuid4()),
        "session_key": "agent:main:main",
        "messages": [AgentMessage(role="user", content="hello")],
    }
    return IngestBatchParams(**(defaults | overrides))


def make_assemble_params(**overrides: Any):
    from elephantbroker.schemas.context import AssembleParams
    defaults: dict[str, Any] = {
        "session_id": str(uuid.uuid4()),
        "session_key": "agent:main:main",
    }
    return AssembleParams(**(defaults | overrides))


def make_compact_params(**overrides: Any):
    from elephantbroker.schemas.context import CompactParams
    defaults: dict[str, Any] = {
        "session_id": str(uuid.uuid4()),
        "session_key": "agent:main:main",
    }
    return CompactParams(**(defaults | overrides))


def make_after_turn_params(**overrides: Any):
    from elephantbroker.schemas.context import AfterTurnParams
    defaults: dict[str, Any] = {
        "session_id": str(uuid.uuid4()),
        "session_key": "agent:main:main",
    }
    return AfterTurnParams(**(defaults | overrides))


def make_session_context(**overrides: Any):
    from elephantbroker.schemas.context import SessionContext
    defaults: dict[str, Any] = {
        "session_key": "agent:main:main",
        "session_id": str(uuid.uuid4()),
        "profile_name": "coding",
        "profile": make_profile_policy(),
    }
    return SessionContext(**(defaults | overrides))


def make_session_artifact(**overrides: Any):
    from elephantbroker.schemas.artifact import SessionArtifact
    defaults: dict[str, Any] = {"tool_name": "test-tool", "content": "test content"}
    return SessionArtifact(**(defaults | overrides))


def make_compaction_context(**overrides: Any):
    from elephantbroker.schemas.context import CompactionContext
    defaults: dict[str, Any] = {
        "session_key": "agent:main:main",
        "session_id": str(uuid.uuid4()),
        "messages": [],
    }
    return CompactionContext(**(defaults | overrides))


def make_assembly_placement_policy(**overrides: Any):
    from elephantbroker.schemas.profile import AssemblyPlacementPolicy
    return AssemblyPlacementPolicy(**overrides)


def make_context_assembly_config(**overrides: Any) -> ContextAssemblyConfig:
    return ContextAssemblyConfig(**overrides)


def make_compaction_llm_config(**overrides: Any) -> CompactionLLMConfig:
    return CompactionLLMConfig(**overrides)


# --- Phase 7 factory additions ---


def make_guard_check_input(**overrides: Any) -> GuardCheckInput:
    defaults: dict[str, Any] = {
        "session_id": uuid.uuid4(),
        "action_type": GuardActionType.MESSAGE_SEND,
        "action_content": "test message",
    }
    return GuardCheckInput(**(defaults | overrides))


def make_guard_result(**overrides: Any) -> GuardResult:
    defaults: dict[str, Any] = {"outcome": GuardOutcome.PASS}
    return GuardResult(**(defaults | overrides))


def make_guard_event(**overrides: Any) -> GuardEvent:
    defaults: dict[str, Any] = {
        "session_id": uuid.uuid4(),
        "input_summary": "test",
        "outcome": GuardOutcome.PASS,
    }
    return GuardEvent(**(defaults | overrides))


def make_static_rule(**overrides: Any) -> StaticRule:
    defaults: dict[str, Any] = {
        "id": "test_rule",
        "pattern_type": StaticRulePatternType.KEYWORD,
        "pattern": "test",
    }
    return StaticRule(**(defaults | overrides))


def make_structural_validator_spec(**overrides: Any) -> StructuralValidatorSpec:
    defaults: dict[str, Any] = {
        "id": "test_val",
        "action_type": GuardActionType.TOOL_CALL,
    }
    return StructuralValidatorSpec(**(defaults | overrides))


def make_approval_request(**overrides: Any) -> ApprovalRequest:
    defaults: dict[str, Any] = {
        "guard_event_id": uuid.uuid4(),
        "session_id": uuid.uuid4(),
        "action_summary": "test",
        "decision_domain": "general",
    }
    return ApprovalRequest(**(defaults | overrides))


def make_autonomy_policy(**overrides: Any) -> AutonomyPolicy:
    return AutonomyPolicy(**overrides)


# --- Phase 9: Consolidation factories ---


def make_consolidation_config(**overrides: Any):
    from elephantbroker.schemas.consolidation import ConsolidationConfig
    return ConsolidationConfig(**overrides)


def make_consolidation_report(**overrides: Any):
    from elephantbroker.schemas.consolidation import ConsolidationReport
    defaults: dict[str, Any] = {"org_id": "test-org", "gateway_id": "test-gw"}
    return ConsolidationReport(**(defaults | overrides))


def make_stage_result(**overrides: Any):
    from elephantbroker.schemas.consolidation import StageResult
    defaults: dict[str, Any] = {"stage": 1, "name": "test_stage"}
    return StageResult(**(defaults | overrides))


def make_duplicate_cluster(**overrides: Any):
    from elephantbroker.schemas.consolidation import DuplicateCluster
    defaults: dict[str, Any] = {
        "fact_ids": [str(uuid.uuid4())],
        "canonical_candidate_id": str(uuid.uuid4()),
        "avg_similarity": 0.95,
        "session_keys": ["s1"],
    }
    return DuplicateCluster(**(defaults | overrides))


def make_domain_suggestion(**overrides: Any):
    from elephantbroker.schemas.consolidation import DomainSuggestion
    defaults: dict[str, Any] = {
        "action_target": "test_tool",
        "suggested_domain": "general",
        "occurrences": 5,
        "similarity_to_existing": 0.8,
    }
    return DomainSuggestion(**(defaults | overrides))


def make_procedure_suggestion(**overrides: Any):
    from elephantbroker.schemas.procedure import ProcedureSuggestion
    defaults: dict[str, Any] = {
        "pattern_description": "test pattern",
        "tool_sequence": ["tool_a", "tool_b"],
        "sessions_observed": 3,
    }
    return ProcedureSuggestion(**(defaults | overrides))


def make_verification_gap(**overrides: Any):
    from elephantbroker.schemas.evidence import VerificationGap
    defaults: dict[str, Any] = {
        "claim_id": uuid.uuid4(),
        "claim_text": "test claim",
        "missing_proof_type": "diff_hash",
        "missing_proof_description": "missing hash proof",
    }
    return VerificationGap(**(defaults | overrides))
