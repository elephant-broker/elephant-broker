"""Tests for Phase 6 context schema additions."""
from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from elephantbroker.schemas.artifact import (
    CreateArtifactRequest,
    SessionArtifact,
    SessionArtifactSearchRequest,
)
from elephantbroker.schemas.config import (
    ArtifactAssemblyConfig,
    ArtifactCaptureConfig,
    AsyncAnalysisConfig,
    CompactionLLMConfig,
    ContextAssemblyConfig,
    ElephantBrokerConfig,
)
from elephantbroker.schemas.context import (
    AfterTurnParams,
    AgentMessage,
    AssembleParams,
    BootstrapParams,
    BuildOverlayRequest,
    CompactParams,
    CompactionContext,
    ContextWindowReport,
    IngestBatchParams,
    IngestParams,
    SessionCompactState,
    SessionContext,
    SubagentEndedParams,
    SubagentRollbackRequest,
    SubagentSpawnParams,
    SubagentSpawnResult,
    TokenUsageReport,
)
from elephantbroker.schemas.profile import AssemblyPlacementPolicy, ProfilePolicy
from elephantbroker.schemas.trace import TraceEventType
from tests.fixtures.factories import make_profile_policy


class TestBootstrapParams:
    def test_defaults(self):
        p = BootstrapParams(session_key="sk", session_id="sid")
        assert p.profile_name == "coding"
        assert p.is_subagent is False
        assert p.parent_session_key is None

    def test_required_fields(self):
        with pytest.raises(ValidationError):
            BootstrapParams(session_id="sid")  # missing session_key

    def test_subagent_fields(self):
        p = BootstrapParams(session_key="child", session_id="sid", is_subagent=True, parent_session_key="parent")
        assert p.is_subagent is True
        assert p.parent_session_key == "parent"


class TestIngestParams:
    def test_defaults(self):
        msg = AgentMessage(role="user", content="hello")
        p = IngestParams(session_id="sid", session_key="sk", message=msg)
        assert p.is_heartbeat is False


class TestIngestBatchParams:
    def test_defaults(self):
        msgs = [AgentMessage(role="user", content="hi")]
        p = IngestBatchParams(session_id="sid", session_key="sk", messages=msgs)
        assert p.profile_name == "coding"
        assert len(p.messages) == 1


class TestAssembleParams:
    def test_defaults(self):
        p = AssembleParams(session_id="sid", session_key="sk")
        assert p.token_budget is None
        assert p.query == ""
        assert p.messages == []


class TestCompactParams:
    def test_defaults(self):
        p = CompactParams(session_id="sid", session_key="sk")
        assert p.force is False
        assert p.compaction_target is None

    def test_with_target(self):
        p = CompactParams(session_id="sid", session_key="sk", compaction_target="budget", force=True)
        assert p.compaction_target == "budget"


class TestAfterTurnParams:
    def test_defaults(self):
        p = AfterTurnParams(session_id="sid", session_key="sk")
        # P4: None = plugin silent; see AfterTurnParams comment. The runtime
        # falls back to the tail-walker when this field is absent.
        assert p.pre_prompt_message_count is None
        assert p.is_heartbeat is False


class TestSubagentSpawnParams:
    def test_create(self):
        p = SubagentSpawnParams(parent_session_key="p", child_session_key="c")
        assert p.ttl_ms is None


class TestSubagentEndedParams:
    def test_defaults(self):
        p = SubagentEndedParams(child_session_key="c")
        assert p.reason == "completed"

    def test_ignores_unknown_parent_session_key(self):
        """TF-06-007 V4: SubagentEndedParams has NO `parent_session_key` field.
        Unknown fields are silently ignored (Pydantic v2 default `extra='ignore'`).
        Confirms the schema does not require, accept, or expose the parent key —
        it lives only in Redis under the child's `session_parent` mapping."""
        # 1. Field is not declared on the model
        assert "parent_session_key" not in SubagentEndedParams.model_fields
        # 2. Construction with an extraneous parent_session_key still succeeds…
        p = SubagentEndedParams(
            child_session_key="c",
            reason="completed",
            parent_session_key="agent:parent:main",  # type: ignore[call-arg]
        )
        # …but the value is dropped — not stored, not exposed in dump.
        assert not hasattr(p, "parent_session_key")
        assert "parent_session_key" not in p.model_dump()


class TestSubagentSpawnResult:
    def test_defaults(self):
        r = SubagentSpawnResult(parent_session_key="p", child_session_key="c")
        assert r.parent_mapping_stored is False


class TestSessionContext:
    def test_defaults(self):
        profile = make_profile_policy()
        sc = SessionContext(session_key="sk", session_id="sid", profile_name="coding", profile=profile)
        assert sc.org_id == ""
        assert sc.team_ids == []
        assert sc.turn_count == 0
        assert sc.compact_count == 0
        assert sc.fact_last_injection_turn == {}
        assert sc.goal_inject_history == {}
        assert sc.parent_session_key is None

    def test_json_roundtrip(self):
        profile = make_profile_policy()
        sc = SessionContext(session_key="sk", session_id="sid", profile_name="coding", profile=profile)
        json_str = sc.model_dump_json()
        sc2 = SessionContext.model_validate_json(json_str)
        assert sc2.session_key == "sk"
        assert sc2.profile.id == "test"
        assert sc2.org_id == ""

    def test_org_team_forward_compat(self):
        profile = make_profile_policy()
        sc = SessionContext(session_key="sk", session_id="sid", profile_name="coding", profile=profile, org_id="org1", team_ids=["team1"])
        assert sc.org_id == "org1"
        assert sc.team_ids == ["team1"]


class TestSessionCompactState:
    def test_defaults(self):
        s = SessionCompactState(session_key="sk", session_id="sid")
        assert s.goal_summary == ""
        assert s.decisions_made == []
        assert s.token_count == 0


class TestCompactionContext:
    def test_with_goals(self):
        from elephantbroker.schemas.goal import GoalState
        goals = [GoalState(title="Goal 1")]
        cc = CompactionContext(session_key="sk", session_id="sid", messages=[], current_goals=goals)
        assert len(cc.current_goals) == 1


class TestContextWindowReport:
    def test_min_tokens(self):
        with pytest.raises(ValidationError):
            ContextWindowReport(session_key="sk", session_id="sid", provider="p", model="m", context_window_tokens=999)

    def test_valid(self):
        r = ContextWindowReport(session_key="sk", session_id="sid", provider="openai", model="gpt-4", context_window_tokens=128000)
        assert r.context_window_tokens == 128000


class TestTokenUsageReport:
    def test_defaults(self):
        r = TokenUsageReport(session_key="sk", session_id="sid", input_tokens=100, output_tokens=50, total_tokens=150)
        assert r.cache_read_tokens == 0


class TestBuildOverlayRequest:
    def test_create(self):
        r = BuildOverlayRequest(session_key="sk", session_id="sid")
        assert r.session_key == "sk"


class TestSubagentRollbackRequest:
    def test_create(self):
        r = SubagentRollbackRequest(parent_session_key="p", child_session_key="c", rollback_key="k")
        assert r.rollback_key == "k"


# --- Config models ---

class TestContextAssemblyConfig:
    def test_defaults(self):
        c = ContextAssemblyConfig()
        assert c.max_context_window_fraction == 0.15
        assert c.fallback_context_window == 128000
        assert c.compaction_trigger_multiplier == 2.0


class TestCompactionLLMConfig:
    def test_defaults(self):
        c = CompactionLLMConfig()
        assert c.model == "gemini/gemini-2.5-flash-lite"
        assert c.temperature == 0.2


class TestArtifactCaptureConfig:
    def test_defaults(self):
        c = ArtifactCaptureConfig()
        assert c.enabled is True
        assert c.min_content_chars == 200


class TestElephantBrokerConfigPhase6:
    def test_has_phase6_fields(self):
        c = ElephantBrokerConfig()
        assert isinstance(c.context_assembly, ContextAssemblyConfig)
        assert isinstance(c.compaction_llm, CompactionLLMConfig)
        assert c.consolidation_min_retention_seconds == 172800


# --- Artifact models ---

class TestSessionArtifact:
    def test_defaults(self):
        a = SessionArtifact(tool_name="psql", content="SELECT 1")
        assert a.injected_count == 0
        assert a.searched_count == 0
        assert a.summary == ""


class TestSessionArtifactSearchRequest:
    def test_create(self):
        r = SessionArtifactSearchRequest(session_key="sk", session_id="sid", query="test")
        assert r.max_results == 5


class TestCreateArtifactRequest:
    def test_scope_default(self):
        r = CreateArtifactRequest(content="data", session_key="sk", session_id="sid")
        assert r.scope == "session"


# --- Profile additions ---

class TestAssemblyPlacementPolicy:
    def test_defaults(self):
        p = AssemblyPlacementPolicy()
        assert p.system_prompt_constraints is True
        assert p.goal_injection_cadence == "smart"
        assert p.goal_reminder_interval == 5
        assert p.keep_last_n_tool_outputs == 1
        assert p.conversation_dedup_threshold == 0.7


class TestProfilePolicyPhase6:
    def test_has_assembly_placement(self):
        p = make_profile_policy()
        assert isinstance(p.assembly_placement, AssemblyPlacementPolicy)
        assert p.session_data_ttl_seconds == 86400


# --- Trace additions ---

class TestTraceEventTypePhase6:
    def test_new_types_exist(self):
        assert TraceEventType.BOOTSTRAP_COMPLETED == "bootstrap_completed"
        assert TraceEventType.AFTER_TURN_COMPLETED == "after_turn_completed"
        assert TraceEventType.TOKEN_USAGE_REPORTED == "token_usage_reported"
        assert TraceEventType.CONTEXT_WINDOW_REPORTED == "context_window_reported"
        assert TraceEventType.SUCCESSFUL_USE_TRACKED == "successful_use_tracked"
        assert TraceEventType.SUBAGENT_PARENT_MAPPED == "subagent_parent_mapped"
