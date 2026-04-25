"""Unit tests for DataPoint model mappings."""
from __future__ import annotations

import uuid

import pytest

from elephantbroker.runtime.adapters.cognee.datapoints import (
    ActorDataPoint,
    ArtifactDataPoint,
    ClaimDataPoint,
    EvidenceDataPoint,
    FactDataPoint,
    GoalDataPoint,
    ProcedureDataPoint,
)
from elephantbroker.schemas.actor import ActorRef, ActorType
from elephantbroker.schemas.artifact import ToolArtifact
from elephantbroker.schemas.evidence import ClaimRecord, ClaimStatus, EvidenceRef
from elephantbroker.schemas.fact import FactAssertion, FactCategory
from elephantbroker.schemas.goal import GoalState, GoalStatus
from elephantbroker.schemas.procedure import ProcedureDefinition

# ---------------------------------------------------------------------------
# FactDataPoint
# ---------------------------------------------------------------------------

class TestFactDataPoint:
    def test_from_schema(self):
        fact = FactAssertion(text="Hello world", category=FactCategory.GENERAL, confidence=0.9)
        dp = FactDataPoint.from_schema(fact)
        assert dp.text == "Hello world"
        assert dp.category == "general"
        assert dp.confidence == 0.9
        assert dp.eb_id == str(fact.id)

    def test_to_schema(self):
        fact = FactAssertion(text="Test fact", category=FactCategory.IDENTITY)
        dp = FactDataPoint.from_schema(fact)
        restored = dp.to_schema()
        assert restored.text == "Test fact"
        assert restored.category == FactCategory.IDENTITY
        assert restored.id == fact.id

    def test_round_trip(self):
        actor_id = uuid.uuid4()
        goal_id = uuid.uuid4()
        fact = FactAssertion(
            text="Round trip",
            category=FactCategory.EVENT,
            confidence=0.75,
            source_actor_id=actor_id,
            target_actor_ids=[uuid.uuid4()],
            goal_ids=[goal_id],
            use_count=5,
            successful_use_count=3,
            provenance_refs=["ref1"],
        )
        dp = FactDataPoint.from_schema(fact)
        restored = dp.to_schema()
        assert restored.text == fact.text
        assert restored.category == fact.category
        assert restored.confidence == fact.confidence
        assert restored.source_actor_id == actor_id
        assert len(restored.target_actor_ids) == 1
        assert restored.goal_ids == [goal_id]
        assert restored.use_count == 5
        assert restored.successful_use_count == 3
        assert restored.provenance_refs == ["ref1"]

    def test_archived_and_blacklisted_round_trip(self):
        """Phase 9: archived and autorecall_blacklisted fields survive DataPoint round-trip."""
        fact = FactAssertion(
            text="archived test",
            archived=True,
            autorecall_blacklisted=True,
        )
        dp = FactDataPoint.from_schema(fact)
        assert dp.archived is True
        assert dp.autorecall_blacklisted is True
        restored = dp.to_schema()
        assert restored.archived is True
        assert restored.autorecall_blacklisted is True

    def test_archived_defaults_to_false(self):
        """Phase 9: new facts default to archived=False, autorecall_blacklisted=False."""
        fact = FactAssertion(text="normal fact")
        dp = FactDataPoint.from_schema(fact)
        assert dp.archived is False
        assert dp.autorecall_blacklisted is False


# ---------------------------------------------------------------------------
# ActorDataPoint
# ---------------------------------------------------------------------------

class TestActorDataPoint:
    def test_from_schema(self):
        actor = ActorRef(type=ActorType.WORKER_AGENT, display_name="bot-1", trust_level=0.8)
        dp = ActorDataPoint.from_schema(actor)
        assert dp.display_name == "bot-1"
        assert dp.actor_type == "worker_agent"
        assert dp.trust_level == 0.8

    def test_to_schema(self):
        actor = ActorRef(type=ActorType.MANAGER_AGENT, display_name="mgr", authority_level=5)
        dp = ActorDataPoint.from_schema(actor)
        restored = dp.to_schema()
        assert restored.display_name == "mgr"
        assert restored.type == ActorType.MANAGER_AGENT
        assert restored.authority_level == 5

    def test_round_trip(self):
        org = uuid.uuid4()
        team = uuid.uuid4()
        actor = ActorRef(
            type=ActorType.REVIEWER_AGENT,
            display_name="reviewer",
            handles=["@rev"],
            org_id=org,
            team_ids=[team],
            tags=["qa"],
        )
        dp = ActorDataPoint.from_schema(actor)
        restored = dp.to_schema()
        assert restored.id == actor.id
        assert restored.handles == ["@rev"]
        assert restored.org_id == org
        assert restored.team_ids == [team]
        assert restored.tags == ["qa"]


# ---------------------------------------------------------------------------
# GoalDataPoint
# ---------------------------------------------------------------------------

class TestGoalDataPoint:
    def test_from_schema(self):
        goal = GoalState(title="Ship v1", description="Release the product")
        dp = GoalDataPoint.from_schema(goal)
        assert dp.title == "Ship v1"
        assert dp.description == "Release the product"

    def test_to_schema(self):
        goal = GoalState(title="Test goal", status=GoalStatus.PAUSED)
        dp = GoalDataPoint.from_schema(goal)
        restored = dp.to_schema()
        assert restored.title == "Test goal"
        assert restored.status == GoalStatus.PAUSED

    def test_round_trip(self):
        owner = uuid.uuid4()
        goal = GoalState(
            title="Complex goal",
            description="With many fields",
            status=GoalStatus.COMPLETED,
            owner_actor_ids=[owner],
            success_criteria=["done"],
            blockers=["blocker1"],
            confidence=0.6,
        )
        dp = GoalDataPoint.from_schema(goal)
        restored = dp.to_schema()
        assert restored.id == goal.id
        assert restored.owner_actor_ids == [owner]
        assert restored.success_criteria == ["done"]
        assert restored.blockers == ["blocker1"]
        assert restored.confidence == 0.6


# ---------------------------------------------------------------------------
# ProcedureDataPoint
# ---------------------------------------------------------------------------

class TestProcedureDataPoint:
    def test_from_schema(self):
        # R2-P2.1 #1146: is_manual_only=True required for activation_modes-empty procedures.
        proc = ProcedureDefinition(name="Deploy", description="Deploy to prod", is_manual_only=True)
        dp = ProcedureDataPoint.from_schema(proc)
        assert dp.name == "Deploy"
        assert dp.description == "Deploy to prod"

    def test_to_schema(self):
        proc = ProcedureDefinition(name="Review", version=3, is_manual_only=True)
        dp = ProcedureDataPoint.from_schema(proc)
        restored = dp.to_schema()
        assert restored.name == "Review"
        assert restored.version == 3

    def test_round_trip(self):
        actor = uuid.uuid4()
        proc = ProcedureDefinition(
            name="Full proc",
            description="Detailed",
            version=2,
            source_actor_id=actor,
            is_manual_only=True,
        )
        dp = ProcedureDataPoint.from_schema(proc)
        restored = dp.to_schema()
        assert restored.id == proc.id
        assert restored.source_actor_id == actor


# ---------------------------------------------------------------------------
# ClaimDataPoint
# ---------------------------------------------------------------------------

class TestClaimDataPoint:
    def test_from_schema(self):
        claim = ClaimRecord(claim_text="The API returns 200", status=ClaimStatus.TOOL_SUPPORTED)
        dp = ClaimDataPoint.from_schema(claim)
        assert dp.claim_text == "The API returns 200"
        assert dp.status == "tool_supported"

    def test_to_schema(self):
        claim = ClaimRecord(claim_text="Works", claim_type="functional")
        dp = ClaimDataPoint.from_schema(claim)
        restored = dp.to_schema()
        assert restored.claim_text == "Works"
        assert restored.claim_type == "functional"

    def test_round_trip(self):
        proc_id = uuid.uuid4()
        goal_id = uuid.uuid4()
        claim = ClaimRecord(
            claim_text="Verified claim",
            status=ClaimStatus.SUPERVISOR_VERIFIED,
            procedure_id=proc_id,
            goal_id=goal_id,
        )
        dp = ClaimDataPoint.from_schema(claim)
        restored = dp.to_schema()
        assert restored.id == claim.id
        assert restored.procedure_id == proc_id
        assert restored.goal_id == goal_id
        assert restored.status == ClaimStatus.SUPERVISOR_VERIFIED


# ---------------------------------------------------------------------------
# EvidenceDataPoint
# ---------------------------------------------------------------------------

class TestEvidenceDataPoint:
    def test_from_schema(self):
        ev = EvidenceRef(type="tool_output", ref_value="sha256:abc123")
        dp = EvidenceDataPoint.from_schema(ev)
        assert dp.evidence_type == "tool_output"
        assert dp.ref_value == "sha256:abc123"

    def test_to_schema(self):
        ev = EvidenceRef(type="chunk_ref", ref_value="chunk-42", content_hash="hash123")
        dp = EvidenceDataPoint.from_schema(ev)
        restored = dp.to_schema()
        assert restored.type == "chunk_ref"
        assert restored.ref_value == "chunk-42"
        assert restored.content_hash == "hash123"

    def test_round_trip(self):
        actor = uuid.uuid4()
        ev = EvidenceRef(
            type="supervisor_sign_off",
            ref_value="approved",
            created_by_actor_id=actor,
        )
        dp = EvidenceDataPoint.from_schema(ev)
        restored = dp.to_schema()
        assert restored.id == ev.id
        assert restored.created_by_actor_id == actor


# ---------------------------------------------------------------------------
# ArtifactDataPoint
# ---------------------------------------------------------------------------

class TestDateConversions:
    def test_dt_to_epoch_ms(self):
        from datetime import UTC, datetime
        from elephantbroker.runtime.adapters.cognee.datapoints import _dt_to_epoch_ms
        dt = datetime(2024, 1, 1, tzinfo=UTC)
        assert _dt_to_epoch_ms(dt) == 1704067200000

    def test_epoch_ms_to_dt(self):
        from datetime import UTC, datetime
        from elephantbroker.runtime.adapters.cognee.datapoints import _epoch_ms_to_dt
        result = _epoch_ms_to_dt(1704067200000)
        assert result == datetime(2024, 1, 1, tzinfo=UTC)

    def test_dt_to_epoch_ms_epoch_zero(self):
        from datetime import UTC, datetime
        from elephantbroker.runtime.adapters.cognee.datapoints import _dt_to_epoch_ms
        dt = datetime(1970, 1, 1, tzinfo=UTC)
        assert _dt_to_epoch_ms(dt) == 0


class TestFactDataPointEdgeCases:
    def test_fact_from_schema_empty_lists(self):
        fact = FactAssertion(text="Test", category=FactCategory.GENERAL, target_actor_ids=[], goal_ids=[])
        dp = FactDataPoint.from_schema(fact)
        restored = dp.to_schema()
        assert restored.target_actor_ids == []
        assert restored.goal_ids == []

    def test_fact_from_schema_with_empty_eb_id(self):
        dp = FactDataPoint(text="Test", category="general", eb_id="")
        restored = dp.to_schema()
        assert restored.id is not None  # generates new UUID

    def test_to_schema_zero_timestamp_falls_back_to_now(self):
        """G1: eb_created_at/eb_updated_at == 0 (sentinel) falls back to datetime.now(UTC).

        Pins the `if self.eb_X_at else datetime.now(UTC)` guard at datapoints.py:110-111.
        Ensures DataPoints ingested before timestamp migrations still reconstruct with a
        sane creation/update time rather than epoch 1970.
        """
        from datetime import UTC, datetime

        dp = FactDataPoint(text="x", category="general", eb_created_at=0, eb_updated_at=0)
        restored = dp.to_schema()
        now = datetime.now(UTC)
        assert abs((restored.created_at - now).total_seconds()) < 2.0
        assert abs((restored.updated_at - now).total_seconds()) < 2.0


class TestClaimDataPointEdgeCases:
    def test_claim_from_schema_empty_eb_id(self):
        dp = ClaimDataPoint(claim_text="Test", eb_id="")
        restored = dp.to_schema()
        assert restored.id is not None


class TestArtifactDataPointEdgeCases:
    def test_artifact_from_schema_empty_eb_id(self):
        dp = ArtifactDataPoint(tool_name="test", eb_id="")
        restored = dp.to_schema()
        assert restored.artifact_id is not None


class TestArtifactDataPoint:
    def test_from_schema(self):
        art = ToolArtifact(tool_name="grep", content="result data", summary="search results")
        dp = ArtifactDataPoint.from_schema(art)
        assert dp.tool_name == "grep"
        assert dp.summary == "search results"
        assert dp.content == "result data"

    def test_to_schema(self):
        art = ToolArtifact(tool_name="ls", content="file.txt", token_estimate=10)
        dp = ArtifactDataPoint.from_schema(art)
        restored = dp.to_schema()
        assert restored.tool_name == "ls"
        assert restored.token_estimate == 10

    def test_round_trip(self):
        session = uuid.uuid4()
        actor = uuid.uuid4()
        goal = uuid.uuid4()
        art = ToolArtifact(
            tool_name="compiler",
            content="binary output",
            summary="compiled",
            session_id=session,
            actor_id=actor,
            goal_id=goal,
            tags=["build"],
        )
        dp = ArtifactDataPoint.from_schema(art)
        restored = dp.to_schema()
        assert restored.artifact_id == art.artifact_id
        assert restored.session_id == session
        assert restored.actor_id == actor
        assert restored.goal_id == goal
        assert restored.tags == ["build"]


class TestFactDataPointPhase4:
    def test_memory_class_field(self):
        from elephantbroker.runtime.adapters.cognee.datapoints import FactDataPoint
        dp = FactDataPoint(text="test", category="general", eb_id="abc")
        assert dp.memory_class == "episodic"

    def test_session_key_field(self):
        from elephantbroker.runtime.adapters.cognee.datapoints import FactDataPoint
        dp = FactDataPoint(text="test", category="general", eb_id="abc", session_key="sk")
        assert dp.session_key == "sk"

    def test_from_schema_with_str_category(self):
        from elephantbroker.schemas.fact import FactAssertion
        from elephantbroker.runtime.adapters.cognee.datapoints import FactDataPoint
        fact = FactAssertion(text="test", category="code_decision")
        dp = FactDataPoint.from_schema(fact)
        assert dp.category == "code_decision"

    def test_from_schema_with_enum_category(self):
        from elephantbroker.schemas.fact import FactAssertion, FactCategory
        from elephantbroker.runtime.adapters.cognee.datapoints import FactDataPoint
        fact = FactAssertion(text="test", category=FactCategory.IDENTITY)
        dp = FactDataPoint.from_schema(fact)
        assert dp.category == "identity"

    def test_to_schema_produces_str_category(self):
        from elephantbroker.runtime.adapters.cognee.datapoints import FactDataPoint
        dp = FactDataPoint(text="test", category="preference", eb_id=str(__import__("uuid").uuid4()))
        fact = dp.to_schema()
        assert isinstance(fact.category, str)
        assert fact.category == "preference"

    def test_roundtrip_memory_class(self):
        from elephantbroker.schemas.fact import FactAssertion, MemoryClass
        from elephantbroker.runtime.adapters.cognee.datapoints import FactDataPoint
        fact = FactAssertion(text="test", category="general", memory_class=MemoryClass.SEMANTIC)
        dp = FactDataPoint.from_schema(fact)
        assert dp.memory_class == "semantic"
        restored = dp.to_schema()
        assert restored.memory_class == "semantic"

    def test_roundtrip_session_fields(self):
        import uuid
        from elephantbroker.schemas.fact import FactAssertion
        from elephantbroker.runtime.adapters.cognee.datapoints import FactDataPoint
        sid = uuid.uuid4()
        fact = FactAssertion(text="test", category="general", session_key="sk", session_id=sid)
        dp = FactDataPoint.from_schema(fact)
        assert dp.session_key == "sk"
        assert dp.session_id == str(sid)
        restored = dp.to_schema()
        assert restored.session_key == "sk"
        assert restored.session_id == sid


# --- Phase 7 DataPoint round-trip tests ---


class TestFactDataPointPhase7:
    def test_decision_domain_round_trip(self):
        from elephantbroker.schemas.fact import FactAssertion
        fact = FactAssertion(text="test fact", decision_domain="financial")
        dp = FactDataPoint.from_schema(fact)
        assert dp.decision_domain == "financial"
        restored = dp.to_schema()
        assert restored.decision_domain == "financial"

    def test_decision_domain_none_round_trip(self):
        from elephantbroker.schemas.fact import FactAssertion
        fact = FactAssertion(text="test fact")
        dp = FactDataPoint.from_schema(fact)
        assert dp.decision_domain is None
        restored = dp.to_schema()
        assert restored.decision_domain is None


class TestProcedureDataPointPhase7:
    def test_steps_json_round_trip(self):
        from elephantbroker.schemas.procedure import ProcedureDefinition, ProcedureStep, ProofRequirement, ProofType
        proc = ProcedureDefinition(
            name="Deploy",
            steps=[ProcedureStep(order=0, instruction="Run tests",
                                 required_evidence=[ProofRequirement(description="log", proof_type=ProofType.CHUNK_REF)])],
            is_manual_only=True,
        )
        dp = ProcedureDataPoint.from_schema(proc)
        assert "Run tests" in dp.steps_json
        restored = dp.to_schema()
        assert len(restored.steps) == 1
        assert restored.steps[0].instruction == "Run tests"
        assert len(restored.steps[0].required_evidence) == 1

    def test_red_line_bindings_round_trip(self):
        from elephantbroker.schemas.procedure import ProcedureDefinition
        proc = ProcedureDefinition(name="Deploy", red_line_bindings=["no_unreviewed_deploys"], is_manual_only=True)
        dp = ProcedureDataPoint.from_schema(proc)
        restored = dp.to_schema()
        assert restored.red_line_bindings == ["no_unreviewed_deploys"]

    def test_approval_requirements_round_trip(self):
        from elephantbroker.schemas.procedure import ProcedureDefinition
        proc = ProcedureDefinition(name="Deploy", approval_requirements=["tech_lead"], is_manual_only=True)
        dp = ProcedureDataPoint.from_schema(proc)
        restored = dp.to_schema()
        assert restored.approval_requirements == ["tech_lead"]

    def test_decision_domain_round_trip(self):
        from elephantbroker.schemas.procedure import ProcedureDefinition
        proc = ProcedureDefinition(name="Deploy", decision_domain="code_change", is_manual_only=True)
        dp = ProcedureDataPoint.from_schema(proc)
        assert dp.decision_domain == "code_change"
        restored = dp.to_schema()
        assert restored.decision_domain == "code_change"

    def test_to_schema_from_dict(self):
        import json
        from elephantbroker.schemas.procedure import ProcedureDefinition, ProcedureStep
        d = {
            "eb_id": "550e8400-e29b-41d4-a716-446655440000",
            "name": "Deploy",
            "description": "Deploy to prod",
            "decision_domain": "code_change",
            "steps_json": json.dumps([{"order": 0, "instruction": "Run tests", "required_evidence": [], "is_optional": False}]),
            "red_line_bindings_json": json.dumps(["no_unreviewed_deploys"]),
            "approval_requirements_json": json.dumps(["tech_lead"]),
            "gateway_id": "test-gw",
        }
        proc = ProcedureDataPoint.to_schema_from_dict(d)
        assert proc.name == "Deploy"
        assert proc.decision_domain == "code_change"
        assert len(proc.steps) == 1
        assert proc.red_line_bindings == ["no_unreviewed_deploys"]
        assert proc.approval_requirements == ["tech_lead"]

    def test_to_schema_from_dict_old_field_names(self):
        """Backward compat: old field names without _json suffix."""
        import json
        d = {
            "eb_id": "550e8400-e29b-41d4-a716-446655440000",
            "name": "Deploy",
            "red_line_bindings": json.dumps(["binding1"]),
            "steps": json.dumps([{"order": 0, "instruction": "Step 1"}]),
        }
        proc = ProcedureDataPoint.to_schema_from_dict(d)
        assert proc.red_line_bindings == ["binding1"]
        assert len(proc.steps) == 1

    def test_procedure_to_schema_handles_malformed_json(self):
        """G4: malformed JSON in steps_json / red_line_bindings_json / approval_requirements_json
        must NOT raise -- degrade gracefully to empty list.

        Pins the three try/except Exception blocks at datapoints.py:295-309. Operationally
        critical: a corrupt Neo4j node property must never crash the procedure lookup path,
        only degrade into an empty collection so the caller can still retrieve the
        surrounding procedure metadata.
        """
        dp = ProcedureDataPoint(
            name="x",
            steps_json="not-json[",
            red_line_bindings_json="{bad",
            approval_requirements_json="}])",
        )
        restored = dp.to_schema()
        assert restored.steps == []
        assert restored.red_line_bindings == []
        assert restored.approval_requirements == []


class TestGoalDataPointPhase7:
    def test_goal_metadata_round_trip(self):
        """Auto-goal metadata survives DataPoint round-trip through Cognee."""
        from elephantbroker.schemas.goal import GoalState
        goal = GoalState(
            title="Complete: Deploy",
            metadata={"source_type": "auto", "source_system": "procedure",
                       "source_id": "abc123", "resolved_by_runtime": "false"},
        )
        dp = GoalDataPoint.from_schema(goal)
        assert dp.goal_meta.get("source_type") == "auto"
        restored = dp.to_schema()
        assert restored.metadata["source_type"] == "auto"
        assert restored.metadata["resolved_by_runtime"] == "false"

    def test_goal_metadata_empty_round_trip(self):
        from elephantbroker.schemas.goal import GoalState
        goal = GoalState(title="Regular goal")
        dp = GoalDataPoint.from_schema(goal)
        assert dp.goal_meta == {}
        restored = dp.to_schema()
        assert restored.metadata == {}

    def test_goal_meta_values_coerced_to_str(self):
        """G3: goal_meta non-str values are coerced to str on to_schema() so GoalState.metadata
        (typed `dict[str, str]`) round-trips safely through Neo4j even when Cognee/Neo4j
        write back native Python types (int, bool, float) for JSON primitives.

        Pins the coercion at datapoints.py:228 (`{str(k): str(v) for k, v in self.goal_meta.items()}`).
        """
        dp = GoalDataPoint(
            title="x",
            goal_meta={"count": 42, "flag": True, "ratio": 0.5},
        )
        restored = dp.to_schema()
        assert restored.metadata == {"count": "42", "flag": "True", "ratio": "0.5"}


# ---------------------------------------------------------------------------
# Contract / cross-class tests (TF-FN-006)
# ---------------------------------------------------------------------------


class TestActorDataPointBackwardCompat:
    """G2: ActorDataPoint has no real backward-compat wiring for the legacy team_id field."""

    def test_legacy_team_id_silently_dropped(self):
        """G2: Pydantic drops the legacy singular `team_id` key; team_ids stays at default [].

        Pins actual behavior (no backward-compat logic exists). The misleading
        'Backward compat' comment that formerly lived at datapoints.py:158 was deleted
        in D8 because it described behavior that the code did NOT implement; the
        auditor confirmed no legacy production data carries the old key.
        """
        dp = ActorDataPoint.model_validate({
            "display_name": "legacy_actor",
            "actor_type": "worker_agent",
            "team_id": "legacy-uuid-should-be-ignored",
        })
        assert dp.team_ids == []


class TestDataPointContractLimits:
    """Documents the DataPoint contract boundary -- fields that are intentionally NOT
    carried on the DataPoint (they live elsewhere in the graph)."""

    def test_claim_datapoint_does_not_carry_evidence_refs(self):
        """G5: ClaimRecord.evidence_refs are stored as SUPPORTS graph edges, not on the
        DataPoint (#154). Pins the contract that evidence traversal is graph-traversal,
        not field-access."""
        assert "evidence_refs" not in ClaimDataPoint.model_fields

    def test_artifact_datapoint_does_not_carry_content_hash(self):
        """G6: ArtifactHash is lost on DataPoint round-trip (#156). Pins the contract
        that content hashing is done outside the DataPoint surface; callers that need
        hashes must look them up via ToolArtifactStore.get_by_hash()."""
        assert "content_hash" not in ArtifactDataPoint.model_fields


class TestAllDataPointsGatewayIdFallback:
    """G7 + G8: cross-class invariants that must hold for every EB-facing DataPoint class."""

    _DP_CLASSES = [
        FactDataPoint,
        ActorDataPoint,
        GoalDataPoint,
        ProcedureDataPoint,
        ClaimDataPoint,
        EvidenceDataPoint,
        ArtifactDataPoint,
    ]

    def test_from_schema_uses_getattr_fallback_for_gateway_id(self):
        """G7: every DataPoint.from_schema(schema) reads gateway_id via getattr-with-default,
        NOT direct attribute access. Pins the compatibility contract -- schemas that
        pre-date gateway identity (or mocks in tests) must still round-trip without
        AttributeError when gateway_id is absent from the source schema."""
        import inspect
        import re

        pattern = re.compile(r"getattr\([^,]+,\s*['\"]gateway_id['\"]")
        for cls in self._DP_CLASSES:
            src = inspect.getsource(cls.from_schema)
            assert pattern.search(src), (
                f"{cls.__name__}.from_schema must read gateway_id via getattr fallback "
                f"(pattern: getattr(..., 'gateway_id', ...)). Direct attribute access would "
                f"break on pre-gateway-identity schemas and in test fixtures."
            )

    @pytest.mark.parametrize(
        "dp_cls",
        [
            FactDataPoint,
            ActorDataPoint,
            GoalDataPoint,
            ProcedureDataPoint,
            ClaimDataPoint,
            EvidenceDataPoint,
            ArtifactDataPoint,
        ],
    )
    def test_all_datapoint_classes_have_index_fields(self, dp_cls):
        """G8: every EB-facing DataPoint declares a non-empty index_fields list on its
        metadata default. This list drives Cognee's vector indexing at add_data_points()
        time -- an empty or missing list would silently skip embedding that class into
        Qdrant, breaking semantic search on that entity type."""
        dp = dp_cls.model_construct()
        idx = dp.metadata.get("index_fields")
        assert isinstance(idx, list)
        assert len(idx) >= 1
