"""Tests that each factory produces a valid model and accepts overrides."""
import uuid

from elephantbroker.schemas.actor import ActorRef, ActorType
from elephantbroker.schemas.artifact import ToolArtifact
from elephantbroker.schemas.config import ElephantBrokerConfig
from elephantbroker.schemas.evidence import ClaimRecord, ClaimStatus, VerificationState, VerificationSummary
from elephantbroker.schemas.fact import FactAssertion, FactCategory
from elephantbroker.schemas.goal import GoalState, GoalStatus
from elephantbroker.schemas.procedure import ProcedureDefinition, ProcedureExecution, ProcedureStep
from elephantbroker.schemas.profile import ProfilePolicy
from elephantbroker.schemas.trace import TraceEvent, TraceEventType
from elephantbroker.schemas.working_set import ScoringWeights, WorkingSetItem, WorkingSetSnapshot
from tests.fixtures.factories import (
    make_actor_ref,
    make_claim_record,
    make_config,
    make_evidence_ref,
    make_fact_assertion,
    make_goal_state,
    make_procedure_definition,
    make_procedure_execution,
    make_procedure_step,
    make_profile_policy,
    make_scoring_weights,
    make_tool_artifact,
    make_trace_event,
    make_verification_state,
    make_verification_summary,
    make_working_set_item,
    make_working_set_snapshot,
)


class TestMakeActorRef:
    def test_default(self):
        ref = make_actor_ref()
        assert isinstance(ref, ActorRef)
        assert ref.type == ActorType.WORKER_AGENT

    def test_override(self):
        ref = make_actor_ref(type=ActorType.HUMAN_COORDINATOR, display_name="boss")
        assert ref.display_name == "boss"
        assert ref.type == ActorType.HUMAN_COORDINATOR


class TestMakeGoalState:
    def test_default(self):
        g = make_goal_state()
        assert isinstance(g, GoalState)
        assert g.title == "Test goal"

    def test_override(self):
        g = make_goal_state(title="Custom", status=GoalStatus.COMPLETED)
        assert g.title == "Custom"
        assert g.status == GoalStatus.COMPLETED


class TestMakeFactAssertion:
    def test_default(self):
        f = make_fact_assertion()
        assert isinstance(f, FactAssertion)
        assert f.text == "Test fact"

    def test_override(self):
        f = make_fact_assertion(text="Custom", category=FactCategory.IDENTITY)
        assert f.category == FactCategory.IDENTITY


class TestMakeClaimRecord:
    def test_default(self):
        c = make_claim_record()
        assert isinstance(c, ClaimRecord)

    def test_override(self):
        c = make_claim_record(claim_text="Custom", status=ClaimStatus.REJECTED)
        assert c.status == ClaimStatus.REJECTED


class TestMakeEvidenceRef:
    def test_default(self):
        e = make_evidence_ref()
        assert e.ref_value == "test-ref"

    def test_override(self):
        e = make_evidence_ref(ref_value="custom")
        assert e.ref_value == "custom"


class TestMakeVerificationState:
    def test_default(self):
        vs = make_verification_state()
        assert isinstance(vs, VerificationState)

    def test_override(self):
        cid = uuid.uuid4()
        vs = make_verification_state(claim_id=cid, status=ClaimStatus.REJECTED)
        assert vs.claim_id == cid


class TestMakeVerificationSummary:
    def test_default(self):
        vs = make_verification_summary()
        assert isinstance(vs, VerificationSummary)

    def test_override(self):
        vs = make_verification_summary(total_claims=5, verified=3)
        assert vs.total_claims == 5


class TestMakeProcedureDefinition:
    def test_default(self):
        p = make_procedure_definition()
        assert isinstance(p, ProcedureDefinition)

    def test_override(self):
        p = make_procedure_definition(name="Custom")
        assert p.name == "Custom"


class TestMakeProcedureStep:
    def test_default(self):
        s = make_procedure_step()
        assert isinstance(s, ProcedureStep)

    def test_override(self):
        s = make_procedure_step(instruction="Custom step")
        assert s.instruction == "Custom step"


class TestMakeProcedureExecution:
    def test_default(self):
        e = make_procedure_execution()
        assert isinstance(e, ProcedureExecution)

    def test_override(self):
        pid = uuid.uuid4()
        e = make_procedure_execution(procedure_id=pid, current_step_index=3)
        assert e.procedure_id == pid
        assert e.current_step_index == 3


class TestMakeTraceEvent:
    def test_default(self):
        t = make_trace_event()
        assert isinstance(t, TraceEvent)

    def test_override(self):
        t = make_trace_event(event_type=TraceEventType.GUARD_TRIGGERED)
        assert t.event_type == TraceEventType.GUARD_TRIGGERED


class TestMakeWorkingSetItem:
    def test_default(self):
        i = make_working_set_item()
        assert isinstance(i, WorkingSetItem)

    def test_override(self):
        i = make_working_set_item(text="Custom", must_inject=True)
        assert i.text == "Custom"
        assert i.must_inject is True


class TestMakeWorkingSetSnapshot:
    def test_default(self):
        s = make_working_set_snapshot()
        assert isinstance(s, WorkingSetSnapshot)

    def test_override(self):
        s = make_working_set_snapshot(token_budget=8000)
        assert s.token_budget == 8000


class TestMakeScoringWeights:
    def test_default(self):
        w = make_scoring_weights()
        assert isinstance(w, ScoringWeights)

    def test_override(self):
        w = make_scoring_weights(turn_relevance=2.0)
        assert w.turn_relevance == 2.0


class TestMakeProfilePolicy:
    def test_default(self):
        p = make_profile_policy()
        assert isinstance(p, ProfilePolicy)

    def test_override(self):
        p = make_profile_policy(id="custom", name="Custom")
        assert p.id == "custom"


class TestMakeToolArtifact:
    def test_default(self):
        a = make_tool_artifact()
        assert isinstance(a, ToolArtifact)

    def test_override(self):
        a = make_tool_artifact(tool_name="custom-tool")
        assert a.tool_name == "custom-tool"


class TestMakeConfig:
    def test_default(self):
        c = make_config()
        assert isinstance(c, ElephantBrokerConfig)

    def test_override(self):
        c = make_config(default_profile="research")
        assert c.default_profile == "research"
