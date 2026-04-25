"""Tests for procedure schemas."""
import uuid

import pytest
from pydantic import ValidationError

from elephantbroker.schemas.base import Scope
from elephantbroker.schemas.procedure import (
    ProcedureActivation,
    ProcedureDefinition,
    ProcedureExecution,
    ProcedureStep,
    ProofRequirement,
    ProofType,
)


class TestProofType:
    def test_all_proof_types(self):
        assert len(ProofType) == 5
        expected = {"DIFF_HASH", "CHUNK_REF", "RECEIPT", "VERSION_RECORD", "SUPERVISOR_SIGN_OFF"}
        assert {t.name for t in ProofType} == expected


class TestProofRequirement:
    def test_valid_creation(self):
        pr = ProofRequirement(description="Needs supervisor sign-off")
        assert pr.required is True
        assert pr.proof_type == ProofType.CHUNK_REF

    def test_proof_type_enum(self):
        pr = ProofRequirement(description="x", proof_type=ProofType.SUPERVISOR_SIGN_OFF)
        assert pr.proof_type == ProofType.SUPERVISOR_SIGN_OFF


class TestProcedureStep:
    def test_valid_creation(self):
        step = ProcedureStep(order=0, instruction="First step")
        assert isinstance(step.step_id, uuid.UUID)

    def test_order_non_negative(self):
        with pytest.raises(ValidationError):
            ProcedureStep(order=-1, instruction="bad")

    def test_empty_instruction_rejected(self):
        with pytest.raises(ValidationError):
            ProcedureStep(order=0, instruction="")

    def test_is_optional_default(self):
        step = ProcedureStep(order=0, instruction="x")
        assert step.is_optional is False

    def test_required_evidence(self):
        pr = ProofRequirement(description="proof")
        step = ProcedureStep(order=0, instruction="x", required_evidence=[pr])
        assert len(step.required_evidence) == 1


class TestProcedureActivation:
    def test_defaults(self):
        act = ProcedureActivation()
        assert act.manual is False
        assert act.actor_default is False
        assert act.trigger_word is None
        assert act.task_classifier is None
        assert act.goal_bound is False
        assert act.supervisor_forced is False

    def test_trigger_word(self):
        act = ProcedureActivation(trigger_word="deploy")
        assert act.trigger_word == "deploy"


class TestProcedureExecution:
    def test_valid_creation(self):
        ex = ProcedureExecution(procedure_id=uuid.uuid4())
        assert ex.current_step_index == 0
        assert ex.completed_steps == []

    def test_step_index_non_negative(self):
        with pytest.raises(ValidationError):
            ProcedureExecution(procedure_id=uuid.uuid4(), current_step_index=-1)


class TestProcedureDefinition:
    def test_valid_creation(self):
        # R2-P2.1 #1146: is_manual_only=True required when activation_modes
        # is empty (otherwise model_validator rejects).
        proc = ProcedureDefinition(name="Deploy process", is_manual_only=True)
        assert proc.version == 1
        assert proc.steps == []
        assert proc.scope == Scope.SESSION

    def test_empty_name_rejected(self):
        with pytest.raises(ValidationError):
            ProcedureDefinition(name="", is_manual_only=True)

    def test_json_round_trip(self):
        step = ProcedureStep(order=0, instruction="Do it")
        proc = ProcedureDefinition(name="Test proc", steps=[step], is_manual_only=True)
        data = proc.model_dump(mode="json")
        restored = ProcedureDefinition.model_validate(data)
        assert len(restored.steps) == 1
        assert restored.name == proc.name

    def test_activation_modes(self):
        act = ProcedureActivation(manual=True)
        # Non-empty activation_modes → is_manual_only may stay False (default).
        proc = ProcedureDefinition(name="x", activation_modes=[act])
        assert len(proc.activation_modes) == 1

    def test_new_fields_default(self):
        # R2-P2.1 #1146: is_manual_only=True needed to pass validator.
        proc = ProcedureDefinition(name="x", is_manual_only=True)
        assert proc.activation_modes == []
        assert proc.required_evidence == []
        assert proc.red_line_bindings == []
        assert proc.role_variants == {}
        assert proc.approval_requirements == []
        assert proc.retry_patterns == []
