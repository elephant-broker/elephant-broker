"""H4+M11: ProcedureDataPoint round-trip fidelity for activation_modes and is_manual_only.

Pre-fix: activation_modes were never persisted (no activation_modes_json field),
and to_schema()/to_schema_from_dict() hardcoded is_manual_only=True. Any procedure
with activation_modes round-tripping through Neo4j permanently lost its triggers
and degraded to manual-only — a silent data mutation on read.

Post-fix: activation_modes_json persisted via from_schema(), restored via to_schema()
and to_schema_from_dict(). is_manual_only read from stored field, not hardcoded.
Legacy records without activation_modes_json default to manual-only (back-compat).
"""
from __future__ import annotations

import json
import uuid

import pytest

from elephantbroker.runtime.adapters.cognee.datapoints import ProcedureDataPoint
from elephantbroker.schemas.procedure import (
    ProcedureActivation,
    ProcedureDefinition,
    ProcedureStep,
)


def _make_activation(**overrides) -> ProcedureActivation:
    return ProcedureActivation(**overrides)


class TestProcedureDataPointRoundTrip:

    def test_round_trip_preserves_activation_modes(self):
        modes = [
            _make_activation(goal_bound=True),
            _make_activation(trigger_word="deploy"),
        ]
        proc = ProcedureDefinition(
            name="Auto-deploy",
            activation_modes=modes,
            is_manual_only=False,
        )
        dp = ProcedureDataPoint.from_schema(proc)
        assert dp.activation_modes_json != "[]"

        restored = dp.to_schema()
        assert len(restored.activation_modes) == 2
        assert restored.activation_modes[0].goal_bound is True
        assert restored.activation_modes[1].trigger_word == "deploy"
        assert restored.is_manual_only is False

    def test_round_trip_preserves_manual_only_flag(self):
        proc = ProcedureDefinition(
            name="Runbook",
            activation_modes=[],
            is_manual_only=True,
        )
        dp = ProcedureDataPoint.from_schema(proc)
        restored = dp.to_schema()
        assert restored.is_manual_only is True
        assert restored.activation_modes == []

    def test_round_trip_via_dict(self):
        modes = [_make_activation(supervisor_forced=True)]
        proc = ProcedureDefinition(
            name="Supervised",
            activation_modes=modes,
            is_manual_only=False,
        )
        dp = ProcedureDataPoint.from_schema(proc)
        d = {
            "eb_id": str(proc.id),
            "name": dp.name,
            "description": dp.description,
            "scope": dp.scope,
            "is_manual_only": dp.is_manual_only,
            "activation_modes_json": dp.activation_modes_json,
            "steps_json": dp.steps_json,
            "red_line_bindings_json": dp.red_line_bindings_json,
            "approval_requirements_json": dp.approval_requirements_json,
            "gateway_id": dp.gateway_id,
        }
        restored = ProcedureDataPoint.to_schema_from_dict(d)
        assert len(restored.activation_modes) == 1
        assert restored.activation_modes[0].supervisor_forced is True
        assert restored.is_manual_only is False

    def test_round_trip_validator_passes(self):
        """Model validator (#1146) must not reject a properly round-tripped procedure."""
        modes = [_make_activation(manual=True, actor_default=True)]
        proc = ProcedureDefinition(
            name="Valid",
            activation_modes=modes,
            is_manual_only=False,
        )
        dp = ProcedureDataPoint.from_schema(proc)
        restored = dp.to_schema()
        assert restored.activation_modes
        assert restored.is_manual_only is False

    def test_legacy_datapoint_without_activation_modes_json_defaults_to_manual(self):
        """Backward compat: a DataPoint missing activation_modes_json (pre-H4
        record) still produces a valid ProcedureDefinition assumed manual-only."""
        dp = ProcedureDataPoint(
            id=uuid.uuid4(),
            name="Legacy",
            eb_id=str(uuid.uuid4()),
            is_manual_only=False,
            activation_modes_json="[]",
        )
        restored = dp.to_schema()
        assert restored.activation_modes == []
        assert restored.is_manual_only is True
