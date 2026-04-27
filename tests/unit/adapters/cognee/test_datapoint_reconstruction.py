"""TF-FN-020 G2-G5 — full simulated-Neo4j -> DataPoint -> to_schema pipeline.

These pin the cross-layer contract:
  simulated_neo4j_props (dict)
      -> clean_graph_props (graph_utils.py)
      -> DataPoint(**props)          (adapters/cognee/datapoints.py)
      -> .to_schema()                 (same file, per-class)

TF-FN-006 already pins the FactDataPoint from_schema/to_schema round-trip
against a pure-Pydantic FactAssertion. This file adds the Neo4j-flavor
round-trip: values arrive in the shapes Neo4j actually hands back
(JSON-encoded dicts, Cognee-injected ``id`` that must be stripped,
ProcedureDataPoint's manually-serialized JSON-string fields), and the
tests assert the reconstruction pipeline survives those shapes.

Together, TF-FN-006 + TF-FN-020 cover the Pydantic-only AND Neo4j-flavor
reconstruction matrix.
"""
from __future__ import annotations

import json
import uuid
from datetime import UTC, datetime

from elephantbroker.runtime.adapters.cognee.datapoints import (
    FactDataPoint, GoalDataPoint, ProcedureDataPoint,
)
from elephantbroker.runtime.graph_utils import clean_graph_props


def _epoch_ms(dt: datetime) -> int:
    """Match datapoints.py's ``_dt_to_epoch_ms`` helper shape."""
    return int(dt.timestamp() * 1000)


class TestFactDataPointNeo4jRoundTrip:
    def test_fact_datapoint_reconstruction_from_simulated_neo4j_props(self):
        """G2 (TF-FN-020): full pipeline
        ``simulated_neo4j_dict -> clean_graph_props -> FactDataPoint(**props) -> .to_schema()``
        returns a FactAssertion with all fields restored. Verifies the
        happy path that powers every structural search in
        ``facade.search()``'s Stage-2 (structural fallback).
        """
        eb_id = str(uuid.uuid4())
        source_actor_id = str(uuid.uuid4())
        now = datetime(2026, 4, 24, 12, 0, 0, tzinfo=UTC)
        # Simulated Neo4j-returned props (Cognee injects ``id`` — a
        # Cognee-generated UUID that differs from our eb_id). All dict-shaped
        # fields serialized as JSON strings per Neo4j property-type coercion.
        simulated = {
            "id": str(uuid.uuid4()),  # Cognee-side, MUST be stripped
            "_labels": ["FactDataPoint"],
            "eb_id": eb_id,
            "text": "coffee brews at 93C",
            "category": "fact",
            "scope": "session",
            "confidence": 0.9,
            "memory_class": "episodic",
            "session_key": "agent:main:main",
            "session_id": str(uuid.uuid4()),
            "source_actor_id": source_actor_id,
            "target_actor_ids": [],
            "goal_ids": [],
            "eb_created_at": _epoch_ms(now),
            "eb_updated_at": _epoch_ms(now),
            "use_count": 2,
            "successful_use_count": 1,
            "provenance_refs": [],
            "gateway_id": "gw-prod",
        }

        props = clean_graph_props(simulated)
        # `id` and `_labels` stripped; everything else passed through.
        assert "id" not in props
        assert "_labels" not in props
        assert props["eb_id"] == eb_id

        dp = FactDataPoint(**props)
        schema = dp.to_schema()
        assert str(schema.id) == eb_id
        assert schema.text == "coffee brews at 93C"
        assert schema.category == "fact"
        assert schema.confidence == 0.9
        assert schema.gateway_id == "gw-prod"
        assert schema.session_key == "agent:main:main"
        assert schema.use_count == 2
        assert schema.successful_use_count == 1


class TestGoalDataPointJsonDictPipeline:
    def test_goal_datapoint_goal_meta_json_string_pipeline(self):
        """G3 (TF-FN-020): the dict-shaped ``goal_meta`` field survives
        the full pipeline. Neo4j stores the dict as a JSON string (curly-
        brace-prefixed); ``clean_graph_props`` deserialises it at
        ``graph_utils.py:25-29`` (the ``{``-prefix check), then
        ``GoalDataPoint.to_schema()`` at ``datapoints.py:226`` guards
        ``isinstance(self.goal_meta, dict)`` before coercing values to
        strings into ``GoalState.metadata``.

        Pins both halves of the round-trip — the utility deserialises
        the dict, and the DataPoint guards against non-dict goal_meta
        (defensive against a future Neo4j driver that doesn't serialize
        dicts to JSON).
        """
        eb_id = str(uuid.uuid4())
        now = datetime(2026, 4, 24, 12, 0, 0, tzinfo=UTC)
        simulated = {
            "id": str(uuid.uuid4()),  # Cognee-injected, to be stripped
            "eb_id": eb_id,
            "title": "ship batch 6",
            "description": "finish 20 foundation flows",
            "status": "active",
            "scope": "session",
            "eb_created_at": _epoch_ms(now),
            "eb_updated_at": _epoch_ms(now),
            "owner_actor_ids": [],
            "success_criteria": ["all flows Tested & Implemented"],
            "blockers": [],
            "confidence": 0.85,
            "evidence": [],
            "gateway_id": "gw-prod",
            # The pivotal field — arrives as a JSON string per Neo4j
            # property coercion.
            "goal_meta": '{"priority": "high", "owner_team": "batch6-foundation"}',
        }

        props = clean_graph_props(simulated)
        # `{`-prefix triggered deserialisation.
        assert props["goal_meta"] == {
            "priority": "high", "owner_team": "batch6-foundation",
        }

        dp = GoalDataPoint(**props)
        schema = dp.to_schema()
        assert str(schema.id) == eb_id
        assert schema.title == "ship batch 6"
        assert schema.gateway_id == "gw-prod"
        # metadata is coerced to dict[str, str] by to_schema's loop.
        assert schema.metadata == {
            "priority": "high", "owner_team": "batch6-foundation",
        }


class TestProcedureDataPointJsonStringPipeline:
    def test_procedure_datapoint_steps_json_pipeline_bypasses_clean_graph_props(self):
        """G4 (TF-FN-020 — UPDATED post R2-P3): ``ProcedureDataPoint`` uses
        the ``*_json: str`` workaround pattern (``steps_json`` at
        ``datapoints.py:265``, plus ``red_line_bindings_json`` and
        ``approval_requirements_json``). The DataPoint field type is
        ``str`` and the class's own ``to_schema()`` explicitly calls
        ``json.loads()`` on each.

        **R2-P3 contract update:** previously the bypass relied on
        ``clean_graph_props`` deserialising ONLY ``{``-prefix strings
        (TF-FN-020 G1 pin). Post-#1163 fix, ``clean_graph_props`` now
        deserialises both ``{`` AND ``[`` prefixes BUT explicitly opts
        out of any key whose name ends in ``_json``. So the
        ProcedureDataPoint contract is preserved by the explicit
        ``*_json`` skip-suffix rule rather than the prior
        list-deserialisation gap. Paired with the new
        ``test_clean_graph_props_skips_json_suffix_keys_for_strings`` in
        ``test_graph_utils.py`` which pins the selective rule.

        If a future refactor drops the ``*_json`` opt-out OR renames the
        ``steps_json`` field to something without that suffix, this
        pipeline breaks (the str-typed field receives a list, raising
        ValidationError on ``ProcedureDataPoint(**props)``).
        """
        eb_id = str(uuid.uuid4())
        now = datetime(2026, 4, 24, 12, 0, 0, tzinfo=UTC)
        # steps_json is manually-pre-serialized list-of-dict by from_schema.
        # Shape must match ProcedureStep's pydantic contract (step_id, order,
        # instruction) — the ``except Exception: pass`` in to_schema swallows
        # ValidationError silently so a wrong shape yields an empty steps list
        # without raising.
        step_id = str(uuid.uuid4())
        steps_json_payload = json.dumps([
            {
                "step_id": step_id,
                "order": 0,
                "instruction": "verify target is valid",
                "required_evidence": [],
                "is_optional": False,
            },
        ])
        simulated = {
            "id": str(uuid.uuid4()),
            "eb_id": eb_id,
            "name": "deployment_procedure",
            "description": "",
            "scope": "session",
            "eb_created_at": _epoch_ms(now),
            "eb_updated_at": _epoch_ms(now),
            "dp_version": 1,
            "source_actor_id": None,
            "gateway_id": "gw-prod",
            "decision_domain": None,
            "steps_json": steps_json_payload,
            "red_line_bindings_json": "[]",
            "approval_requirements_json": "[]",
        }

        props = clean_graph_props(simulated)
        # Key invariant: the JSON-array strings survive clean_graph_props
        # unchanged — graph_utils.py:25 only deserialises `{`-prefix.
        assert props["steps_json"] == steps_json_payload
        assert props["red_line_bindings_json"] == "[]"
        assert props["approval_requirements_json"] == "[]"

        dp = ProcedureDataPoint(**props)
        # The str-typed field holds the pre-serialized payload verbatim.
        assert dp.steps_json == steps_json_payload

        schema = dp.to_schema()
        # to_schema calls json.loads internally and materialises ProcedureStep instances.
        assert str(schema.id) == eb_id
        assert schema.name == "deployment_procedure"
        assert len(schema.steps) == 1
        assert str(schema.steps[0].step_id) == step_id
        assert schema.steps[0].instruction == "verify target is valid"
        assert schema.steps[0].order == 0


class TestCleanGraphPropsCogneeIdStripping:
    def test_clean_graph_props_strips_cognee_id_so_reconstruction_mints_new_id(self):
        """G5 (TF-FN-020): ``clean_graph_props`` strips the ``id`` key
        that Cognee injects on Neo4j nodes, because ``FactDataPoint``
        (and every DataPoint subclass) inherits ``id`` from
        ``DataPoint`` with ``Field(default_factory=uuid.uuid4)``.
        Passing the Cognee-side id through would overwrite the
        auto-generated UUID and risk id-collision with a sibling
        DataPoint stored under the same Cognee id.

        ``eb_id`` — the ElephantBroker-side stable identifier — is
        preserved. ``to_schema()`` reads ``eb_id`` (not ``id``) into
        ``FactAssertion.id``.

        Defensive guard: protects any caller who accidentally writes
        ``add_data_points([dp])`` with a DataPoint that was just read
        from the graph (carrying the Cognee id). The strip ensures a
        fresh id is minted, preventing write-over.
        """
        eb_id = str(uuid.uuid4())
        cognee_injected_id = str(uuid.uuid4())
        simulated = {
            "id": cognee_injected_id,  # Cognee-side
            "eb_id": eb_id,             # EB-side (stable)
            "text": "a fact",
            "category": "fact",
            "gateway_id": "gw-prod",
        }
        props = clean_graph_props(simulated)
        # Cognee id stripped; eb_id preserved.
        assert "id" not in props
        assert props["eb_id"] == eb_id

        # Reconstruction succeeds; the DataPoint's inherited `id` gets a
        # fresh UUID (not equal to the Cognee-side value we just stripped).
        dp = FactDataPoint(**props)
        assert str(dp.id) != cognee_injected_id
        # eb_id is the stable cross-system identifier.
        assert dp.eb_id == eb_id
        # to_schema() reads eb_id into FactAssertion.id (not the fresh
        # Cognee-side id), so the semantic identity is preserved even
        # though the storage-side id is regenerated.
        schema = dp.to_schema()
        assert str(schema.id) == eb_id
