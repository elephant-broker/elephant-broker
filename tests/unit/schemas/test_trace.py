"""Tests for trace schemas."""
import uuid

import pytest
from pydantic import ValidationError

from elephantbroker.schemas.trace import TraceEvent, TraceEventType, TraceQuery


class TestTraceEventType:
    def test_all_event_types(self):
        assert len(TraceEventType) == 51  # 44 (Phase 8) + 3 (Phase 9) + 4 (Phase 5 session goals)

    def test_spec_types_present(self):
        expected = {
            "INPUT_RECEIVED", "RETRIEVAL_PERFORMED", "RETRIEVAL_SOURCE_RESULT",
            "TOOL_INVOKED", "ARTIFACT_CREATED",
            "CLAIM_MADE", "CLAIM_VERIFIED", "PROCEDURE_STEP_PASSED", "PROCEDURE_STEP_FAILED",
            "GUARD_TRIGGERED", "COMPACTION_ACTION", "SUBAGENT_SPAWNED", "SUBAGENT_ENDED",
            "CONTEXT_ASSEMBLED", "SCORING_COMPLETED",
            "FACT_EXTRACTED", "FACT_SUPERSEDED", "MEMORY_CLASS_ASSIGNED", "DEDUP_TRIGGERED",
            "SESSION_BOUNDARY", "INGEST_BUFFER_FLUSH", "GDPR_DELETE",
            "COGNEE_COGNIFY_COMPLETED", "DEGRADED_OPERATION",
            # Phase 6 additions
            "BOOTSTRAP_COMPLETED", "AFTER_TURN_COMPLETED", "TOKEN_USAGE_REPORTED",
            "CONTEXT_WINDOW_REPORTED", "SUCCESSFUL_USE_TRACKED", "SUBAGENT_PARENT_MAPPED",
            # Phase 7 additions
            "GUARD_PASSED", "GUARD_NEAR_MISS", "CONSTRAINT_REINJECTED",
            "PROCEDURE_COMPLETION_CHECKED",
            # Phase 8 additions
            "PROFILE_RESOLVED", "ORG_CREATED", "TEAM_CREATED",
            "MEMBER_ADDED", "MEMBER_REMOVED", "ACTOR_MERGED",
            "AUTHORITY_CHECK_FAILED", "HANDLE_RESOLVED",
            "PERSISTENT_GOAL_CREATED", "BOOTSTRAP_ORG_CREATED",
            # Phase 5 additions (session goal lifecycle)
            "SESSION_GOAL_CREATED", "SESSION_GOAL_UPDATED",
            "SESSION_GOAL_BLOCKER_ADDED", "SESSION_GOAL_PROGRESS",
            # Phase 9 additions
            "CONSOLIDATION_STARTED", "CONSOLIDATION_STAGE_COMPLETED",
            "CONSOLIDATION_COMPLETED",
        }
        assert {t.name for t in TraceEventType} == expected


class TestTraceEvent:
    def test_valid_creation(self):
        ev = TraceEvent(event_type=TraceEventType.INPUT_RECEIVED)
        assert isinstance(ev.id, uuid.UUID)
        assert ev.payload == {}

    def test_json_round_trip(self):
        ev = TraceEvent(
            event_type=TraceEventType.COMPACTION_ACTION,
            payload={"tokens_before": 5000},
        )
        data = ev.model_dump(mode="json")
        restored = TraceEvent.model_validate(data)
        assert restored.event_type == TraceEventType.COMPACTION_ACTION
        assert restored.payload == {"tokens_before": 5000}

    def test_optional_fields_default(self):
        ev = TraceEvent(event_type=TraceEventType.GUARD_TRIGGERED)
        assert ev.session_id is None
        assert ev.actor_ids == []
        assert ev.artifact_ids == []
        assert ev.claim_ids == []
        assert ev.procedure_ids == []
        assert ev.goal_ids == []
        assert ev.parent_event_id is None

    def test_actor_ids_list(self):
        ids = [uuid.uuid4(), uuid.uuid4()]
        ev = TraceEvent(event_type=TraceEventType.TOOL_INVOKED, actor_ids=ids)
        assert len(ev.actor_ids) == 2

    def test_payload_accepts_objects(self):
        ev = TraceEvent(
            event_type=TraceEventType.SCORING_COMPLETED,
            payload={"score": 0.95, "count": 42, "items": ["a", "b"]},
        )
        assert ev.payload["score"] == 0.95


class TestTraceQuery:
    def test_defaults(self):
        q = TraceQuery()
        assert q.limit == 100
        assert q.offset == 0
        assert q.actor_ids is None

    def test_limit_bounds(self):
        with pytest.raises(ValidationError):
            TraceQuery(limit=0)
        with pytest.raises(ValidationError):
            TraceQuery(limit=10001)

    def test_offset_non_negative(self):
        with pytest.raises(ValidationError):
            TraceQuery(offset=-1)

    def test_actor_ids_filter(self):
        ids = [uuid.uuid4()]
        q = TraceQuery(actor_ids=ids)
        assert len(q.actor_ids) == 1
