"""Tests for pipeline schemas."""
import uuid

from elephantbroker.schemas.pipeline import (
    ArtifactIngestResult,
    ArtifactInput,
    MessageRole,
    ProcedureIngestResult,
    SessionEndRequest,
    SessionStartRequest,
    TurnIngestResult,
    TurnInput,
)


class TestMessageRole:
    def test_values(self):
        assert MessageRole.USER == "user"
        assert MessageRole.ASSISTANT == "assistant"
        assert MessageRole.SYSTEM == "system"
        assert MessageRole.TOOL == "tool"


class TestTurnInput:
    def test_required_session_key(self):
        t = TurnInput(session_key="agent:main:main")
        assert t.session_key == "agent:main:main"

    def test_defaults(self):
        t = TurnInput(session_key="sk")
        assert t.session_id is None
        assert t.actor_context == {}
        assert t.messages == []
        assert t.goal_ids == []
        assert t.profile_name == "coding"

    def test_with_messages(self):
        t = TurnInput(
            session_key="sk",
            messages=[{"role": "user", "content": "hello"}],
        )
        assert len(t.messages) == 1


class TestTurnIngestResult:
    def test_defaults(self):
        r = TurnIngestResult()
        assert r.facts_extracted == []
        assert r.facts_stored == 0
        assert r.facts_superseded == 0
        assert r.actors_resolved == []
        assert r.memory_classes_assigned == {}
        assert r.trace_event_id is None


class TestArtifactInput:
    def test_required_fields(self):
        a = ArtifactInput(tool_name="bash")
        assert a.tool_name == "bash"
        assert a.tool_args == {}
        assert a.tool_output == ""

    def test_all_fields(self):
        sid = uuid.uuid4()
        a = ArtifactInput(
            tool_name="bash",
            tool_args={"cmd": "ls"},
            tool_output="file.py",
            session_id=sid,
        )
        assert a.session_id == sid


class TestArtifactIngestResult:
    def test_defaults(self):
        r = ArtifactIngestResult()
        assert r.artifact is None
        assert r.summary is None
        assert r.facts_extracted == []
        assert r.is_duplicate is False
        assert r.trace_event_id is None


class TestProcedureIngestResult:
    def test_defaults(self):
        r = ProcedureIngestResult()
        assert r.procedure is None
        assert r.is_new is True
        assert r.previous_version is None
        assert r.edges_created == 0
        assert r.trace_event_id is None


class TestSessionEndRequest:
    def test_defaults(self):
        r = SessionEndRequest(session_key="sk", session_id="abc")
        assert r.reason == "reset"

    def test_custom_reason(self):
        r = SessionEndRequest(session_key="sk", session_id="abc", reason="timeout")
        assert r.reason == "timeout"


class TestSessionStartRequest:
    def test_minimal(self):
        r = SessionStartRequest(session_key="sk", session_id="abc")
        assert r.parent_session_key is None

    def test_with_parent(self):
        r = SessionStartRequest(session_key="sk", session_id="abc", parent_session_key="parent:sk")
        assert r.parent_session_key == "parent:sk"
