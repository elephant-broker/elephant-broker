"""Tests for context schemas — verifies 1:1 mapping to OpenClaw ContextEngine result types."""
import uuid

import pytest
from pydantic import ValidationError

from elephantbroker.schemas.context import (
    AgentMessage,
    AssembleResult,
    BootstrapResult,
    CompactResult,
    CompactResultDetail,
    ContextEngineRuntimeContext,
    IngestBatchResult,
    IngestResult,
    SubagentEndReason,
    SubagentPacket,
    SystemPromptOverlay,
)


class TestBootstrapResult:
    """Maps to OpenClaw's BootstrapResult {bootstrapped, importedMessages?, reason?}."""

    def test_minimal(self):
        r = BootstrapResult(bootstrapped=True)
        assert r.imported_messages is None
        assert r.reason is None

    def test_with_imported_messages(self):
        msg = AgentMessage(role="user", content="hello")
        r = BootstrapResult(bootstrapped=True, imported_messages=[msg])
        assert len(r.imported_messages) == 1

    def test_json_round_trip(self):
        r = BootstrapResult(bootstrapped=False, reason="session not found")
        data = r.model_dump(mode="json")
        restored = BootstrapResult.model_validate(data)
        assert restored.bootstrapped is False
        assert restored.reason == "session not found"

    def test_openclaw_shape(self):
        """Verify JSON shape matches OpenClaw's BootstrapResult."""
        r = BootstrapResult(bootstrapped=True)
        data = r.model_dump(mode="json")
        assert "bootstrapped" in data
        # importedMessages and reason are optional
        assert "imported_messages" in data or data.get("imported_messages") is None


class TestIngestResult:
    """Maps to OpenClaw's IngestResult {ingested}."""

    def test_valid(self):
        r = IngestResult(ingested=True)
        assert r.ingested is True

    def test_json_shape(self):
        data = IngestResult(ingested=False).model_dump(mode="json")
        assert "ingested" in data


class TestIngestBatchResult:
    """Maps to OpenClaw's IngestBatchResult {ingestedCount}."""

    def test_valid(self):
        r = IngestBatchResult(ingested_count=5)
        assert r.ingested_count == 5
        assert r.facts_stored == 0

    def test_non_negative(self):
        with pytest.raises(ValidationError):
            IngestBatchResult(ingested_count=-1)

    def test_facts_stored_defaults_to_zero(self):
        r = IngestBatchResult(ingested_count=5)
        assert r.facts_stored == 0

    def test_facts_stored_accepts_positive(self):
        r = IngestBatchResult(ingested_count=5, facts_stored=3)
        assert r.facts_stored == 3

    def test_serialization_includes_facts_stored(self):
        data = IngestBatchResult(ingested_count=2, facts_stored=1).model_dump(mode="json")
        assert "facts_stored" in data
        assert data["facts_stored"] == 1


class TestAssembleResult:
    """Maps to OpenClaw's AssembleResult {messages, estimatedTokens, systemPromptAddition?}."""

    def test_minimal(self):
        r = AssembleResult()
        assert r.messages == []
        assert r.estimated_tokens == 0
        assert r.system_prompt_addition is None

    def test_with_messages(self):
        msgs = [AgentMessage(role="system", content="You are helpful")]
        r = AssembleResult(messages=msgs, estimated_tokens=50)
        assert len(r.messages) == 1

    def test_json_round_trip(self):
        r = AssembleResult(
            messages=[AgentMessage(role="user", content="hi")],
            estimated_tokens=10,
            system_prompt_addition="extra context",
        )
        data = r.model_dump(mode="json")
        restored = AssembleResult.model_validate(data)
        assert restored.system_prompt_addition == "extra context"

    def test_openclaw_shape(self):
        """Verify all required OpenClaw fields present."""
        data = AssembleResult().model_dump(mode="json")
        assert "messages" in data
        assert "estimated_tokens" in data


class TestCompactResult:
    """Maps to OpenClaw's CompactResult {ok, compacted, reason?, result?}."""

    def test_minimal(self):
        r = CompactResult(ok=True, compacted=True)
        assert r.reason is None
        assert r.result is None

    def test_with_detail(self):
        detail = CompactResultDetail(tokens_before=5000, tokens_after=2000, summary="Compacted session")
        r = CompactResult(ok=True, compacted=True, result=detail)
        assert r.result.tokens_before == 5000

    def test_json_round_trip(self):
        detail = CompactResultDetail(tokens_before=5000)
        r = CompactResult(ok=True, compacted=True, reason="threshold exceeded", result=detail)
        data = r.model_dump(mode="json")
        restored = CompactResult.model_validate(data)
        assert restored.result.tokens_before == 5000

    def test_openclaw_shape(self):
        """Verify JSON shape matches OpenClaw's CompactResult."""
        data = CompactResult(ok=True, compacted=False).model_dump(mode="json")
        assert "ok" in data
        assert "compacted" in data


class TestSubagentEndReason:
    def test_valid_values(self):
        valid: list[SubagentEndReason] = ["deleted", "completed", "swept", "released"]
        assert len(valid) == 4


class TestSystemPromptOverlay:
    """Maps to OpenClaw's before_prompt_build hook return."""

    def test_all_none_by_default(self):
        o = SystemPromptOverlay()
        assert o.system_prompt is None
        assert o.prepend_context is None
        assert o.prepend_system_context is None
        assert o.append_system_context is None

    def test_json_round_trip(self):
        o = SystemPromptOverlay(append_system_context="Remember: be concise")
        data = o.model_dump(mode="json")
        restored = SystemPromptOverlay.model_validate(data)
        assert restored.append_system_context == "Remember: be concise"


class TestSubagentPacket:
    def test_valid_creation(self):
        p = SubagentPacket(parent_session_key="parent-1", child_session_key="child-1")
        assert p.inherited_facts_count == 0
        assert p.ttl_ms is None

    def test_json_round_trip(self):
        p = SubagentPacket(
            parent_session_key="p",
            child_session_key="c",
            inherited_goals=[uuid.uuid4()],
            ttl_ms=30000,
        )
        data = p.model_dump(mode="json")
        restored = SubagentPacket.model_validate(data)
        assert restored.ttl_ms == 30000


class TestContextEngineRuntimeContext:
    def test_defaults(self):
        ctx = ContextEngineRuntimeContext()
        assert ctx.active_tools == []
        assert ctx.custom_data == {}
