"""Tests for artifact schemas."""
import uuid

import pytest
from pydantic import ValidationError

from elephantbroker.schemas.artifact import ArtifactHash, ArtifactSummary, ToolArtifact


class TestArtifactHash:
    def test_valid_creation(self):
        h = ArtifactHash(value="abc123")
        assert h.algorithm == "sha256"

    def test_empty_value_rejected(self):
        with pytest.raises(ValidationError):
            ArtifactHash(value="")


class TestArtifactSummary:
    def test_valid_creation(self):
        from datetime import UTC, datetime

        s = ArtifactSummary(
            artifact_id=uuid.uuid4(),
            tool_name="pytest",
            summary="All tests passed",
            created_at=datetime.now(UTC),
        )
        assert s.token_estimate == 0


class TestToolArtifact:
    def test_valid_creation(self):
        a = ToolArtifact(tool_name="ruff", content="No issues found")
        assert isinstance(a.artifact_id, uuid.UUID)
        assert a.tags == []

    def test_empty_tool_name_rejected(self):
        with pytest.raises(ValidationError):
            ToolArtifact(tool_name="", content="x")

    def test_json_round_trip(self):
        a = ToolArtifact(tool_name="pytest", content="output here", summary="passed")
        data = a.model_dump(mode="json")
        restored = ToolArtifact.model_validate(data)
        assert restored.tool_name == "pytest"
        assert restored.summary == "passed"
