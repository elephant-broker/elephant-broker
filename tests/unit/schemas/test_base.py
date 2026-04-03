"""Tests for base schema types."""
import pytest

from elephantbroker.schemas.base import ErrorDetail, PaginatedResult, Scope, Timestamp


class TestScope:
    def test_all_scope_values(self):
        assert len(Scope) == 8
        assert Scope.GLOBAL == "global"
        assert Scope.ORGANIZATION == "organization"
        assert Scope.TEAM == "team"
        assert Scope.ACTOR == "actor"
        assert Scope.SESSION == "session"
        assert Scope.TASK == "task"
        assert Scope.SUBAGENT == "subagent"
        assert Scope.ARTIFACT == "artifact"

    def test_scope_from_string(self):
        assert Scope("session") == Scope.SESSION
        assert Scope("team") == Scope.TEAM

    def test_invalid_scope_raises(self):
        with pytest.raises(ValueError):
            Scope("invalid")
        with pytest.raises(ValueError):
            Scope("turn")
        with pytest.raises(ValueError):
            Scope("project")


class TestErrorDetail:
    def test_valid_creation(self):
        err = ErrorDetail(code="E001", message="Something went wrong")
        assert err.code == "E001"

    def test_optional_details_default_none(self):
        err = ErrorDetail(code="E001", message="msg")
        assert err.details is None

    def test_optional_field_default_none(self):
        err = ErrorDetail(code="E001", message="msg")
        assert err.field is None

    def test_field_can_be_set(self):
        err = ErrorDetail(code="E001", message="msg", field="name")
        assert err.field == "name"

    def test_json_round_trip(self):
        err = ErrorDetail(code="E001", message="msg", details={"key": "val"}, field="x")
        data = err.model_dump(mode="json")
        restored = ErrorDetail.model_validate(data)
        assert restored == err


class TestTimestamp:
    def test_default_is_utc_now(self):
        from datetime import datetime

        from pydantic import BaseModel

        class Example(BaseModel):
            created_at: Timestamp

        obj = Example()
        assert obj.created_at.tzinfo is not None
        assert isinstance(obj.created_at, datetime)

    def test_accepts_explicit_value(self):
        from datetime import UTC, datetime

        from pydantic import BaseModel

        class Example(BaseModel):
            created_at: Timestamp

        dt = datetime(2025, 1, 1, tzinfo=UTC)
        obj = Example(created_at=dt)
        assert obj.created_at == dt


class TestPaginatedResult:
    def test_defaults(self):
        result = PaginatedResult[str]()
        assert result.items == []
        assert result.total == 0
        assert result.has_more is False

    def test_with_items(self):
        result = PaginatedResult[str](items=["a", "b"], total=2)
        assert len(result.items) == 2
