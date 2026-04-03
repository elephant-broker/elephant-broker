"""Base types shared across all ElephantBroker schemas."""
from __future__ import annotations

from datetime import UTC, datetime
from enum import StrEnum
from typing import Annotated, Generic, TypeVar

from pydantic import BaseModel, Field

Timestamp = Annotated[datetime, Field(default_factory=lambda: datetime.now(UTC))]
"""UTC datetime that defaults to now. Use as a field type in Pydantic models."""

T = TypeVar("T")


class Scope(StrEnum):
    """Visibility scope for memory entries."""
    GLOBAL = "global"
    ORGANIZATION = "organization"
    TEAM = "team"
    ACTOR = "actor"
    SESSION = "session"
    TASK = "task"
    SUBAGENT = "subagent"
    ARTIFACT = "artifact"


class ErrorDetail(BaseModel):
    """Structured error information."""
    code: str
    message: str
    field: str | None = None
    details: dict[str, str] | None = None


class PaginatedResult(BaseModel, Generic[T]):
    """Paginated query result wrapper."""
    items: list[T] = Field(default_factory=list)
    total: int = 0
    offset: int = 0
    limit: int = 50
    has_more: bool = False
