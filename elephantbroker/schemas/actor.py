"""Actor model primitives — typed actors replace the single 'user' concept."""
from __future__ import annotations

import uuid
from datetime import UTC, datetime
from enum import StrEnum

from pydantic import BaseModel, Field


class ActorType(StrEnum):
    """All recognized actor types in the system."""
    HUMAN_COORDINATOR = "human_coordinator"
    HUMAN_OPERATOR = "human_operator"
    MANAGER_AGENT = "manager_agent"
    WORKER_AGENT = "worker_agent"
    REVIEWER_AGENT = "reviewer_agent"
    SUPERVISOR_AGENT = "supervisor_agent"
    PEER_AGENT = "peer_agent"
    SERVICE_ACTOR = "service_actor"
    EXTERNAL_HUMAN = "external_human"
    EXTERNAL_AGENT = "external_agent"
    ORGANIZATION_ACTOR = "organization_actor"
    TEAM_ACTOR = "team_actor"


class ActorRef(BaseModel):
    """Reference to a specific actor instance."""
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    type: ActorType
    display_name: str
    authority_level: int = Field(default=0, ge=0)
    handles: list[str] = Field(default_factory=list)
    org_id: uuid.UUID | None = None
    team_ids: list[uuid.UUID] = Field(default_factory=list)
    trust_level: float = Field(default=0.5, ge=0.0, le=1.0)
    tags: list[str] = Field(default_factory=list)
    gateway_id: str = ""


class RelationshipType(StrEnum):
    """Types of relationships between actors."""
    DELEGATES_TO = "delegates_to"
    SUPERVISES = "supervises"
    REPORTS_TO = "reports_to"
    COLLABORATES_WITH = "collaborates_with"
    TRUSTS = "trusts"
    BLOCKS = "blocks"
    OWNS_GOAL = "owns_goal"
    OWNS_ARTIFACT = "owns_artifact"
    REQUESTED_BY = "requested_by"
    APPROVED_BY = "approved_by"
    VERIFIED_BY = "verified_by"
    PROHIBITED_BY = "prohibited_by"


class ActorRelationship(BaseModel):
    """Directed relationship between two actors."""
    source_actor_id: uuid.UUID
    target_actor_id: uuid.UUID
    relationship_type: RelationshipType
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, str] = Field(default_factory=dict)


class ActorContext(BaseModel):
    """Full actor context for a session — the resolved actor and their relationships."""
    speaker: ActorRef
    addressed_actor: ActorRef | None = None
    authority_chain: list[ActorRef] = Field(default_factory=list)
    coordinators: list[ActorRef] = Field(default_factory=list)
    team_scopes: list[uuid.UUID] = Field(default_factory=list)
    org_scope: uuid.UUID | None = None
    delegation_chain: list[ActorRef] = Field(default_factory=list)
