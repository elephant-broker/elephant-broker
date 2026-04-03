"""Tests for actor schemas."""
import uuid

import pytest
from pydantic import ValidationError

from elephantbroker.schemas.actor import (
    ActorContext,
    ActorRef,
    ActorRelationship,
    ActorType,
    RelationshipType,
)


class TestActorType:
    def test_all_actor_types_exist(self):
        assert len(ActorType) == 12

    def test_from_string(self):
        assert ActorType("human_coordinator") == ActorType.HUMAN_COORDINATOR
        assert ActorType("supervisor_agent") == ActorType.SUPERVISOR_AGENT
        assert ActorType("team_actor") == ActorType.TEAM_ACTOR

    def test_spec_types_present(self):
        expected = {
            "HUMAN_COORDINATOR", "HUMAN_OPERATOR", "MANAGER_AGENT", "WORKER_AGENT",
            "REVIEWER_AGENT", "SUPERVISOR_AGENT", "PEER_AGENT", "SERVICE_ACTOR",
            "EXTERNAL_HUMAN", "EXTERNAL_AGENT", "ORGANIZATION_ACTOR", "TEAM_ACTOR",
        }
        assert {t.name for t in ActorType} == expected


class TestActorRef:
    def test_valid_creation(self):
        ref = ActorRef(type=ActorType.WORKER_AGENT, display_name="worker-1")
        assert ref.display_name == "worker-1"
        assert isinstance(ref.id, uuid.UUID)

    def test_rejects_invalid_type(self):
        with pytest.raises(ValidationError):
            ActorRef(type="not_a_type", display_name="bad")

    def test_json_round_trip(self):
        ref = ActorRef(type=ActorType.SUPERVISOR_AGENT, display_name="sup")
        data = ref.model_dump(mode="json")
        restored = ActorRef.model_validate(data)
        assert restored.display_name == ref.display_name
        assert restored.type == ref.type

    def test_optional_fields_default(self):
        ref = ActorRef(type=ActorType.SERVICE_ACTOR, display_name="m")
        assert ref.authority_level == 0
        assert ref.handles == []
        assert ref.org_id is None
        assert ref.team_ids == []
        assert ref.trust_level == 0.5
        assert ref.tags == []

    def test_authority_level_bounds(self):
        ref = ActorRef(type=ActorType.SERVICE_ACTOR, display_name="m", authority_level=5)
        assert ref.authority_level == 5
        with pytest.raises(ValidationError):
            ActorRef(type=ActorType.SERVICE_ACTOR, display_name="m", authority_level=-1)

    def test_trust_level_bounds(self):
        ref = ActorRef(type=ActorType.WORKER_AGENT, display_name="w", trust_level=0.0)
        assert ref.trust_level == 0.0
        ref2 = ActorRef(type=ActorType.WORKER_AGENT, display_name="w", trust_level=1.0)
        assert ref2.trust_level == 1.0
        with pytest.raises(ValidationError):
            ActorRef(type=ActorType.WORKER_AGENT, display_name="w", trust_level=1.1)
        with pytest.raises(ValidationError):
            ActorRef(type=ActorType.WORKER_AGENT, display_name="w", trust_level=-0.1)

    def test_handles_and_tags(self):
        ref = ActorRef(
            type=ActorType.HUMAN_COORDINATOR,
            display_name="coord",
            handles=["@coord", "coord@org"],
            tags=["admin", "lead"],
        )
        assert len(ref.handles) == 2
        assert len(ref.tags) == 2


class TestRelationshipType:
    def test_all_types_exist(self):
        assert len(RelationshipType) == 12

    def test_spec_types_present(self):
        expected = {
            "DELEGATES_TO", "SUPERVISES", "REPORTS_TO", "COLLABORATES_WITH",
            "TRUSTS", "BLOCKS", "OWNS_GOAL", "OWNS_ARTIFACT",
            "REQUESTED_BY", "APPROVED_BY", "VERIFIED_BY", "PROHIBITED_BY",
        }
        assert {t.name for t in RelationshipType} == expected


class TestActorRelationship:
    def test_valid_creation(self):
        rel = ActorRelationship(
            source_actor_id=uuid.uuid4(),
            target_actor_id=uuid.uuid4(),
            relationship_type=RelationshipType.SUPERVISES,
        )
        assert rel.relationship_type == RelationshipType.SUPERVISES

    def test_json_round_trip(self):
        rel = ActorRelationship(
            source_actor_id=uuid.uuid4(),
            target_actor_id=uuid.uuid4(),
            relationship_type=RelationshipType.DELEGATES_TO,
        )
        data = rel.model_dump(mode="json")
        restored = ActorRelationship.model_validate(data)
        assert restored.relationship_type == rel.relationship_type


class TestActorContext:
    def test_valid_creation(self):
        actor = ActorRef(type=ActorType.HUMAN_COORDINATOR, display_name="user")
        ctx = ActorContext(speaker=actor)
        assert ctx.addressed_actor is None
        assert ctx.authority_chain == []
        assert ctx.coordinators == []
        assert ctx.team_scopes == []
        assert ctx.org_scope is None
        assert ctx.delegation_chain == []

    def test_with_addressed_actor(self):
        speaker = ActorRef(type=ActorType.HUMAN_COORDINATOR, display_name="user")
        target = ActorRef(type=ActorType.WORKER_AGENT, display_name="worker")
        ctx = ActorContext(speaker=speaker, addressed_actor=target)
        assert ctx.addressed_actor is not None
        assert ctx.addressed_actor.display_name == "worker"
