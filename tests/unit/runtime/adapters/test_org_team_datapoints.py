"""Tests for OrganizationDataPoint and TeamDataPoint (Phase 8)."""
import uuid

from elephantbroker.runtime.adapters.cognee.datapoints import (
    ActorDataPoint,
    GoalDataPoint,
    OrganizationDataPoint,
    TeamDataPoint,
)
from elephantbroker.schemas.actor import ActorRef, ActorType
from elephantbroker.schemas.goal import GoalState


class TestOrganizationDataPoint:
    def test_org_datapoint_round_trip(self):
        org_id = str(uuid.uuid4())
        dp = OrganizationDataPoint(
            id=uuid.UUID(org_id), name="Acme Corporation",
            display_label="Acme", eb_id=org_id,
        )
        assert dp.name == "Acme Corporation"
        assert dp.display_label == "Acme"
        assert dp.eb_id == org_id

    def test_org_datapoint_no_gateway_id(self):
        dp = OrganizationDataPoint(
            id=uuid.uuid4(), name="Test Org", eb_id=str(uuid.uuid4()),
        )
        assert not hasattr(dp, "gateway_id") or dp.__dict__.get("gateway_id") is None

    def test_org_display_label_defaults_empty(self):
        dp = OrganizationDataPoint(id=uuid.uuid4(), name="Test", eb_id="x")
        assert dp.display_label == ""


class TestTeamDataPoint:
    def test_team_datapoint_round_trip(self):
        team_id = str(uuid.uuid4())
        org_id = str(uuid.uuid4())
        dp = TeamDataPoint(
            id=uuid.UUID(team_id), name="Backend Engineering",
            display_label="Backend", org_id=org_id, eb_id=team_id,
        )
        assert dp.name == "Backend Engineering"
        assert dp.org_id == org_id
        assert dp.eb_id == team_id

    def test_team_datapoint_no_gateway_id(self):
        dp = TeamDataPoint(
            id=uuid.uuid4(), name="Test", org_id="org1", eb_id="x",
        )
        assert not hasattr(dp, "gateway_id") or dp.__dict__.get("gateway_id") is None

    def test_team_has_org_id(self):
        org = str(uuid.uuid4())
        dp = TeamDataPoint(id=uuid.uuid4(), name="Test", org_id=org, eb_id="x")
        assert dp.org_id == org


class TestActorDataPointTeamIds:
    def test_actor_multi_team_round_trip(self):
        t1, t2 = uuid.uuid4(), uuid.uuid4()
        actor = ActorRef(
            type=ActorType.HUMAN_COORDINATOR,
            display_name="CEO",
            team_ids=[t1, t2],
        )
        dp = ActorDataPoint.from_schema(actor)
        assert len(dp.team_ids) == 2
        restored = dp.to_schema()
        assert len(restored.team_ids) == 2
        assert t1 in restored.team_ids
        assert t2 in restored.team_ids

    def test_actor_empty_team_ids(self):
        actor = ActorRef(type=ActorType.WORKER_AGENT, display_name="bot")
        dp = ActorDataPoint.from_schema(actor)
        assert dp.team_ids == []
        restored = dp.to_schema()
        assert restored.team_ids == []


class TestGoalDataPointOrgTeam:
    def test_goal_org_id_round_trip(self):
        org = uuid.uuid4()
        goal = GoalState(title="Q1 Roadmap", org_id=org)
        dp = GoalDataPoint.from_schema(goal)
        assert dp.org_id == str(org)
        restored = dp.to_schema()
        assert restored.org_id == org

    def test_goal_team_id_round_trip(self):
        team = uuid.uuid4()
        goal = GoalState(title="Ship auth", team_id=team)
        dp = GoalDataPoint.from_schema(goal)
        assert dp.team_id == str(team)
        restored = dp.to_schema()
        assert restored.team_id == team

    def test_goal_no_org_team_defaults_none(self):
        goal = GoalState(title="Test")
        dp = GoalDataPoint.from_schema(goal)
        assert dp.org_id is None
        assert dp.team_id is None
        restored = dp.to_schema()
        assert restored.org_id is None
        assert restored.team_id is None
