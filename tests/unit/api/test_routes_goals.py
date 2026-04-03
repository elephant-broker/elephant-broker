"""Tests for goal routes."""
import uuid
from unittest.mock import AsyncMock

from elephantbroker.schemas.goal import GoalState


class TestGoalRoutes:
    async def test_create_goal(self, client):
        body = {"title": "Test goal"}
        r = await client.post("/goals/", json=body)
        assert r.status_code == 200

    async def test_get_goal_not_found(self, client):
        r = await client.get(f"/goals/{uuid.uuid4()}")
        assert r.status_code == 404

    async def test_update_goal(self, client, mock_graph):
        goal_id = uuid.uuid4()
        mock_graph.get_entity.return_value = {
            "eb_id": str(goal_id), "title": "Test", "description": "",
            "status": "active", "scope": "session", "eb_created_at": 0,
            "eb_updated_at": 0, "owner_actor_ids": [], "success_criteria": [],
            "blockers": [], "confidence": 1.0,
        }
        r = await client.put(f"/goals/{goal_id}", json={"status": "completed"})
        assert r.status_code == 200

    async def test_get_hierarchy(self, client):
        r = await client.get(f"/goals/hierarchy?root_goal_id={uuid.uuid4()}")
        assert r.status_code == 200

    async def test_create_goal_missing_title_422(self, client):
        r = await client.post("/goals/", json={})
        assert r.status_code == 422

    async def test_update_goal_invalid_status_422(self, client):
        r = await client.put(f"/goals/{uuid.uuid4()}", json={"status": "invalid"})
        assert r.status_code == 422

    async def test_create_goal_when_goals_disabled(self, client, container):
        container.goal_manager = None
        body = {"title": "Test goal"}
        r = await client.post("/goals/", json=body)
        assert r.status_code == 500


class TestGoalGatewayIsolation:
    """Gateway-ID enforcement tests for goal routes."""

    async def test_create_goal_stamps_gateway_id(self, client, container):
        """POST /goals/ stamps goal.gateway_id from the X-EB-Gateway-ID header."""
        captured_goals: list[GoalState] = []

        async def capture_set_goal(goal, **kwargs):
            captured_goals.append(goal)
            return goal

        container.goal_manager.set_goal = AsyncMock(side_effect=capture_set_goal)

        body = {"title": "Tenant-scoped goal"}
        r = await client.post(
            "/goals/",
            json=body,
            headers={"X-EB-Gateway-ID": "tenant-99"},
        )
        assert r.status_code == 200
        assert len(captured_goals) == 1
        assert captured_goals[0].gateway_id == "tenant-99"

    async def test_create_goal_default_gateway(self, client, container):
        """Without X-EB-Gateway-ID header, middleware defaults to 'local'."""
        captured_goals: list[GoalState] = []

        async def capture_set_goal(goal, **kwargs):
            captured_goals.append(goal)
            return goal

        container.goal_manager.set_goal = AsyncMock(side_effect=capture_set_goal)

        body = {"title": "Local goal"}
        r = await client.post("/goals/", json=body)
        assert r.status_code == 200
        assert len(captured_goals) == 1
        assert captured_goals[0].gateway_id == "local"

    async def test_session_goal_stamps_gateway_id(self, client, container):
        """POST /goals/session stamps gateway_id on session goals."""
        captured_goals: list[GoalState] = []
        sid = uuid.uuid4()

        async def capture_add_goal(session_key, session_id, goal):
            captured_goals.append(goal)
            return goal

        # Create a mock session goal store
        mock_store = AsyncMock()
        mock_store.add_goal = AsyncMock(side_effect=capture_add_goal)
        container.session_goal_store = mock_store

        body = {"title": "Session-scoped goal", "description": "test"}
        r = await client.post(
            f"/goals/session?session_key=agent:main:main&session_id={sid}",
            json=body,
            headers={"X-EB-Gateway-ID": "tenant-77"},
        )
        assert r.status_code == 200
        assert len(captured_goals) == 1
        assert captured_goals[0].gateway_id == "tenant-77"
