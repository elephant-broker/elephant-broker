"""Tests for session goals API routes."""
import uuid

from unittest.mock import AsyncMock, MagicMock

from elephantbroker.schemas.goal import GoalState, GoalStatus


def _make_mock_session_goal_store():
    """Create a mock SessionGoalStore with standard methods."""
    store = AsyncMock()
    store.get_goals = AsyncMock(return_value=[])
    store.set_goals = AsyncMock()
    store.add_goal = AsyncMock()
    store.add_blocker = AsyncMock(return_value=None)
    store.update_goal = AsyncMock(return_value=None)
    store.flush_to_cognee = AsyncMock(return_value=0)
    return store


class TestSessionGoalRoutes:
    async def test_get_session_goals_returns_list(self, client, container):
        session_key = "agent:main:main"
        session_id = uuid.uuid4()
        goal = GoalState(title="Implement feature X")
        store = _make_mock_session_goal_store()
        store.get_goals.return_value = [goal]
        container.session_goal_store = store

        r = await client.get(
            "/goals/session",
            params={"session_key": session_key, "session_id": str(session_id)},
        )
        assert r.status_code == 200
        data = r.json()
        assert "goals" in data
        assert len(data["goals"]) == 1
        assert data["goals"][0]["title"] == "Implement feature X"

    async def test_get_session_goals_empty_session_returns_empty(self, client, container):
        store = _make_mock_session_goal_store()
        store.get_goals.return_value = []
        container.session_goal_store = store

        r = await client.get(
            "/goals/session",
            params={"session_key": "agent:main:main", "session_id": str(uuid.uuid4())},
        )
        assert r.status_code == 200
        data = r.json()
        assert data["goals"] == []

    async def test_create_session_goal_returns_created(self, client, container):
        session_key = "agent:main:main"
        session_id = uuid.uuid4()

        async def fake_add_goal(sk, sid, goal):
            return goal

        store = _make_mock_session_goal_store()
        store.add_goal = AsyncMock(side_effect=fake_add_goal)
        container.session_goal_store = store

        r = await client.post(
            "/goals/session",
            params={"session_key": session_key, "session_id": str(session_id)},
            json={"title": "Fix the bug", "description": "Resolve crash on startup"},
        )
        assert r.status_code == 200
        data = r.json()
        assert data["title"] == "Fix the bug"
        assert data["description"] == "Resolve crash on startup"
        assert data["status"] == "active"

    async def test_create_session_goal_with_parent_id(self, client, container):
        parent_id = uuid.uuid4()
        parent_goal = GoalState(id=parent_id, title="Parent goal")

        async def fake_add_goal(sk, sid, goal):
            return goal

        store = _make_mock_session_goal_store()
        store.get_goals = AsyncMock(return_value=[parent_goal])
        store.add_goal = AsyncMock(side_effect=fake_add_goal)
        container.session_goal_store = store

        r = await client.post(
            "/goals/session",
            params={"session_key": "agent:main:main", "session_id": str(uuid.uuid4())},
            json={"title": "Sub-task", "parent_goal_id": str(parent_id)},
        )
        assert r.status_code == 200
        data = r.json()
        assert data["parent_goal_id"] == str(parent_id)

    async def test_create_session_goal_missing_title_returns_422(self, client, container):
        store = _make_mock_session_goal_store()
        container.session_goal_store = store

        r = await client.post(
            "/goals/session",
            params={"session_key": "agent:main:main", "session_id": str(uuid.uuid4())},
            json={"description": "No title provided"},
        )
        assert r.status_code == 422

    async def test_update_status_completed_with_evidence(self, client, container):
        goal_id = uuid.uuid4()
        updated_goal = GoalState(
            id=goal_id, title="Test goal", status=GoalStatus.COMPLETED,
        )
        store = _make_mock_session_goal_store()
        store.update_goal.return_value = updated_goal
        container.session_goal_store = store

        r = await client.patch(
            f"/goals/session/{goal_id}",
            params={"session_key": "agent:main:main", "session_id": str(uuid.uuid4())},
            json={"status": "completed", "evidence": "All tests pass"},
        )
        assert r.status_code == 200
        data = r.json()
        assert data["status"] == "completed"

    async def test_update_status_goal_not_found_returns_404(self, client, container):
        store = _make_mock_session_goal_store()
        store.update_goal.return_value = None
        container.session_goal_store = store

        r = await client.patch(
            f"/goals/session/{uuid.uuid4()}",
            params={"session_key": "agent:main:main", "session_id": str(uuid.uuid4())},
            json={"status": "completed"},
        )
        assert r.status_code == 404

    async def test_update_status_invalid_status_returns_422(self, client, container):
        store = _make_mock_session_goal_store()
        container.session_goal_store = store

        r = await client.patch(
            f"/goals/session/{uuid.uuid4()}",
            params={"session_key": "agent:main:main", "session_id": str(uuid.uuid4())},
            json={"status": "invalid_status"},
        )
        assert r.status_code == 422

    async def test_add_blocker_appends_to_goal(self, client, container):
        goal_id = uuid.uuid4()
        goal = GoalState(id=goal_id, title="Blocked goal", blockers=["Waiting for API key"])
        store = _make_mock_session_goal_store()
        store.add_blocker = AsyncMock(return_value=goal)
        container.session_goal_store = store

        r = await client.post(
            f"/goals/session/{goal_id}/blocker",
            params={"session_key": "agent:main:main", "session_id": str(uuid.uuid4())},
            json={"blocker": "Waiting for API key"},
        )
        assert r.status_code == 200
        data = r.json()
        assert "Waiting for API key" in data["blockers"]

    async def test_add_blocker_goal_not_found_returns_404(self, client, container):
        store = _make_mock_session_goal_store()
        store.add_blocker = AsyncMock(return_value=None)
        container.session_goal_store = store

        r = await client.post(
            f"/goals/session/{uuid.uuid4()}/blocker",
            params={"session_key": "agent:main:main", "session_id": str(uuid.uuid4())},
            json={"blocker": "Some blocker"},
        )
        assert r.status_code == 404

    async def test_record_progress_increases_confidence(self, client, container):
        goal_id = uuid.uuid4()
        goal = GoalState(id=goal_id, title="Progress goal", confidence=0.5)
        store = _make_mock_session_goal_store()
        store.get_goals.return_value = [goal]
        container.session_goal_store = store

        r = await client.post(
            f"/goals/session/{goal_id}/progress",
            params={"session_key": "agent:main:main", "session_id": str(uuid.uuid4())},
            json={"evidence": "Completed step 3 of 5"},
        )
        assert r.status_code == 200
        data = r.json()
        assert data["confidence"] == 0.6  # 0.5 + 0.1

    async def test_record_progress_goal_not_found_returns_404(self, client, container):
        store = _make_mock_session_goal_store()
        store.get_goals.return_value = []
        container.session_goal_store = store

        r = await client.post(
            f"/goals/session/{uuid.uuid4()}/progress",
            params={"session_key": "agent:main:main", "session_id": str(uuid.uuid4())},
            json={"evidence": "Some progress"},
        )
        assert r.status_code == 404

    async def test_session_end_flushes_goals_to_cognee(self, client, container):
        # GF-15: In FULL mode, session_end route delegates to lifecycle.session_end()
        # which handles goal flush internally. Configure the lifecycle mock.
        container.context_lifecycle.session_end = AsyncMock(return_value={"goals_flushed": 3})

        r = await client.post(
            "/sessions/end",
            json={
                "session_key": "agent:main:main",
                "session_id": str(uuid.uuid4()),
                "reason": "reset",
            },
        )
        assert r.status_code == 200
        data = r.json()
        assert data["goals_flushed"] == 3
        container.context_lifecycle.session_end.assert_awaited_once()

    async def test_get_session_goals_with_store_unavailable(self, client, container):
        # When session_goal_store is None, endpoint returns empty goals list
        container.session_goal_store = None

        r = await client.get(
            "/goals/session",
            params={"session_key": "agent:main:main", "session_id": str(uuid.uuid4())},
        )
        assert r.status_code == 200
        data = r.json()
        assert data["goals"] == []
