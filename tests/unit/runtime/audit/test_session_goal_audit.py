"""Tests for SessionGoalAuditStore."""
import tempfile

import pytest

from elephantbroker.runtime.audit.session_goal_audit import SessionGoalAuditStore


@pytest.fixture
def db_path() -> str:
    return tempfile.mktemp(suffix=".db")


@pytest.fixture
def store(db_path: str) -> SessionGoalAuditStore:
    return SessionGoalAuditStore(db_path=db_path)


class TestSessionGoalAuditStore:
    @pytest.mark.asyncio
    async def test_init_creates_table(self, store: SessionGoalAuditStore) -> None:
        await store.init_db()
        try:
            events = await store.get_session_events("sk", "sid")
            assert events == []
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_record_goal_created(self, store: SessionGoalAuditStore) -> None:
        await store.init_db()
        try:
            await store.record_event(
                "sk1", "sid1", "goal-1", "Implement auth module",
                "created",
            )
            events = await store.get_session_events("sk1", "sid1")
            assert len(events) == 1
            assert events[0]["event_type"] == "created"
            assert events[0]["goal_title"] == "Implement auth module"
            assert events[0]["parent_goal_id"] is None
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_record_goal_completed(self, store: SessionGoalAuditStore) -> None:
        await store.init_db()
        try:
            await store.record_event(
                "sk1", "sid1", "goal-1", "Implement auth module",
                "completed",
                evidence="All auth tests passing, PR #42 merged",
            )
            events = await store.get_session_events("sk1", "sid1")
            assert len(events) == 1
            ev = events[0]
            assert ev["event_type"] == "completed"
            assert ev["evidence"] == "All auth tests passing, PR #42 merged"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_record_flushed_event(self, store: SessionGoalAuditStore) -> None:
        await store.init_db()
        try:
            await store.record_event(
                "sk1", "sid1", "goal-1", "Implement auth module",
                "created",
            )
            await store.record_event(
                "sk1", "sid1", "goal-1", "Implement auth module",
                "flushed",
                evidence="Session reset via /new",
            )
            events = await store.get_session_events("sk1", "sid1")
            assert len(events) == 2
            assert events[0]["event_type"] == "created"
            assert events[1]["event_type"] == "flushed"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_get_session_events(self, store: SessionGoalAuditStore) -> None:
        await store.init_db()
        try:
            # Events across two sessions
            await store.record_event("sk1", "sid1", "goal-1", "Goal A", "created")
            await store.record_event(
                "sk1", "sid1", "goal-2", "Goal B", "created",
                parent_goal_id="goal-1",
            )
            await store.record_event("sk2", "sid2", "goal-3", "Goal C", "created")

            events_s1 = await store.get_session_events("sk1", "sid1")
            events_s2 = await store.get_session_events("sk2", "sid2")

            assert len(events_s1) == 2
            assert len(events_s2) == 1
            # Check parent_goal_id was recorded
            assert events_s1[1]["parent_goal_id"] == "goal-1"
            assert events_s2[0]["goal_title"] == "Goal C"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_disabled_audit_noop(self, db_path: str) -> None:
        store = SessionGoalAuditStore(db_path=db_path, enabled=False)
        await store.init_db()
        try:
            await store.record_event("sk1", "sid1", "goal-1", "Goal A", "created")
            events = await store.get_session_events("sk1", "sid1")
            assert events == []
        finally:
            await store.close()
