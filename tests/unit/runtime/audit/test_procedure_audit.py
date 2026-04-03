"""Tests for ProcedureAuditStore."""
import asyncio
import tempfile
import uuid

import pytest

from elephantbroker.runtime.audit.procedure_audit import ProcedureAuditStore


@pytest.fixture
def db_path() -> str:
    return tempfile.mktemp(suffix=".db")


@pytest.fixture
def store(db_path: str) -> ProcedureAuditStore:
    return ProcedureAuditStore(db_path=db_path)


class TestProcedureAuditStore:
    @pytest.mark.asyncio
    async def test_init_creates_table(self, store: ProcedureAuditStore) -> None:
        await store.init_db()
        try:
            # Table should exist — querying it should not raise
            events = await store.get_session_events("sk", "sid")
            assert events == []
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_record_qualified_event(self, store: ProcedureAuditStore) -> None:
        await store.init_db()
        try:
            await store.record_event(
                "sk1", "sid1", "proc-1", "Deploy Checklist",
                "qualified",
            )
            events = await store.get_session_events("sk1", "sid1")
            assert len(events) == 1
            assert events[0]["event_type"] == "qualified"
            assert events[0]["procedure_name"] == "Deploy Checklist"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_record_activated_event(self, store: ProcedureAuditStore) -> None:
        await store.init_db()
        try:
            exec_id = str(uuid.uuid4())
            await store.record_event(
                "sk1", "sid1", "proc-1", "Deploy Checklist",
                "activated", execution_id=exec_id,
            )
            events = await store.get_session_events("sk1", "sid1")
            assert len(events) == 1
            assert events[0]["event_type"] == "activated"
            assert events[0]["execution_id"] == exec_id
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_record_step_completed_with_proof(self, store: ProcedureAuditStore) -> None:
        await store.init_db()
        try:
            await store.record_event(
                "sk1", "sid1", "proc-1", "Deploy Checklist",
                "step_completed",
                execution_id="exec-1",
                step_id="step-3",
                step_instruction="Run integration tests",
                proof_type="tool_output",
                proof_value="All 42 tests passed",
            )
            events = await store.get_session_events("sk1", "sid1")
            assert len(events) == 1
            ev = events[0]
            assert ev["step_id"] == "step-3"
            assert ev["step_instruction"] == "Run integration tests"
            assert ev["proof_type"] == "tool_output"
            assert ev["proof_value"] == "All 42 tests passed"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_get_session_events_filters_correctly(self, store: ProcedureAuditStore) -> None:
        await store.init_db()
        try:
            # Record events for two different sessions
            await store.record_event("sk1", "sid1", "proc-1", "Proc A", "qualified")
            await store.record_event("sk1", "sid1", "proc-1", "Proc A", "activated")
            await store.record_event("sk2", "sid2", "proc-2", "Proc B", "qualified")

            events_s1 = await store.get_session_events("sk1", "sid1")
            events_s2 = await store.get_session_events("sk2", "sid2")

            assert len(events_s1) == 2
            assert len(events_s2) == 1
            assert events_s2[0]["procedure_name"] == "Proc B"
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_get_procedure_events(self, store: ProcedureAuditStore) -> None:
        await store.init_db()
        try:
            await store.record_event("sk1", "sid1", "proc-1", "Proc A", "qualified")
            await store.record_event("sk2", "sid2", "proc-1", "Proc A", "activated")
            await store.record_event("sk1", "sid1", "proc-2", "Proc B", "qualified")

            events = await store.get_procedure_events("proc-1")
            assert len(events) == 2
            assert all(e["procedure_id"] == "proc-1" for e in events)
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_disabled_audit_noop(self, db_path: str) -> None:
        store = ProcedureAuditStore(db_path=db_path, enabled=False)
        await store.init_db()
        try:
            # Should silently do nothing
            await store.record_event("sk1", "sid1", "proc-1", "Proc A", "qualified")
            events = await store.get_session_events("sk1", "sid1")
            assert events == []
        finally:
            await store.close()

    @pytest.mark.asyncio
    async def test_concurrent_writes(self, store: ProcedureAuditStore) -> None:
        await store.init_db()
        try:
            # Write 10 events concurrently
            tasks = [
                store.record_event(
                    "sk1", "sid1", "proc-1", "Proc A",
                    f"event_{i}",
                )
                for i in range(10)
            ]
            await asyncio.gather(*tasks)

            events = await store.get_session_events("sk1", "sid1")
            assert len(events) == 10
            event_types = {e["event_type"] for e in events}
            assert event_types == {f"event_{i}" for i in range(10)}
        finally:
            await store.close()
