"""Tests for procedure routes."""
import uuid
from unittest.mock import AsyncMock, MagicMock

from elephantbroker.schemas.guards import StepCheckResult
from elephantbroker.schemas.procedure import ProcedureExecution


class TestProcedureRoutes:
    async def test_create_procedure(self, client, monkeypatch, mock_add_data_points, mock_cognee):
        monkeypatch.setattr("elephantbroker.runtime.procedures.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.procedures.engine.cognee", mock_cognee)
        # #1146: must include is_manual_only or activation_modes per R2-P2.1
        body = {"name": "Test procedure", "description": "A test", "is_manual_only": True}
        r = await client.post("/procedures/", json=body)
        assert r.status_code == 200
        assert r.json()["name"] == "Test procedure"

    async def test_get_procedure(self, client):
        r = await client.get(f"/procedures/{uuid.uuid4()}")
        assert r.status_code == 200

    async def test_activate_procedure(self, client, mock_graph, monkeypatch, mock_add_data_points, mock_cognee):
        monkeypatch.setattr("elephantbroker.runtime.procedures.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.procedures.engine.cognee", mock_cognee)
        proc_id = uuid.uuid4()
        mock_graph.get_entity.return_value = {"eb_id": str(proc_id), "name": "test"}
        body = {"actor_id": str(uuid.uuid4())}
        r = await client.post(f"/procedures/{proc_id}/activate", json=body)
        assert r.status_code == 200

    async def test_create_procedure_missing_name_422(self, client):
        r = await client.post("/procedures/", json={})
        assert r.status_code == 422

    async def test_create_procedure_when_procedures_disabled(self, client, container, monkeypatch, mock_add_data_points, mock_cognee):
        container.procedure_engine = None
        # #1146: must include is_manual_only or activation_modes per R2-P2.1
        body = {"name": "Test proc", "is_manual_only": True}
        r = await client.post("/procedures/", json=body)
        assert r.status_code == 500


class TestProcedureRouteToolMetrics:
    """Gap #8: inc_procedure_tool(tool) must fire on each procedure route."""

    async def test_create_emits_tool_metric(self, client, container, monkeypatch, mock_add_data_points, mock_cognee):
        monkeypatch.setattr("elephantbroker.runtime.procedures.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.procedures.engine.cognee", mock_cognee)
        container.metrics_ctx.inc_procedure_tool = MagicMock()
        body = {"name": "Test procedure", "description": "A test", "is_manual_only": True}
        r = await client.post("/procedures/", json=body)
        assert r.status_code == 200
        container.metrics_ctx.inc_procedure_tool.assert_called_once_with("create")

    async def test_activate_emits_tool_metric(self, client, container, mock_graph, monkeypatch, mock_add_data_points, mock_cognee):
        monkeypatch.setattr("elephantbroker.runtime.procedures.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.procedures.engine.cognee", mock_cognee)
        proc_id = uuid.uuid4()
        mock_graph.get_entity.return_value = {"eb_id": str(proc_id), "name": "test"}
        container.metrics_ctx.inc_procedure_tool = MagicMock()
        body = {"actor_id": str(uuid.uuid4())}
        r = await client.post(f"/procedures/{proc_id}/activate", json=body)
        assert r.status_code == 200
        container.metrics_ctx.inc_procedure_tool.assert_called_once_with("activate")

    async def test_complete_step_emits_tool_metric(self, client, container):
        container.metrics_ctx.inc_procedure_tool = MagicMock()
        eid = uuid.uuid4()
        sid = uuid.uuid4()
        r = await client.post(f"/procedures/{eid}/step/{sid}/complete", json={})
        # Step may return 200 with completed=False (execution not found) — that's fine,
        # the metric fires at route entry regardless
        container.metrics_ctx.inc_procedure_tool.assert_called_once_with("complete_step")

    async def test_session_status_emits_tool_metric(self, client, container):
        container.metrics_ctx.inc_procedure_tool = MagicMock()
        r = await client.get("/procedures/session/status")
        assert r.status_code == 200
        container.metrics_ctx.inc_procedure_tool.assert_called_once_with("session_status")


class TestProcedureLifecycleAudit:
    """TF-05-009 audit-event contract pins.

    Routes consult ``getattr(container, "procedure_audit", None)`` and call
    ``audit.record_event(...)`` on every lifecycle transition. The default
    test container does not set ``procedure_audit`` (audit is optional in
    the test fixture), so each test mounts a MagicMock for the duration
    of the call and asserts the expected ``event_type`` per route.
    """

    async def test_activate_records_audit_event(
        self, client, container, mock_graph, monkeypatch,
        mock_add_data_points, mock_cognee,
    ):
        """TF-05-009 #1: ``POST /procedures/{id}/activate`` records an
        ``activated`` audit event.

        Pins ``api/routes/procedures.py:73-82`` — the ``activated`` event
        type plus the ``execution_id`` payload key. A regression that
        renames the event type or drops the audit hook here would erase
        the only persistent record of which executions started.
        """
        monkeypatch.setattr(
            "elephantbroker.runtime.procedures.engine.add_data_points",
            mock_add_data_points,
        )
        monkeypatch.setattr(
            "elephantbroker.runtime.procedures.engine.cognee", mock_cognee,
        )
        proc_id = uuid.uuid4()
        mock_graph.get_entity.return_value = {"eb_id": str(proc_id), "name": "test"}
        audit = MagicMock()
        audit.record_event = AsyncMock()
        container.procedure_audit = audit
        body = {"actor_id": str(uuid.uuid4())}
        r = await client.post(f"/procedures/{proc_id}/activate", json=body)
        assert r.status_code == 200
        audit.record_event.assert_called_once()
        kwargs = audit.record_event.call_args.kwargs
        assert kwargs["event_type"] == "activated"
        assert kwargs["procedure_id"] == str(proc_id)
        assert "execution_id" in kwargs

    async def test_step_complete_with_proof_records_two_events(
        self, client, container,
    ):
        """TF-05-009 #2: ``POST /procedures/{eid}/step/{sid}/complete``
        with a ``proof_value`` records BOTH ``step_completed`` and
        ``proof_submitted`` audit events.

        Pins ``api/routes/procedures.py:127-145`` — the proof submission
        intentionally fires a *second* audit call so the proof artifact
        and the step completion can be correlated independently in the
        audit log. Without proof, only ``step_completed`` fires; with
        proof, two events must land. A regression that folds the proof
        into the ``step_completed`` payload (or drops the second call)
        would silently break the audit-log invariant.

        Mocks ``check_step`` to return ``StepCheckResult(complete=True)``
        and pre-populates ``engine._executions[execution_id]`` so the
        audit branch (which reads from that dict for context) is reached.
        """
        engine = container.procedure_engine
        execution_id = uuid.uuid4()
        step_id = uuid.uuid4()
        proc_id = uuid.uuid4()
        # Pre-populate engine state so the audit-context lookup succeeds.
        engine._executions[execution_id] = ProcedureExecution(
            execution_id=execution_id,
            procedure_id=proc_id,
            session_key="agent:main:main",
            session_id=uuid.uuid4(),
        )
        # check_step must return complete=True to reach the audit branch.
        engine.check_step = AsyncMock(return_value=StepCheckResult(
            step_id=str(step_id), complete=True, missing_evidence=[],
        ))
        engine.record_step_evidence = AsyncMock()
        audit = MagicMock()
        audit.record_event = AsyncMock()
        container.procedure_audit = audit

        body = {"proof_value": "screenshot.png"}
        r = await client.post(
            f"/procedures/{execution_id}/step/{step_id}/complete", json=body,
        )
        assert r.status_code == 200
        # Two audit events: step_completed + proof_submitted.
        assert audit.record_event.await_count == 2
        event_types = [
            call.kwargs["event_type"]
            for call in audit.record_event.await_args_list
        ]
        assert event_types == ["step_completed", "proof_submitted"]
        # The proof event carries the proof_value verbatim.
        proof_call = audit.record_event.await_args_list[1].kwargs
        assert proof_call["proof_value"] == "screenshot.png"
        assert proof_call["execution_id"] == str(execution_id)
        assert proof_call["step_id"] == str(step_id)
