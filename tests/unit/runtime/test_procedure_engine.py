"""Tests for ProcedureEngine."""
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from elephantbroker.runtime.procedures.engine import ProcedureEngine
from elephantbroker.runtime.trace.ledger import TraceLedger
from tests.fixtures.factories import make_procedure_definition


class TestProcedureEngine:
    def _make(self):
        graph = AsyncMock()
        ledger = TraceLedger()
        return ProcedureEngine(graph, ledger, dataset_name="test_ds"), graph, ledger

    async def test_activate(self):
        engine, graph, _ = self._make()
        proc_id = uuid.uuid4()
        graph.get_entity = AsyncMock(return_value={"eb_id": str(proc_id), "name": "test"})
        result = await engine.activate(proc_id, uuid.uuid4())
        assert result.procedure_id == proc_id

    async def test_activate_missing_raises(self):
        engine, graph, _ = self._make()
        graph.get_entity = AsyncMock(return_value=None)
        with pytest.raises(KeyError):
            await engine.activate(uuid.uuid4(), uuid.uuid4())

    async def test_check_step(self):
        engine, graph, _ = self._make()
        proc_id = uuid.uuid4()
        graph.get_entity = AsyncMock(return_value={"eb_id": str(proc_id), "name": "test"})
        execution = await engine.activate(proc_id, uuid.uuid4())
        result = await engine.check_step(execution.execution_id, uuid.uuid4())
        assert result.complete is True

    async def test_validate_completion(self):
        engine, graph, _ = self._make()
        proc_id = uuid.uuid4()
        graph.get_entity = AsyncMock(return_value={"eb_id": str(proc_id), "name": "test"})
        execution = await engine.activate(proc_id, uuid.uuid4())
        await engine.check_step(execution.execution_id, uuid.uuid4())
        result = await engine.validate_completion(execution.execution_id)
        assert result.complete is True

    async def test_validate_incomplete(self):
        engine, graph, _ = self._make()
        proc_id = uuid.uuid4()
        graph.get_entity = AsyncMock(return_value={"eb_id": str(proc_id), "name": "test"})
        execution = await engine.activate(proc_id, uuid.uuid4())
        result = await engine.validate_completion(execution.execution_id)
        assert result.complete is False

    async def test_store_procedure_calls_add_data_points(self, monkeypatch, mock_add_data_points, mock_cognee):
        """store_procedure() calls add_data_points with ProcedureDataPoint."""
        engine, graph, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.procedures.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.procedures.engine.cognee", mock_cognee)
        proc = make_procedure_definition()
        result = await engine.store_procedure(proc)
        assert result.id == proc.id
        assert len(mock_add_data_points.calls) == 1
        dp = mock_add_data_points.calls[0]["data_points"][0]
        assert dp.eb_id == str(proc.id)

    async def test_store_procedure_calls_cognee_add(self, monkeypatch, mock_add_data_points, mock_cognee):
        """store_procedure() calls cognee.add() with procedure text."""
        engine, graph, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.procedures.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.procedures.engine.cognee", mock_cognee)
        proc = make_procedure_definition(name="Deploy service")
        await engine.store_procedure(proc)
        mock_cognee.add.assert_called_once()
        text = mock_cognee.add.call_args[0][0]
        assert "Deploy service" in text

    async def test_store_procedure_cognee_text_includes_description(self, monkeypatch, mock_add_data_points, mock_cognee):
        """When procedure has description, cognee.add() text includes it."""
        engine, graph, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.procedures.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.procedures.engine.cognee", mock_cognee)
        proc = make_procedure_definition(name="Deploy", description="Deploy to prod")
        await engine.store_procedure(proc)
        text = mock_cognee.add.call_args[0][0]
        assert "Deploy to prod" in text

    async def test_store_procedure_emits_trace_event(self, monkeypatch, mock_add_data_points, mock_cognee):
        """store_procedure() emits INPUT_RECEIVED trace event."""
        engine, graph, ledger = self._make()
        monkeypatch.setattr("elephantbroker.runtime.procedures.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.procedures.engine.cognee", mock_cognee)
        proc = make_procedure_definition()
        await engine.store_procedure(proc)
        from elephantbroker.schemas.trace import TraceQuery
        events = await ledger.query_trace(TraceQuery())
        assert len(events) == 1

    async def test_check_step_idempotent(self):
        engine, graph, _ = self._make()
        proc_id = uuid.uuid4()
        graph.get_entity = AsyncMock(return_value={"eb_id": str(proc_id), "name": "test"})
        execution = await engine.activate(proc_id, uuid.uuid4())
        step_id = uuid.uuid4()
        await engine.check_step(execution.execution_id, step_id)
        await engine.check_step(execution.execution_id, step_id)
        assert execution.completed_steps.count(step_id) == 1

    # --- Phase 6: Redis persistence tests (TD-6) ---

    async def test_persist_execution_to_redis(self):
        """_persist_execution writes execution state to Redis."""
        import json
        from elephantbroker.runtime.redis_keys import RedisKeyBuilder
        redis = AsyncMock()
        redis.get = AsyncMock(return_value=None)
        keys = RedisKeyBuilder("test")
        graph = AsyncMock()
        graph.get_entity = AsyncMock(return_value={"eb_id": "test", "name": "test"})
        engine = ProcedureEngine(graph, TraceLedger(), redis=redis, redis_keys=keys)
        proc_id = uuid.uuid4()
        execution = await engine.activate(proc_id, uuid.uuid4())
        redis.setex.reset_mock()  # Reset from activate's persist
        await engine._persist_execution("sk", "sid", execution)
        redis.setex.assert_called_once()
        # Verify JSON stored
        call_args = redis.setex.call_args
        stored = json.loads(call_args[0][2])
        assert str(execution.execution_id) in stored

    async def test_restore_executions_from_redis(self):
        """restore_executions loads execution state from Redis."""
        import json
        from elephantbroker.runtime.redis_keys import RedisKeyBuilder
        redis = AsyncMock()
        keys = RedisKeyBuilder("test")
        engine = ProcedureEngine(AsyncMock(), TraceLedger(), redis=redis, redis_keys=keys)
        # Create a mock stored execution
        from elephantbroker.schemas.procedure import ProcedureExecution
        exec_data = ProcedureExecution(procedure_id=uuid.uuid4())
        stored = {str(exec_data.execution_id): exec_data.model_dump(mode="json")}
        redis.get = AsyncMock(return_value=json.dumps(stored))
        await engine.restore_executions("sk", "sid")
        assert exec_data.execution_id in engine._executions

    async def test_no_redis_graceful(self):
        """Engine works without Redis (in-memory only, existing behavior)."""
        engine, graph, _ = self._make()
        # _persist_execution should not raise without Redis
        from elephantbroker.schemas.procedure import ProcedureExecution
        exec_data = ProcedureExecution(procedure_id=uuid.uuid4())
        await engine._persist_execution("sk", "sid", exec_data)
        # No exception = pass

    async def test_restore_redis_error_graceful(self):
        """restore_executions handles Redis errors gracefully."""
        from elephantbroker.runtime.redis_keys import RedisKeyBuilder
        redis = AsyncMock()
        redis.get = AsyncMock(side_effect=Exception("Redis down"))
        engine = ProcedureEngine(AsyncMock(), TraceLedger(), redis=redis, redis_keys=RedisKeyBuilder("test"))
        await engine.restore_executions("sk", "sid")
        assert len(engine._executions) == 0

    async def test_execution_json_roundtrip(self):
        """ProcedureExecution survives JSON serialization."""
        import json
        from elephantbroker.schemas.procedure import ProcedureExecution
        proc_id = uuid.uuid4()
        execution = ProcedureExecution(procedure_id=proc_id)
        data = execution.model_dump(mode="json")
        restored = ProcedureExecution(**data)
        assert restored.procedure_id == proc_id
        assert restored.execution_id == execution.execution_id

    # --- Amendment 6.1: configurable TTL ---

    async def test_persist_execution_uses_config_ttl(self):
        """BUG-3: TTL should come from constructor, not hardcoded 86400."""
        from elephantbroker.runtime.redis_keys import RedisKeyBuilder
        from elephantbroker.schemas.procedure import ProcedureExecution
        redis = AsyncMock()
        redis.get = AsyncMock(return_value=None)
        keys = RedisKeyBuilder("test")
        engine = ProcedureEngine(
            AsyncMock(), TraceLedger(), redis=redis, redis_keys=keys,
            ttl_seconds=259200,
        )
        execution = ProcedureExecution(procedure_id=uuid.uuid4())
        await engine._persist_execution("sk", "sid", execution)
        assert redis.setex.call_args[0][1] == 259200  # NOT 86400

    async def test_procedure_engine_ttl_default(self):
        """Default TTL should be 172800."""
        engine = ProcedureEngine(AsyncMock(), TraceLedger())
        assert engine._ttl == 172800

    # --- Amendment 7.2 tests ---

    async def test_validate_completion_uses_procedure_completion_checked_event(self, monkeypatch, mock_add_data_points, mock_cognee):
        """Amendment 7.2 M11: validate_completion emits PROCEDURE_COMPLETION_CHECKED event type."""
        from elephantbroker.schemas.guards import CompletionCheckResult
        from elephantbroker.schemas.trace import TraceQuery, TraceEventType
        monkeypatch.setattr("elephantbroker.runtime.procedures.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.procedures.engine.cognee", mock_cognee)
        engine, graph, ledger = self._make()
        proc_id = uuid.uuid4()
        graph.get_entity = AsyncMock(return_value={"eb_id": str(proc_id), "name": "test"})
        execution = await engine.activate(proc_id, uuid.uuid4())
        # Mock evidence engine returning complete=True so trace event fires
        mock_evidence = AsyncMock()
        mock_evidence.check_completion_requirements = AsyncMock(
            return_value=CompletionCheckResult(complete=True, procedure_id=proc_id))
        engine._evidence_engine = mock_evidence
        await engine.validate_completion(execution.execution_id)
        events = await ledger.query_trace(TraceQuery())
        completion_events = [
            e for e in events if e.event_type == TraceEventType.PROCEDURE_COMPLETION_CHECKED
        ]
        assert len(completion_events) >= 1

    async def test_activate_with_decision_domain(self, monkeypatch, mock_add_data_points, mock_cognee):
        """activate() populates decision_domain from procedure definition."""
        engine, graph, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.procedures.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.procedures.engine.cognee", mock_cognee)
        proc = make_procedure_definition(name="Deploy", decision_domain="code_change")
        await engine.store_procedure(proc)
        # Setup graph mock to return entity with decision_domain
        graph.get_entity = AsyncMock(return_value={
            "eb_id": str(proc.id), "name": "Deploy", "decision_domain": "code_change",
        })
        execution = await engine.activate(proc.id, uuid.uuid4())
        assert execution.decision_domain == "code_change"

    async def test_store_procedure_with_red_line_bindings(self, monkeypatch, mock_add_data_points, mock_cognee):
        """store_procedure preserves red_line_bindings on procedure."""
        engine, graph, _ = self._make()
        monkeypatch.setattr("elephantbroker.runtime.procedures.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.procedures.engine.cognee", mock_cognee)
        proc = make_procedure_definition(
            name="Sensitive op",
            red_line_bindings=["no_financial_actions", "require_supervisor"],
        )
        result = await engine.store_procedure(proc)
        assert result.red_line_bindings == ["no_financial_actions", "require_supervisor"]
        # Verify datapoint was stored
        assert len(mock_add_data_points.calls) == 1

    async def test_multiple_concurrent_executions_same_procedure(self):
        """Multiple activations of the same procedure create distinct executions."""
        engine, graph, _ = self._make()
        proc_id = uuid.uuid4()
        graph.get_entity = AsyncMock(return_value={"eb_id": str(proc_id), "name": "test"})
        exec1 = await engine.activate(proc_id, uuid.uuid4())
        exec2 = await engine.activate(proc_id, uuid.uuid4())
        assert exec1.execution_id != exec2.execution_id
        assert exec1.procedure_id == exec2.procedure_id
        assert len(engine._executions) == 2

    async def test_get_entity_called_with_gateway_id(self):
        """Amendment 7.2 H1: activate calls get_entity with gateway_id kwarg."""
        engine = ProcedureEngine(AsyncMock(), TraceLedger(), gateway_id="gw-99")
        engine._graph.get_entity = AsyncMock(return_value={"eb_id": "test", "name": "test"})
        await engine.activate(uuid.uuid4(), uuid.uuid4())
        engine._graph.get_entity.assert_called_once()
        call_kwargs = engine._graph.get_entity.call_args
        assert call_kwargs[1].get("gateway_id") == "gw-99"


class TestProcedureEngineMetrics:
    """Gaps #5/#6/#7: procedure metrics must fire on activate, step complete, proof submit."""

    def _make_with_metrics(self):
        graph = AsyncMock()
        ledger = TraceLedger()
        metrics = MagicMock()
        engine = ProcedureEngine(graph, ledger, dataset_name="test_ds", metrics=metrics)
        return engine, graph, metrics

    async def test_inc_procedure_activated_on_activate(self):
        """Gap #5: inc_procedure_activated() fires on successful activate()."""
        engine, graph, metrics = self._make_with_metrics()
        proc_id = uuid.uuid4()
        graph.get_entity = AsyncMock(return_value={"eb_id": str(proc_id), "name": "test"})
        await engine.activate(proc_id, uuid.uuid4())
        metrics.inc_procedure_activated.assert_called_once()

    async def test_inc_procedure_step_completed_on_check_step(self):
        """Gap #6: inc_procedure_step_completed() fires when step is newly completed."""
        engine, graph, metrics = self._make_with_metrics()
        proc_id = uuid.uuid4()
        graph.get_entity = AsyncMock(return_value={"eb_id": str(proc_id), "name": "test"})
        execution = await engine.activate(proc_id, uuid.uuid4())
        metrics.reset_mock()  # Clear the activate metric call
        step_id = uuid.uuid4()
        await engine.check_step(execution.execution_id, step_id)
        metrics.inc_procedure_step_completed.assert_called_once()

    async def test_inc_procedure_proof_on_record_step_evidence(self):
        """Gap #7: inc_procedure_proof(proof_type) fires when proof evidence is recorded."""
        engine, graph, metrics = self._make_with_metrics()
        proc_id = uuid.uuid4()
        graph.get_entity = AsyncMock(return_value={"eb_id": str(proc_id), "name": "test"})
        execution = await engine.activate(proc_id, uuid.uuid4())
        # Wire evidence engine mock
        evidence_engine = AsyncMock()
        evidence_engine.record_claim = AsyncMock(return_value=MagicMock(id=uuid.uuid4()))
        evidence_engine.attach_evidence = AsyncMock()
        evidence_engine.verify = AsyncMock()
        engine._evidence_engine = evidence_engine
        metrics.reset_mock()
        await engine.record_step_evidence(
            execution.execution_id, uuid.uuid4(), proof_value="screenshot.png",
        )
        metrics.inc_procedure_proof.assert_called_once_with("tool_output")

    async def test_activate_trace_carries_gateway_session_identity(self):
        """TODO-8-R1-004 / TODO-8-R1-008 — activate() trace stamps gateway+session.

        PROCEDURE_STEP_PASSED is reused for the activation event (no
        PROCEDURE_ACTIVATED enum value exists; B2.5 wired
        inc_procedure_activated() beside it for distinct counting). The
        identity fields (gateway_id, session_key, session_id) must reach
        the trace so /trace/session timeline filters do not silently drop
        activation events. Pre-fix, activate() emitted the trace without
        any of these — breaking per-tenant isolation on activation queries.
        """
        from elephantbroker.schemas.trace import TraceEventType, TraceQuery
        engine = ProcedureEngine(AsyncMock(), TraceLedger(), gateway_id="gw-99")
        engine._graph.get_entity = AsyncMock(return_value={"eb_id": "test", "name": "test"})
        proc_id = uuid.uuid4()
        sid = uuid.uuid4()
        await engine.activate(
            proc_id, uuid.uuid4(),
            session_key="agent:main:main", session_id=sid,
        )
        events = await engine._trace.query_trace(TraceQuery())
        activate_events = [
            e for e in events
            if e.event_type == TraceEventType.PROCEDURE_STEP_PASSED
            and e.payload.get("action") == "activate"
        ]
        assert len(activate_events) == 1
        ev = activate_events[0]
        assert ev.gateway_id == "gw-99"
        assert ev.session_key == "agent:main:main"
        assert ev.session_id == sid

    async def test_validate_completion_evidence_path_trace_carries_identity(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """TODO-8-R1-014 — PROCEDURE_COMPLETION_CHECKED carries gateway_id.

        Sibling event in record_step_evidence (engine.py:217) stamps
        gateway_id; the validate_completion success branch was missing
        the same field. Without this, /trace queries scoped by gateway
        would not surface successful completions on the evidence-engine
        path. session_key/session_id are also pinned because they come
        from the in-memory execution record and are equally critical
        for per-session timeline filtering.
        """
        from elephantbroker.schemas.guards import CompletionCheckResult
        from elephantbroker.schemas.trace import TraceEventType, TraceQuery

        monkeypatch.setattr(
            "elephantbroker.runtime.procedures.engine.add_data_points", mock_add_data_points,
        )
        monkeypatch.setattr(
            "elephantbroker.runtime.procedures.engine.cognee", mock_cognee,
        )
        engine = ProcedureEngine(AsyncMock(), TraceLedger(), gateway_id="gw-77")
        engine._graph.get_entity = AsyncMock(return_value={"eb_id": "x", "name": "x"})
        proc_id = uuid.uuid4()
        sid = uuid.uuid4()
        execution = await engine.activate(
            proc_id, uuid.uuid4(),
            session_key="agent:main:main", session_id=sid,
        )
        evidence_engine = AsyncMock()
        evidence_engine.check_completion_requirements = AsyncMock(
            return_value=CompletionCheckResult(complete=True, procedure_id=proc_id),
        )
        engine._evidence_engine = evidence_engine
        await engine.validate_completion(execution.execution_id)

        events = await engine._trace.query_trace(TraceQuery())
        completion_events = [
            e for e in events
            if e.event_type == TraceEventType.PROCEDURE_COMPLETION_CHECKED
        ]
        assert len(completion_events) == 1
        ev = completion_events[0]
        assert ev.gateway_id == "gw-77"
        assert ev.session_key == "agent:main:main"
        assert ev.session_id == sid

    async def test_validate_completion_fallback_path_fires_metric_and_trace(self):
        """TODO-8-R1-007 — fallback path (no evidence engine) is not silent.

        Without an evidence engine, a procedure with completed_steps > 0
        was returning ``CompletionCheckResult(complete=True)`` while
        ``inc_procedure_completed()`` and the
        ``PROCEDURE_COMPLETION_CHECKED`` trace event silently never fired.
        Deployments without an evidence engine (MEMORY_ONLY tier, or
        operators who disable it) saw ``eb_procedure_completed_total``
        permanently stuck at 0.

        This test pins the new behaviour: when fallback returns
        complete=True, BOTH the metric and the trace event fire; when
        fallback returns complete=False (no completed steps), neither
        fires (a non-completion is not a completion event).
        """
        from elephantbroker.schemas.trace import TraceEventType, TraceQuery
        from unittest.mock import MagicMock

        graph = AsyncMock()
        ledger = TraceLedger()
        metrics = MagicMock()
        engine = ProcedureEngine(graph, ledger, gateway_id="gw-fb", metrics=metrics)
        proc_id = uuid.uuid4()
        graph.get_entity = AsyncMock(return_value={"eb_id": str(proc_id), "name": "x"})
        sid = uuid.uuid4()
        execution = await engine.activate(
            proc_id, uuid.uuid4(),
            session_key="agent:main:main", session_id=sid,
        )
        # Mark a step complete so the fallback returns complete=True.
        await engine.check_step(execution.execution_id, uuid.uuid4())
        metrics.reset_mock()  # Clear activate / step-complete calls.

        # No evidence_engine → fallback path runs.
        result = await engine.validate_completion(execution.execution_id)
        assert result.complete is True
        metrics.inc_procedure_completed.assert_called_once_with()
        events = await ledger.query_trace(TraceQuery())
        completion_events = [
            e for e in events
            if e.event_type == TraceEventType.PROCEDURE_COMPLETION_CHECKED
        ]
        assert len(completion_events) == 1
        ev = completion_events[0]
        assert ev.payload.get("path") == "fallback_no_evidence_engine"
        assert ev.gateway_id == "gw-fb"
        assert ev.session_key == "agent:main:main"
        assert ev.session_id == sid

    async def test_validate_completion_fallback_path_no_completed_steps_silent(self):
        """TODO-8-R1-007 — fallback returning complete=False does NOT fire metric/trace.

        Companion to ``test_validate_completion_fallback_path_fires_metric_and_trace``:
        a non-completion (no completed_steps) must NOT increment
        ``inc_procedure_completed`` or emit ``PROCEDURE_COMPLETION_CHECKED``.
        Pin so a future regression that fires unconditionally would be
        caught.
        """
        from elephantbroker.schemas.trace import TraceEventType, TraceQuery
        from unittest.mock import MagicMock

        graph = AsyncMock()
        ledger = TraceLedger()
        metrics = MagicMock()
        engine = ProcedureEngine(graph, ledger, gateway_id="gw-fb", metrics=metrics)
        proc_id = uuid.uuid4()
        graph.get_entity = AsyncMock(return_value={"eb_id": str(proc_id), "name": "x"})
        execution = await engine.activate(proc_id, uuid.uuid4())
        metrics.reset_mock()

        # No completed_steps → fallback returns complete=False.
        result = await engine.validate_completion(execution.execution_id)
        assert result.complete is False
        metrics.inc_procedure_completed.assert_not_called()
        events = await ledger.query_trace(TraceQuery())
        completion_events = [
            e for e in events
            if e.event_type == TraceEventType.PROCEDURE_COMPLETION_CHECKED
        ]
        assert completion_events == []

    async def test_inc_procedure_completed_on_validate_completion(self):
        """TF-05-009 Flag #2: ``inc_procedure_completed()`` fires when
        ``validate_completion`` reaches the success branch.

        Pins the wiring at ``engine.py:252-254`` (added in C8.1). The
        counter ``eb_procedure_completed_total`` was declared in
        ``MetricsContext`` (``metrics.py:594-596``) but no call site was
        actually incrementing it before this fix — staging Prometheus
        showed `eb_procedure_completed_total=0` despite successful
        completion round-trips. Mirrors the
        ``test_inc_procedure_proof_on_record_step_evidence`` harness:
        activate a procedure, wire an evidence-engine mock that returns
        ``CompletionCheckResult(complete=True)``, reset the metrics mock
        to clear the activate-time call, then call
        ``validate_completion`` and assert the counter was incremented
        exactly once.
        """
        from elephantbroker.schemas.guards import CompletionCheckResult

        engine, graph, metrics = self._make_with_metrics()
        proc_id = uuid.uuid4()
        graph.get_entity = AsyncMock(return_value={"eb_id": str(proc_id), "name": "test"})
        execution = await engine.activate(proc_id, uuid.uuid4())
        evidence_engine = AsyncMock()
        evidence_engine.check_completion_requirements = AsyncMock(
            return_value=CompletionCheckResult(complete=True, procedure_id=proc_id),
        )
        engine._evidence_engine = evidence_engine
        metrics.reset_mock()  # Clear the activate-time metric call.
        result = await engine.validate_completion(execution.execution_id)
        assert result.complete is True
        metrics.inc_procedure_completed.assert_called_once_with()
