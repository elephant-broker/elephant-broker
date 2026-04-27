"""Procedure engine — CRUD + activation via Neo4j with Redis persistence (TD-6)."""
from __future__ import annotations

import json
import logging
import uuid

import cognee
from cognee.tasks.storage import add_data_points

from elephantbroker.runtime.adapters.cognee.datapoints import ProcedureDataPoint
from elephantbroker.runtime.adapters.cognee.graph import GraphAdapter
from elephantbroker.runtime.interfaces.procedure_engine import IProcedureEngine
from elephantbroker.runtime.interfaces.trace_ledger import ITraceLedger
from elephantbroker.schemas.procedure import ProcedureDefinition, ProcedureExecution
from elephantbroker.schemas.trace import TraceEvent, TraceEventType
from elephantbroker.schemas.goal import GoalState, GoalStatus
from elephantbroker.schemas.base import Scope
from elephantbroker.schemas.guards import CompletionCheckResult, StepCheckResult

logger = logging.getLogger("elephantbroker.runtime.procedures.engine")


class ProcedureEngine(IProcedureEngine):

    def __init__(self, graph: GraphAdapter, trace_ledger: ITraceLedger,
                 dataset_name: str = "elephantbroker", gateway_id: str = "",
                 redis=None, redis_keys=None, ttl_seconds: int = 172800,
                 metrics=None) -> None:
        self._graph = graph
        self._trace = trace_ledger
        self._executions: dict[uuid.UUID, ProcedureExecution] = {}
        self._dataset_name = dataset_name
        self._gateway_id = gateway_id
        self._redis = redis
        self._keys = redis_keys
        self._ttl = ttl_seconds
        self._metrics = metrics
        self._evidence_engine = None  # Set post-init by container
        self._session_goal_store = None  # Set post-init by container
        self._definitions: dict[uuid.UUID, ProcedureDefinition] = {}
        self._exec_session_map: dict[uuid.UUID, tuple[str, str]] = {}  # execution_id -> (sk, sid)

    async def store_procedure(self, procedure: ProcedureDefinition) -> ProcedureDefinition:
        procedure.gateway_id = procedure.gateway_id or self._gateway_id
        dp = ProcedureDataPoint.from_schema(procedure)
        await add_data_points([dp])

        proc_text = f"Procedure: {procedure.name}"
        if procedure.description:
            proc_text += f" — {procedure.description}"
        await cognee.add(proc_text, dataset_name=self._dataset_name)

        await self._trace.append_event(
            TraceEvent(
                event_type=TraceEventType.INPUT_RECEIVED,
                procedure_ids=[procedure.id],
                payload={"action": "store_procedure", "name": procedure.name},
            )
        )
        return procedure

    async def activate(self, procedure_id: uuid.UUID, actor_id: uuid.UUID | None = None,
                       *, session_key: str = "", session_id: uuid.UUID | None = None) -> ProcedureExecution:
        entity = await self._graph.get_entity(str(procedure_id), gateway_id=self._gateway_id)
        if entity is None:
            raise KeyError(f"Procedure not found: {procedure_id}")

        # Try cache first, then reconstruct ProcedureDefinition from graph
        proc = self._definitions.get(procedure_id)
        if proc is None:
            try:
                from elephantbroker.runtime.adapters.cognee.datapoints import ProcedureDataPoint
                proc = ProcedureDataPoint.to_schema_from_dict(entity)
                self._definitions[procedure_id] = proc
            except Exception as exc:
                logger.warning("Failed to reconstruct ProcedureDefinition for %s: %s", procedure_id, exc)

        execution = ProcedureExecution(
            procedure_id=procedure_id,
            actor_id=actor_id,
            session_key=session_key,
            session_id=session_id,
            decision_domain=proc.decision_domain if proc else None,
        )
        self._executions[execution.execution_id] = execution
        sid_str = str(session_id) if session_id else ""
        self._exec_session_map[execution.execution_id] = (session_key, sid_str)

        # Persist to Redis
        await self._persist_execution(session_key, sid_str, execution)

        # Create auto-goals for proof-required steps (with dedup — abandon orphans from prior activations)
        if self._session_goal_store and session_key and session_id and proc:
            try:
                existing = await self._session_goal_store.get_goals(session_key, session_id)
                for g in existing:
                    if (g.metadata.get("source_type") == "auto"
                            and g.metadata.get("source_id") == str(proc.id)
                            and g.metadata.get("execution_id") != str(execution.execution_id)
                            and g.status == GoalStatus.ACTIVE):
                        g.status = GoalStatus.ABANDONED
                        g.metadata["resolved_by_runtime"] = "true"
                        await self._session_goal_store.update_goal(
                            session_key, session_id, g.id,
                            {"status": GoalStatus.ABANDONED, "metadata": g.metadata})
            except Exception as exc:
                logger.warning("Failed to clean up orphan auto-goals for procedure %s: %s", procedure_id, exc)
            await self._create_auto_goals(execution, proc, session_key, session_id)

        await self._trace.append_event(
            TraceEvent(
                event_type=TraceEventType.PROCEDURE_STEP_PASSED,
                procedure_ids=[procedure_id],
                actor_ids=[actor_id] if actor_id else [],
                payload={"action": "activate", "execution_id": str(execution.execution_id),
                         "decision_domain": (proc.decision_domain if proc else None) or "none"},
            )
        )
        if self._metrics:
            self._metrics.inc_procedure_activated()
        return execution

    async def check_step(self, activation_id: uuid.UUID, step_id: uuid.UUID) -> StepCheckResult:
        execution = self._executions.get(activation_id)
        if execution is None:
            execution = await self._restore_execution(activation_id)
        if execution is None:
            return StepCheckResult(step_id=str(step_id), complete=False, missing_evidence=["Execution not found"])

        # Load procedure definition
        proc = self._definitions.get(execution.procedure_id)
        if proc is None:
            entity = await self._graph.get_entity(str(execution.procedure_id), gateway_id=self._gateway_id)
            if entity:
                try:
                    from elephantbroker.runtime.adapters.cognee.datapoints import ProcedureDataPoint
                    proc = ProcedureDataPoint.to_schema_from_dict(entity)
                    self._definitions[execution.procedure_id] = proc
                except Exception as exc:
                    logger.warning("Failed to reconstruct ProcedureDefinition in check_step for %s: %s", execution.procedure_id, exc)

        # Find the step and validate proof if evidence_engine available
        step = None
        if proc:
            step = next((s for s in proc.steps if s.step_id == step_id), None)

        if step and step.required_evidence and self._evidence_engine:
            from elephantbroker.schemas.evidence import ClaimStatus
            proc_claims = await self._evidence_engine.get_claims_for_procedure(execution.procedure_id)
            missing = []
            for proof_req in step.required_evidence:
                if not proof_req.required:
                    continue
                found = any(
                    c.status not in (ClaimStatus.UNVERIFIED, ClaimStatus.REJECTED)
                    and any(ev.type == proof_req.proof_type.value for ev in c.evidence_refs)
                    for c in proc_claims
                )
                if not found:
                    missing.append(f"{proof_req.proof_type.value}: {proof_req.description}")
            if missing:
                return StepCheckResult(step_id=str(step_id), complete=False, missing_evidence=missing)

        # Mark step complete
        if step_id not in execution.completed_steps:
            execution.completed_steps.append(step_id)
            if self._metrics:
                self._metrics.inc_procedure_step_completed()

        # Update auto-goal sub-goal
        if self._session_goal_store and execution.session_key and execution.session_id:
            await self._resolve_step_auto_goal(execution, step_id)

        await self._persist_execution(execution.session_key, str(execution.session_id or ""), execution)
        return StepCheckResult(step_id=str(step_id), complete=True, missing_evidence=[])

    async def record_step_evidence(
        self, execution_id: uuid.UUID, step_id: uuid.UUID,
        proof_value: str, gateway_id: str = "",
    ) -> None:
        """Auto-create ClaimRecord + EvidenceRef for a completed step.

        Called by the API route after check_step succeeds. Failures are logged
        but do not propagate — step completion is the primary operation.
        """
        if not self._evidence_engine:
            return
        try:
            from elephantbroker.schemas.evidence import ClaimRecord, EvidenceRef
            proc_id = None
            execution = self._executions.get(execution_id)
            if execution:
                proc_id = execution.procedure_id

            claim = ClaimRecord(
                claim_text=f"Step {step_id} completed with proof",
                procedure_id=proc_id,
                step_id=step_id,
                gateway_id=gateway_id,
            )
            claim = await self._evidence_engine.record_claim(claim)

            evidence = EvidenceRef(
                type="tool_output",
                ref_value=proof_value,
                gateway_id=gateway_id,
            )
            await self._evidence_engine.attach_evidence(claim.id, evidence)
            await self._evidence_engine.verify(claim.id)

            if self._metrics:
                self._metrics.inc_procedure_proof(evidence.type)

            if self._trace:
                await self._trace.append_event(TraceEvent(
                    event_type=TraceEventType.PROCEDURE_COMPLETION_CHECKED,
                    gateway_id=gateway_id,
                    procedure_ids=[proc_id] if proc_id else [],
                    payload={
                        "action": "step_evidence_recorded",
                        "execution_id": str(execution_id),
                        "step_id": str(step_id),
                        "claim_id": str(claim.id),
                    },
                ))
        except Exception:
            logger.warning(
                "Auto-evidence creation failed for step %s (step already completed)",
                step_id, exc_info=True,
            )

    async def validate_completion(self, activation_id: uuid.UUID) -> CompletionCheckResult:
        execution = self._executions.get(activation_id)
        if execution is None:
            execution = await self._restore_execution(activation_id)
        if execution is None:
            return CompletionCheckResult(complete=False, procedure_id=uuid.UUID(int=0),
                                         missing_evidence=["Execution not found"])

        if self._evidence_engine:
            result = await self._evidence_engine.check_completion_requirements(execution.procedure_id)
            if result.complete:
                execution.completed_steps = list(set(execution.completed_steps))
                await self._persist_execution(execution.session_key, str(execution.session_id or ""), execution)
                if self._session_goal_store and execution.session_key and execution.session_id:
                    await self._resolve_parent_auto_goal(execution)
                await self._trace.append_event(TraceEvent(
                    event_type=TraceEventType.PROCEDURE_COMPLETION_CHECKED,
                    procedure_ids=[execution.procedure_id],
                    payload={"action": "completed", "execution_id": str(execution.execution_id)},
                ))
            return result

        # Fallback: no evidence engine
        all_done = len(execution.completed_steps) > 0
        return CompletionCheckResult(complete=all_done, procedure_id=execution.procedure_id)

    async def get_active_execution_ids(self, session_key: str, session_id: uuid.UUID) -> list[uuid.UUID]:
        """Return procedure_ids for active (non-completed) executions in this session."""
        result = []
        for exc in self._executions.values():
            if exc.session_key == session_key and exc.session_id == session_id:
                proc = self._definitions.get(exc.procedure_id)
                if proc and proc.steps:
                    required_steps = [s.step_id for s in proc.steps if not s.is_optional]
                    if required_steps and not all(sid in exc.completed_steps for sid in required_steps):
                        result.append(exc.procedure_id)
                    elif not required_steps:
                        result.append(exc.procedure_id)  # All steps optional = assume active
                else:
                    result.append(exc.procedure_id)
        return result

    # --- Phase 6: Redis persistence (TD-6) ---

    async def _persist_execution(self, sk: str, sid: str, execution: ProcedureExecution) -> None:
        """Persist execution state to Redis for crash recovery."""
        if not self._redis or not self._keys:
            return
        try:
            key = self._keys.procedure_exec(sk, sid)
            raw = await self._redis.get(key)
            data = json.loads(raw) if raw else {}
            data[str(execution.execution_id)] = execution.model_dump(mode="json")
            await self._redis.setex(key, self._ttl, json.dumps(data))
        except Exception as exc:
            logger.warning("Failed to persist procedure execution: %s", exc)

    async def _restore_execution(self, execution_id: uuid.UUID) -> ProcedureExecution | None:
        """Try to restore a procedure execution from Redis using reverse index."""
        if not self._redis or not self._keys:
            return None
        mapping = self._exec_session_map.get(execution_id)
        if mapping is None:
            return None
        sk, sid = mapping
        try:
            key = self._keys.procedure_exec(sk, sid)
            raw = await self._redis.get(key)
            if raw:
                data = json.loads(raw)
                exec_data = data.get(str(execution_id))
                if exec_data:
                    execution = ProcedureExecution(**exec_data)
                    self._executions[execution.execution_id] = execution
                    return execution
        except Exception as exc:
            logger.warning("Failed to restore procedure execution %s: %s", execution_id, exc)
        return None

    async def restore_executions(self, sk: str, sid: str) -> None:
        """Restore all executions for a session from Redis. Called during bootstrap."""
        if not self._redis or not self._keys:
            return
        try:
            key = self._keys.procedure_exec(sk, sid)
            raw = await self._redis.get(key)
            if raw:
                data = json.loads(raw)
                for eid_str, exec_data in data.items():
                    execution = ProcedureExecution(**exec_data)
                    self._executions[execution.execution_id] = execution
                    self._exec_session_map[execution.execution_id] = (sk, sid)
                logger.info("Restored %d procedure executions for %s", len(data), sk)
        except Exception as exc:
            logger.warning("Failed to restore procedure executions: %s", exc)

    # --- Phase 7: Auto-goals & completion gate helpers ---

    async def _create_auto_goals(self, execution: ProcedureExecution,
                                  proc: ProcedureDefinition,
                                  session_key: str, session_id: uuid.UUID) -> None:
        """Create session goals for procedure tracking."""
        parent_goal = GoalState(
            title=f"Complete: {proc.name}",
            description=proc.description or f"Complete procedure '{proc.name}' with required evidence",
            status=GoalStatus.ACTIVE,
            scope=Scope.SESSION,
            metadata={
                "source_type": "auto",
                "source_system": "procedure",
                "source_id": str(proc.id),
                "execution_id": str(execution.execution_id),
                "resolved_by_runtime": "false",
            },
        )
        await self._session_goal_store.add_goal(session_key, session_id, parent_goal)

        for step in proc.steps:
            if not step.required_evidence:
                continue
            for proof in step.required_evidence:
                if not proof.required:
                    continue
                sub_goal = GoalState(
                    title=f"Provide: {proof.description} ({proof.proof_type.value})",
                    description=f"Step: {step.instruction}",
                    status=GoalStatus.ACTIVE,
                    scope=Scope.SESSION,
                    parent_goal_id=parent_goal.id,
                    metadata={
                        "source_type": "auto",
                        "source_system": "procedure_step",
                        "source_id": str(proc.id),
                        "execution_id": str(execution.execution_id),
                        "step_id": str(step.step_id),
                        "proof_type": proof.proof_type.value,
                        "resolved_by_runtime": "false",
                    },
                )
                await self._session_goal_store.add_goal(session_key, session_id, sub_goal)

    async def _resolve_step_auto_goal(self, execution: ProcedureExecution, step_id: uuid.UUID) -> None:
        """Mark the auto-goal sub-goal for this step as COMPLETED."""
        if not self._session_goal_store or not execution.session_key or not execution.session_id:
            return
        goals = await self._session_goal_store.get_goals(execution.session_key, execution.session_id)
        for goal in goals:
            if (goal.metadata.get("source_type") == "auto"
                    and goal.metadata.get("source_system") == "procedure_step"
                    and goal.metadata.get("step_id") == str(step_id)
                    and goal.metadata.get("execution_id") == str(execution.execution_id)):
                goal.status = GoalStatus.COMPLETED
                goal.metadata["resolved_by_runtime"] = "true"
                await self._session_goal_store.update_goal(
                    execution.session_key, execution.session_id, goal.id, {"status": GoalStatus.COMPLETED, "metadata": goal.metadata})

                # Check if ALL auto sub-goals are COMPLETED
                parent_id = goal.parent_goal_id
                if parent_id:
                    siblings = [g for g in goals
                                if g.parent_goal_id == parent_id
                                and g.metadata.get("source_type") == "auto"
                                and g.id != goal.id]
                    all_done = all(s.status == GoalStatus.COMPLETED for s in siblings)
                    if all_done:
                        logger.info("All auto sub-goals resolved → auto-validating %s", execution.execution_id)
                        await self.validate_completion(execution.execution_id)
                break

    async def _resolve_parent_auto_goal(self, execution: ProcedureExecution) -> None:
        """Mark parent auto-goal COMPLETED after successful validate_completion()."""
        if not self._session_goal_store or not execution.session_key or not execution.session_id:
            return
        goals = await self._session_goal_store.get_goals(execution.session_key, execution.session_id)
        for goal in goals:
            if (goal.metadata.get("source_type") == "auto"
                    and goal.metadata.get("source_system") == "procedure"
                    and goal.metadata.get("execution_id") == str(execution.execution_id)):
                goal.status = GoalStatus.COMPLETED
                goal.metadata["resolved_by_runtime"] = "true"
                await self._session_goal_store.update_goal(
                    execution.session_key, execution.session_id, goal.id, {"status": GoalStatus.COMPLETED, "metadata": goal.metadata})
                break
