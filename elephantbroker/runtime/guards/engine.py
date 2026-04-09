"""Red-line guard engine — 6-layer cheap-first safety enforcement (Phase 7 — §7.6)."""
from __future__ import annotations

import asyncio
import json
import logging
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime

from elephantbroker.runtime.interfaces.guard_engine import IRedLineGuardEngine
from elephantbroker.runtime.interfaces.trace_ledger import ITraceLedger
from elephantbroker.runtime.observability import VERBOSE, GatewayLoggerAdapter
from elephantbroker.schemas.config import GuardConfig, StrictnessPreset
from elephantbroker.schemas.context import AgentMessage, content_as_text
from elephantbroker.schemas.guards import (
    _OUTCOME_ORDER,
    AUTONOMY_TO_OUTCOME,
    ApprovalRequest,
    ApprovalStatus,
    AutonomyLevel,
    GuardActionType,
    GuardCheckInput,
    GuardEvent,
    GuardLayerResult,
    GuardOutcome,
    GuardResult,
    StaticRulePatternType,
    max_outcome,
)
from elephantbroker.schemas.profile import GuardPolicy
from elephantbroker.schemas.trace import TraceEvent, TraceEventType

logger = logging.getLogger(__name__)


class GuardRulesNotLoadedError(Exception):
    """Raised when preflight_check is called before guard rules are loaded."""
    def __init__(self, session_id):
        super().__init__(
            f"Guard rules not loaded for session {session_id}. "
            "Call POST /guards/refresh/{session_id} first."
        )


@dataclass
class _SessionGuardState:
    """Per-session guard state (in-memory)."""
    session_id: uuid.UUID
    session_key: str = ""
    agent_id: str = ""
    org_id: str = ""  # Phase 8: for org-specific profile resolution
    rule_registry: object = None  # StaticRuleRegistry
    semantic_index: object = None  # SemanticGuardIndex
    structural_validators: list = field(default_factory=list)
    guard_policy: GuardPolicy = field(default_factory=GuardPolicy)
    session_constraints: list[str] = field(default_factory=list)
    active_procedure_ids: list[uuid.UUID] = field(default_factory=list)
    active_procedure_domains: list[str] = field(default_factory=list)
    active_procedure_bindings: list[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    last_accessed_at: datetime = field(default_factory=lambda: datetime.now(UTC))


class RedLineGuardEngine(IRedLineGuardEngine):
    """6-layer guard pipeline: Autonomy → Static → Semantic → Structural → Reinjection → LLM."""

    def __init__(
        self,
        trace_ledger: ITraceLedger,
        embedding_service=None,
        graph=None,
        llm_client=None,
        profile_registry=None,
        redis=None,
        config: GuardConfig | None = None,
        gateway_id: str = "",
        redis_keys=None,
        metrics=None,
        hitl_client=None,
        approval_queue=None,
        autonomy_classifier=None,
        session_goal_store=None,
    ) -> None:
        self._trace = trace_ledger
        self._embed = embedding_service
        self._graph = graph
        self._llm = llm_client
        self._profiles = profile_registry
        self._redis = redis
        self._config = config or GuardConfig()
        self._gateway_id = gateway_id
        self._keys = redis_keys
        self._metrics = metrics
        self._hitl = hitl_client
        self._approvals = approval_queue
        self._autonomy = autonomy_classifier
        self._goals = session_goal_store
        self._sessions: dict[uuid.UUID, _SessionGuardState] = {}
        self._log = GatewayLoggerAdapter(logger, {"gateway_id": gateway_id})

    # ------------------------------------------------------------------
    # Interface methods
    # ------------------------------------------------------------------

    async def preflight_check(self, session_id: uuid.UUID, messages: list[AgentMessage]) -> GuardResult:
        if not self._config.enabled:
            return GuardResult(outcome=GuardOutcome.PASS)

        state = self._sessions.get(session_id)
        if state is None:
            self._log.warning("No guard state for session %s — rules not loaded", session_id)
            if self._metrics:
                # Label was "fallback_pass" in early docs; renamed to "rules_not_loaded"
                # to distinguish from a normal PASS outcome
                self._metrics.inc_guard_check("rules_not_loaded")
            raise GuardRulesNotLoadedError(session_id)

        t0 = time.monotonic()
        state.last_accessed_at = datetime.now(UTC)

        action = self._extract_check_input(session_id, messages)

        preset = self._config.strictness_presets.get(
            state.guard_policy.preflight_check_strictness,
            self._config.strictness_presets.get("medium", StrictnessPreset()),
        )

        layer_results: list[GuardLayerResult] = []
        definitive_result: GuardLayerResult | None = None
        autonomy_floor = GuardOutcome.PASS
        domain = "general"

        # --- Layer 0: Autonomy ---
        if self._autonomy:
            autonomy_floor, l0_result, domain = await self._layer0_autonomy(action, state)
            layer_results.append(l0_result)
            if l0_result.definitive:
                if l0_result.outcome == GuardOutcome.BLOCK:
                    return self._finalize(l0_result.outcome, layer_results, autonomy_floor, state, t0, action, domain)
                else:
                    definitive_result = l0_result
        else:
            layer_results.append(GuardLayerResult(layer=0, definitive=False, outcome=GuardOutcome.PASS))

        # --- Layers 1-3 (only if no definitive yet) ---
        if definitive_result is None:
            try:
                l1 = await self._layer1_static_rules(action, state, preset)
                layer_results.append(l1)
                if l1.definitive:
                    definitive_result = l1
            except Exception as exc:
                self._log.warning("Layer 1 failed: %s — skipping", exc)
                layer_results.append(GuardLayerResult(layer=1, definitive=False, outcome=GuardOutcome.PASS))

        if definitive_result is None:
            try:
                l2 = await self._layer2_cheap_semantic(action, state, preset)
                layer_results.append(l2)
                if l2.definitive:
                    definitive_result = l2
            except Exception as exc:
                self._log.warning("Layer 2 failed: %s — skipping", exc)
                layer_results.append(GuardLayerResult(layer=2, definitive=False, outcome=GuardOutcome.PASS))

        if definitive_result is None:
            try:
                l3 = await self._layer3_structural_validators(action, state, preset)
                layer_results.append(l3)
                if l3.definitive:
                    definitive_result = l3
            except Exception as exc:
                self._log.warning("Layer 3 failed: %s — skipping", exc)
                layer_results.append(GuardLayerResult(layer=3, definitive=False, outcome=GuardOutcome.PASS))

        # --- Layer 4: ALWAYS runs ---
        try:
            l4 = await self._layer4_forced_reinjection(action, state, preset, layer_results)
            layer_results.append(l4)
        except Exception as exc:
            self._log.warning("Layer 4 failed: %s", exc)

        # --- Layer 5: Only if no definitive from 1-3 ---
        if definitive_result is None:
            try:
                l5 = await self._layer5_llm_escalation(action, state, preset)
                layer_results.append(l5)
                if l5.definitive:
                    definitive_result = l5
            except Exception as exc:
                self._log.warning("Layer 5 failed: %s", exc)

        # --- GUARD-GAP-7: Near-miss escalation ---
        # If outcome so far is WARN (near-miss) and we've accumulated enough
        # recent WARN outcomes, force LLM escalation to get a definitive verdict.
        safety_result = definitive_result.outcome if definitive_result else GuardOutcome.PASS
        if safety_result == GuardOutcome.WARN:
            # BSR-9: Skip if Layer 5 already ran this check
            l5_already_ran = any(lr.layer == 5 for lr in layer_results)
            policy = state.guard_policy
            threshold = policy.near_miss_escalation_threshold
            window = policy.near_miss_window_turns
            recent_warns = await self._count_recent_near_misses(state, window)
            _esc_identity = {
                "session_key": state.session_key,
                "agent_key": f"{self._gateway_id}:{state.agent_id}" if state.agent_id else self._gateway_id,
                "session_id": state.session_id,
            }
            if recent_warns >= threshold:
                l5_esc = None
                escalation_outcome = "skipped"
                if l5_already_ran:
                    # BSR-9: Layer 5 already ran — don't waste another LLM call
                    self._log.info("Near-miss escalation: L5 already ran — skipping duplicate LLM call")
                elif policy.llm_escalation_enabled and self._llm:
                    self._log.info(
                        "Near-miss escalation: %d WARNs in last %d turns — forcing LLM check",
                        recent_warns, window,
                    )
                    if self._metrics:
                        self._metrics.inc_guard_near_miss_escalation()
                    try:
                        l5_esc = await self._layer5_llm_escalation(action, state, preset)
                        layer_results.append(l5_esc)
                        escalation_outcome = l5_esc.outcome.value
                        if l5_esc.definitive:
                            safety_result = l5_esc.outcome
                    except Exception as exc:
                        self._log.warning("Near-miss LLM escalation failed: %s", exc)
                        escalation_outcome = "error"
                else:
                    self._log.info(
                        "Near-miss escalation: %d WARNs in last %d turns but LLM disabled — keeping WARN",
                        recent_warns, window,
                    )
                    escalation_outcome = "llm_disabled"
                # TODO-11-005: Emit dedicated near-miss escalation trace
                if self._trace:
                    asyncio.create_task(self._trace.append_event(TraceEvent(
                        event_type=TraceEventType.GUARD_NEAR_MISS,
                        payload={
                            "action": "near_miss_escalation",
                            "recent_warns": recent_warns,
                            "threshold": threshold,
                            "window": window,
                            "escalation_attempted": not l5_already_ran and policy.llm_escalation_enabled,
                            "escalation_outcome": escalation_outcome,
                        },
                        **_esc_identity,
                    )))

        # --- Final composition ---
        final = max_outcome(autonomy_floor, safety_result)
        return self._finalize(final, layer_results, autonomy_floor, state, t0, action, domain)

    async def reinject_constraints(self, session_id: uuid.UUID) -> list[str]:
        state = self._sessions.get(session_id)
        if state is None:
            return []
        return list(state.session_constraints)

    async def get_guard_history(self, session_id: uuid.UUID) -> list[GuardEvent]:
        state = self._sessions.get(session_id)
        if state is None or not self._redis or not self._keys:
            return []
        try:
            key = self._keys.guard_history(state.session_key, str(session_id))
            raw_events = await self._redis.lrange(key, 0, 49)
            events = []
            for raw in raw_events:
                data = raw if isinstance(raw, str) else raw.decode()
                events.append(GuardEvent.model_validate_json(data))
            return events
        except Exception as exc:
            self._log.warning("Failed to read guard history: %s", exc)
            return []

    def _evict_stale_sessions(self) -> None:
        """Remove guard state for sessions not accessed within history_ttl_seconds."""
        ttl = self._config.history_ttl_seconds
        now = datetime.now(UTC)
        stale = [
            sid for sid, state in self._sessions.items()
            if (now - state.last_accessed_at).total_seconds() > ttl
        ]
        for sid in stale:
            del self._sessions[sid]
        if stale:
            self._log.info("Evicted %d stale guard session(s)", len(stale))

    async def load_session_rules(
        self,
        session_id: uuid.UUID,
        profile_name: str,
        active_procedure_ids: list[uuid.UUID] | None = None,
        *,
        session_key: str = "",
        agent_id: str = "",
        org_id: str = "",
    ) -> None:
        # Lazy eviction of stale sessions
        self._evict_stale_sessions()

        # Resolve profile policy (Phase 8: pass org_id for org-specific overrides)
        guard_policy = GuardPolicy()
        if self._profiles:
            try:
                profile = await self._profiles.resolve_profile(profile_name, org_id=org_id or None)
                guard_policy = profile.guards
            except Exception as exc:
                self._log.warning("Failed to resolve profile %s for guard rules: %s", profile_name, exc)

        # Import here to avoid circular at module level
        from elephantbroker.runtime.guards.rules import StaticRuleRegistry
        from elephantbroker.runtime.guards.semantic_index import SemanticGuardIndex

        rule_registry = StaticRuleRegistry()

        # Load procedure bindings from graph
        procedure_bindings: list[str] = []
        procedure_domains: list[str] = []
        if active_procedure_ids and self._graph:
            for pid in active_procedure_ids:
                try:
                    entity = await self._graph.get_entity(str(pid), gateway_id=self._gateway_id)
                    if entity:
                        bindings = entity.get("red_line_bindings_json") or entity.get("red_line_bindings")
                        if bindings:
                            if isinstance(bindings, str):
                                try:
                                    bindings = json.loads(bindings)
                                except (json.JSONDecodeError, TypeError):
                                    bindings = []
                            if isinstance(bindings, list):
                                procedure_bindings.extend(bindings)
                        domain = entity.get("decision_domain")
                        if domain:
                            procedure_domains.append(domain)
                except Exception as exc:
                    self._log.debug("Failed to load procedure %s bindings: %s", pid, exc)

        # Convert policy static_rules (list[object]) to StaticRule instances
        from elephantbroker.schemas.guards import StaticRule
        policy_rules = []
        for r in guard_policy.static_rules:
            if isinstance(r, StaticRule):
                policy_rules.append(r)
            elif isinstance(r, dict):
                try:
                    policy_rules.append(StaticRule(**r))
                except Exception as exc:
                    self._log.warning("Failed to coerce static_rule dict to StaticRule: %s — %s", r, exc)

        # GUARD-GAP-1: Skip builtin rules when config disables them
        builtin_override = [] if not self._config.builtin_rules_enabled else None
        rule_registry.load_rules(
            policy_rules=policy_rules,
            procedure_bindings=procedure_bindings if procedure_bindings else None,
            builtin_rules=builtin_override,
        )

        # Build semantic index (merge redline_exemplars + procedure bindings)
        semantic_index = SemanticGuardIndex(self._embed)
        all_exemplars = list(guard_policy.redline_exemplars)
        if procedure_bindings:
            all_exemplars.extend(procedure_bindings)
        if all_exemplars:
            await semantic_index.build_index(all_exemplars)

        # Build structural validators
        from elephantbroker.schemas.guards import StructuralValidatorSpec
        validators = []
        for v in guard_policy.structural_validators:
            if isinstance(v, StructuralValidatorSpec):
                validators.append(v)
            elif isinstance(v, dict):
                try:
                    validators.append(StructuralValidatorSpec(**v))
                except Exception as exc:
                    self._log.warning("Failed to coerce structural_validator dict to StructuralValidatorSpec: %s — %s", v, exc)

        # Store session state
        self._sessions[session_id] = _SessionGuardState(
            session_id=session_id,
            session_key=session_key,
            agent_id=agent_id,
            org_id=org_id,
            rule_registry=rule_registry,
            semantic_index=semantic_index,
            structural_validators=validators,
            guard_policy=guard_policy,
            session_constraints=[],
            active_procedure_ids=list(active_procedure_ids or []),
            active_procedure_domains=procedure_domains,
            active_procedure_bindings=procedure_bindings,
        )

        self._log.info(
            "Loaded guard rules for session %s: %d rules, %d exemplars, %d validators, %d bindings",
            session_id,
            len(rule_registry._rules),
            len(guard_policy.redline_exemplars),
            len(validators),
            len(procedure_bindings),
        )

    async def unload_session(self, session_id: uuid.UUID) -> None:
        state = self._sessions.pop(session_id, None)
        if state is None:
            return

        # Cancel pending approvals (uses CANCELLED status, not REJECTED)
        if self._approvals:
            try:
                pending = await self._approvals.get_for_session(session_id, state.agent_id)
                for req in pending:
                    if req.status == ApprovalStatus.PENDING:
                        await self._approvals.cancel(
                            req.id, state.agent_id,
                            reason="Session ended",
                            session_goal_store=self._goals,
                            session_key=state.session_key,
                        )
            except Exception as exc:
                self._log.debug("Failed to cancel pending approvals: %s", exc)

        self._log.info("Unloaded guard state for session %s", session_id)

    # ------------------------------------------------------------------
    # Layer implementations
    # ------------------------------------------------------------------

    async def _layer0_autonomy(
        self, action: GuardCheckInput, state: _SessionGuardState,
    ) -> tuple[GuardOutcome, GuardLayerResult, str]:
        t0 = time.monotonic()

        # Read recent fact domains from Redis for Tier 2 classification
        recent_domains: list[str] = []
        if self._redis and self._keys:
            try:
                key = self._keys.fact_domains(state.session_key, str(state.session_id))
                raw = await self._redis.lrange(key, 0, 19)
                recent_domains = [d.decode() if isinstance(d, bytes) else d for d in raw]
                if recent_domains:
                    await self._redis.expire(key, self._config.history_ttl_seconds)
            except Exception as exc:
                self._log.log(VERBOSE, "L0: Redis fact_domains read failed: %s", exc)

        # Classify domain via 3-tier hybrid
        domain = self._autonomy.classify_domain(
            action,
            active_procedure_domains=state.active_procedure_domains,
            recent_fact_domains=recent_domains,
        )

        # Resolve autonomy level from profile policy
        autonomy_level = self._autonomy.resolve_autonomy(domain, state.guard_policy.autonomy)
        autonomy_floor = AUTONOMY_TO_OUTCOME[autonomy_level]

        # Metrics
        if self._metrics:
            self._metrics.inc_autonomy_classification(domain, autonomy_level.value)
            self._metrics.inc_autonomy_domain_tier(self._autonomy._last_source)

        duration_ms = (time.monotonic() - t0) * 1000

        if autonomy_level == AutonomyLevel.HARD_STOP:
            if self._metrics:
                self._metrics.inc_autonomy_hard_stop(domain)
            result = GuardLayerResult(
                layer=0, definitive=True, outcome=GuardOutcome.BLOCK,
                explanation=f"HARD_STOP: domain '{domain}' is prohibited for this profile",
                confidence=1.0,
            )
            self._log.info("L0: HARD_STOP domain=%s → BLOCK (%.1fms)", domain, duration_ms)
            return GuardOutcome.BLOCK, result, domain

        if autonomy_level == AutonomyLevel.APPROVE_FIRST:
            existing = None
            if self._approvals:
                existing = await self._approvals.find_matching(
                    state.session_id, action.action_content, state.agent_id,
                )

            if existing:
                if existing.status == ApprovalStatus.APPROVED:
                    self._log.info("L0: APPROVE_FIRST domain=%s, existing APPROVED → floor=PASS", domain)
                    result = GuardLayerResult(
                        layer=0, definitive=False, outcome=GuardOutcome.PASS,
                        explanation=f"Approved: {existing.approval_message or 'no message'}",
                    )
                    return GuardOutcome.PASS, result, domain
                elif existing.status == ApprovalStatus.REJECTED:
                    result = GuardLayerResult(
                        layer=0, definitive=True, outcome=GuardOutcome.BLOCK,
                        explanation=f"Approval rejected: {existing.rejection_reason or 'no reason'}",
                    )
                    return GuardOutcome.BLOCK, result, domain
                elif existing.status == ApprovalStatus.PENDING:
                    result = GuardLayerResult(
                        layer=0, definitive=True, outcome=GuardOutcome.REQUIRE_APPROVAL,
                        explanation=f"Waiting for approval (request: {existing.id})",
                    )
                    return GuardOutcome.REQUIRE_APPROVAL, result, domain
                elif existing.status == ApprovalStatus.TIMED_OUT:
                    timeout_action = (state.guard_policy.approval_routing.timeout_action
                                      if state.guard_policy.approval_routing else AutonomyLevel.HARD_STOP)
                    mapped = AUTONOMY_TO_OUTCOME.get(timeout_action, GuardOutcome.BLOCK)
                    result = GuardLayerResult(
                        layer=0, definitive=True, outcome=mapped,
                        explanation=f"Approval timed out, timeout_action={timeout_action.value}",
                    )
                    return mapped, result, domain
            else:
                # No existing request — create new
                guard_event = GuardEvent(
                    session_id=state.session_id,
                    input_summary=(
                        f"{action.action_target or ''}: {action.action_content}"
                        [:self._config.input_summary_max_chars]
                    ),
                    outcome=GuardOutcome.REQUIRE_APPROVAL,
                )
                new_req = ApprovalRequest(
                    guard_event_id=guard_event.id,
                    session_id=state.session_id,
                    action_summary=action.action_content[:500],
                    decision_domain=domain,
                    autonomy_level=autonomy_level,
                )
                if self._approvals:
                    await self._approvals.create(
                        new_req, state.agent_id,
                        session_goal_store=self._goals,
                        session_key=state.session_key,
                        session_id=state.session_id,
                    )
                if self._hitl:
                    from elephantbroker.runtime.guards.hitl_client import ApprovalIntent
                    intent = ApprovalIntent(
                        request_id=new_req.id,
                        guard_event_id=guard_event.id,
                        session_id=state.session_id,
                        session_key=state.session_key,
                        gateway_id=self._gateway_id,
                        agent_key=f"{self._gateway_id}:{state.agent_id}",
                        action_summary=action.action_content[:500],
                        decision_domain=domain,
                        matched_rules=[],
                        explanation="Requires approval per autonomy policy",
                        timeout_seconds=(state.guard_policy.approval_routing.timeout_seconds
                                         if state.guard_policy.approval_routing else 300),
                    )
                    await self._hitl.request_approval(intent)
                if self._metrics:
                    self._metrics.inc_approval_requested(domain)
                result = GuardLayerResult(
                    layer=0, definitive=True, outcome=GuardOutcome.REQUIRE_APPROVAL,
                    explanation=f"Approval required for domain '{domain}' (request: {new_req.id})",
                )
                self._log.info("L0: APPROVE_FIRST domain=%s → new approval request %s", domain, new_req.id)
                return GuardOutcome.REQUIRE_APPROVAL, result, domain

        # INFORM or AUTONOMOUS
        result = GuardLayerResult(
            layer=0, definitive=False, outcome=autonomy_floor,
            explanation=f"Autonomy: {autonomy_level.value} for domain '{domain}'",
        )
        self._log.log(VERBOSE, "L0: domain=%s autonomy=%s floor=%s (%.1fms)",
                       domain, autonomy_level, autonomy_floor, duration_ms)
        return autonomy_floor, result, domain

    async def _layer1_static_rules(
        self, action: GuardCheckInput, state: _SessionGuardState, preset: StrictnessPreset,
    ) -> GuardLayerResult:
        t0 = time.monotonic()
        matches = state.rule_registry.match(action)

        if not matches:
            self._log.log(VERBOSE, "L1: 0 rules matched (%.1fms)", (time.monotonic() - t0) * 1000)
            return GuardLayerResult(layer=1, definitive=False, outcome=GuardOutcome.PASS)

        worst = max(matches, key=lambda m: _OUTCOME_ORDER.get(m.rule.outcome.value, 0))
        matched_names = [m.rule.id for m in matches]

        if self._metrics:
            self._metrics.inc_guard_layer_triggered("layer_1")

        if worst.rule.outcome in (GuardOutcome.BLOCK, GuardOutcome.REQUIRE_APPROVAL, GuardOutcome.REQUIRE_EVIDENCE):
            self._log.info("L1: %d rules matched, worst=%s rule=%s (%.1fms)",
                           len(matches), worst.rule.outcome.value, worst.rule.id,
                           (time.monotonic() - t0) * 1000)
            return GuardLayerResult(
                layer=1, definitive=True, outcome=worst.rule.outcome,
                matched_rules=matched_names,
                explanation=f"Rule '{worst.rule.id}': {worst.rule.description}",
                confidence=worst.confidence,
            )

        if worst.rule.outcome == GuardOutcome.WARN:
            effective_outcome = GuardOutcome.WARN
            if preset.warn_outcome_upgrade:
                try:
                    effective_outcome = GuardOutcome(preset.warn_outcome_upgrade)
                except ValueError:
                    pass
            definitive = effective_outcome in (GuardOutcome.BLOCK, GuardOutcome.REQUIRE_APPROVAL)
            self._log.log(VERBOSE, "L1: WARN matched, effective=%s (strictness upgrade=%s)",
                          effective_outcome, preset.warn_outcome_upgrade)
            return GuardLayerResult(
                layer=1, definitive=definitive, outcome=effective_outcome,
                matched_rules=matched_names,
                explanation=f"Rule '{worst.rule.id}': {worst.rule.description}",
            )

        return GuardLayerResult(layer=1, definitive=False, outcome=GuardOutcome.PASS)

    async def _layer2_cheap_semantic(
        self, action: GuardCheckInput, state: _SessionGuardState, preset: StrictnessPreset,
    ) -> GuardLayerResult:
        if not state.semantic_index or not state.semantic_index._exemplar_texts:
            return GuardLayerResult(layer=2, definitive=False, outcome=GuardOutcome.PASS)

        t0 = time.monotonic()
        effective_bm25_block = state.guard_policy.bm25_block_threshold * preset.bm25_threshold_multiplier
        effective_bm25_warn = state.guard_policy.bm25_warn_threshold * preset.bm25_threshold_multiplier
        effective_semantic = preset.semantic_threshold_override or state.guard_policy.semantic_similarity_threshold

        bm25_scores = state.semantic_index.score_bm25(action.action_content)
        if bm25_scores:
            top_exemplar, top_score = bm25_scores[0]
            if self._metrics:
                self._metrics.observe_guard_bm25_score(top_score)

            if top_score >= effective_bm25_block:
                if self._metrics:
                    self._metrics.inc_guard_bm25_short_circuit()
                    self._metrics.inc_guard_layer_triggered("layer_2")
                self._log.info("L2: BM25 BLOCK score=%.2f threshold=%.2f (%.1fms)",
                               top_score, effective_bm25_block, (time.monotonic() - t0) * 1000)
                return GuardLayerResult(
                    layer=2, definitive=True, outcome=GuardOutcome.BLOCK,
                    matched_rules=[f"bm25:{top_exemplar[:50]}"],
                    explanation=f"BM25 match (score={top_score:.2f}): '{top_exemplar[:100]}'",
                    confidence=min(top_score / effective_bm25_block, 1.0),
                )

            if top_score >= effective_bm25_warn:
                self._log.log(VERBOSE, "L2: BM25 WARN score=%.2f", top_score)
                if self._metrics:
                    self._metrics.inc_guard_near_miss()

        # Semantic similarity (1 embedding call)
        try:
            semantic_matches = await state.semantic_index.check_similarity(action.action_content, effective_semantic)
        except Exception as exc:
            self._log.warning("L2: Semantic check failed: %s — falling back to BM25 only", exc)
            semantic_matches = []

        if semantic_matches:
            top = semantic_matches[0]
            if self._metrics:
                self._metrics.observe_guard_semantic_score(top.similarity)
                self._metrics.inc_guard_layer_triggered("layer_2")
            self._log.info("L2: Semantic BLOCK sim=%.3f threshold=%.2f (%.1fms)",
                           top.similarity, effective_semantic, (time.monotonic() - t0) * 1000)
            return GuardLayerResult(
                layer=2, definitive=True, outcome=GuardOutcome.BLOCK,
                matched_rules=[f"semantic:{top.exemplar_text[:50]}"],
                explanation=f"Semantic match (sim={top.similarity:.3f}): '{top.exemplar_text[:100]}'",
                confidence=top.similarity,
            )

        self._log.log(VERBOSE, "L2: No matches above threshold (%.1fms)", (time.monotonic() - t0) * 1000)
        return GuardLayerResult(layer=2, definitive=False, outcome=GuardOutcome.PASS)

    async def _layer3_structural_validators(
        self, action: GuardCheckInput, state: _SessionGuardState, preset: StrictnessPreset,
    ) -> GuardLayerResult:
        if not preset.structural_validators_enabled:
            return GuardLayerResult(layer=3, definitive=False, outcome=GuardOutcome.PASS)

        t0 = time.monotonic()
        checked = 0
        for validator in state.structural_validators:
            if not validator.enabled:
                continue
            if validator.action_type != action.action_type:
                continue
            if validator.action_target_pattern:
                if not re.match(validator.action_target_pattern, action.action_target or "", re.IGNORECASE):
                    continue
            checked += 1
            for field_name in validator.required_fields:
                if field_name not in action.action_metadata:
                    if self._metrics:
                        self._metrics.inc_guard_layer_triggered("layer_3")
                    self._log.info("L3: Validator '%s' failed — missing field '%s' (%.1fms)",
                                   validator.id, field_name, (time.monotonic() - t0) * 1000)
                    return GuardLayerResult(
                        layer=3, definitive=True, outcome=validator.outcome_on_fail,
                        matched_rules=[validator.id],
                        explanation=f"Missing required field '{field_name}' for {validator.description}",
                    )

        self._log.log(VERBOSE, "L3: %d validators checked, all passed (%.1fms)",
                       checked, (time.monotonic() - t0) * 1000)
        return GuardLayerResult(layer=3, definitive=False, outcome=GuardOutcome.PASS)

    async def _layer4_forced_reinjection(
        self, action: GuardCheckInput, state: _SessionGuardState,
        preset: StrictnessPreset, layer_results: list[GuardLayerResult],
    ) -> GuardLayerResult:
        has_non_pass = any(lr.outcome != GuardOutcome.PASS for lr in layer_results)
        has_elevated = any(lr.outcome.value in ("block", "require_approval", "require_evidence", "warn")
                           for lr in layer_results)

        should_reinject = False
        if preset.reinjection_on == "any_non_pass":
            should_reinject = has_non_pass
        elif preset.reinjection_on == "elevated_risk":
            should_reinject = has_elevated
        elif preset.reinjection_on == "block_only":
            should_reinject = any(lr.outcome.value in ("block", "require_approval", "require_evidence")
                                  for lr in layer_results)

        constraints: list[str] = []
        if should_reinject or state.guard_policy.force_system_constraint_injection:
            constraints = self._build_reinjection_constraints(state, layer_results)
            # PERSISTENT: Pending approval status (async — done here, not in sync _build)
            if self._approvals:
                try:
                    pending = [r for r in await self._approvals.get_for_session(state.session_id, state.agent_id)
                               if r.status == ApprovalStatus.PENDING]
                    for req in pending:
                        constraints.append(
                            f"PENDING APPROVAL: {req.action_summary[:80]} "
                            f"(guard: {req.guard_event_id}, status: {req.status.value}). "
                            f"Check with guard_status('{req.guard_event_id}').")
                except Exception as exc:
                    self._log.warning("Failed to check pending approvals for reinjection: %s", exc)
            state.session_constraints = constraints
            if self._metrics and constraints:
                self._metrics.inc_guard_reinjection()

        self._log.log(VERBOSE, "L4: %d constraints reinjected (should=%s)", len(constraints), should_reinject)

        # Emit CONSTRAINT_REINJECTED trace event when constraints are generated
        if constraints and state.session_key:
            asyncio.create_task(self._trace.append_event(TraceEvent(
                event_type=TraceEventType.CONSTRAINT_REINJECTED,
                session_key=state.session_key,
                session_id=state.session_id,
                gateway_id=self._gateway_id,
                payload={"count": len(constraints), "trigger": "force" if state.guard_policy.force_system_constraint_injection else preset.reinjection_on},
            )))

        return GuardLayerResult(
            layer=4, definitive=False, outcome=GuardOutcome.PASS,
            explanation=f"{len(constraints)} constraints reinjected",
        )

    def _build_reinjection_constraints(
        self, state: _SessionGuardState, layer_results: list[GuardLayerResult],
    ) -> list[str]:
        constraints: list[str] = []

        # PERSISTENT: TOOL_TARGET rule summaries
        for rule in state.rule_registry._rules:
            if rule.pattern_type == StaticRulePatternType.TOOL_TARGET and rule.enabled:
                constraints.append(f"TOOL RESTRICTION: {rule.pattern} — {rule.description} ({rule.outcome.value})")

        # PERSISTENT: Active procedure bindings
        for binding in state.active_procedure_bindings:
            constraints.append(f"PROCEDURE RULE: {binding} (from active procedure)")

        # TRIGGER: Latest check results
        for lr in layer_results:
            if lr.outcome != GuardOutcome.PASS and lr.explanation:
                constraints.append(f"GUARD ({lr.outcome.value}): {lr.explanation}")

        return constraints

    async def _layer5_llm_escalation(
        self, action: GuardCheckInput, state: _SessionGuardState, preset: StrictnessPreset,
    ) -> GuardLayerResult:
        if not state.guard_policy.llm_escalation_enabled:
            return GuardLayerResult(layer=5, definitive=False, outcome=GuardOutcome.PASS)
        if preset.llm_escalation_on == "disabled":
            return GuardLayerResult(layer=5, definitive=False, outcome=GuardOutcome.PASS)
        if not self._llm:
            return GuardLayerResult(layer=5, definitive=False, outcome=GuardOutcome.PASS)

        t0 = time.monotonic()
        if self._metrics:
            self._metrics.inc_guard_llm_escalation()

        rule_summaries = "\n".join(
            f"- [{r.pattern_type.value}] {r.pattern}: {r.description} → {r.outcome.value}"
            for r in state.rule_registry._rules[:20] if r.enabled
        )

        system_prompt = "You are a safety evaluator for an AI agent system. Respond with JSON only."
        user_prompt = (
            f"The agent is attempting the following action:\n\n"
            f"Action type: {action.action_type.value}\n"
            f"Action target: {action.action_target or 'none'}\n"
            f"Action content: {action.action_content[:500]}\n"
            f"Action metadata: {json.dumps(dict(action.action_metadata))[:200]}\n\n"
            f"Active safety rules:\n{rule_summaries}\n\n"
            f"Determine:\n1. Does this action violate any active constraints?\n"
            f"2. If yes, should it be BLOCKED or does it REQUIRE_APPROVAL?\n"
            f"3. If no, is it safe to PASS?\n\n"
            f'Respond with JSON: {{"outcome": "pass" | "block" | "require_approval", "explanation": "brief reason"}}'
        )

        try:
            response = await asyncio.wait_for(
                self._llm.complete_json(system_prompt, user_prompt,
                                        max_tokens=self._config.llm_escalation_max_tokens),
                timeout=self._config.llm_escalation_timeout_seconds,
            )
            outcome_str = response.get("outcome", "block").lower().strip()
            explanation = response.get("explanation", "LLM judgment")
            try:
                outcome = GuardOutcome(outcome_str)
            except ValueError:
                outcome = GuardOutcome.BLOCK
            self._log.warning("L5: LLM escalation outcome=%s (%.1fms)",
                              outcome, (time.monotonic() - t0) * 1000)
            return GuardLayerResult(
                layer=5, definitive=True, outcome=outcome,
                explanation=f"LLM: {explanation[:200]}", confidence=0.8,
            )
        except TimeoutError:
            self._log.warning("L5: LLM escalation timeout → BLOCK (fail-closed)")
            return GuardLayerResult(
                layer=5, definitive=True, outcome=GuardOutcome.BLOCK,
                explanation="LLM escalation timeout — fail-closed", confidence=0.5,
            )
        except Exception as exc:
            self._log.warning("L5: LLM escalation error: %s → BLOCK (fail-closed)", exc)
            return GuardLayerResult(
                layer=5, definitive=True, outcome=GuardOutcome.BLOCK,
                explanation=f"LLM escalation error: {exc}", confidence=0.5,
            )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _extract_check_input(self, session_id: uuid.UUID, messages: list[AgentMessage]) -> GuardCheckInput:
        """Extract guard check input from the most recent relevant message."""
        for msg in reversed(messages):
            if msg.metadata.get("tool_name"):
                metadata = dict(msg.metadata)
                if "tool_args" in metadata:
                    try:
                        parsed = json.loads(metadata["tool_args"])
                        if isinstance(parsed, dict):
                            metadata.update(parsed)
                    except (json.JSONDecodeError, TypeError):
                        pass
                return GuardCheckInput(
                    session_id=session_id,
                    action_type=GuardActionType.TOOL_CALL,
                    action_target=msg.metadata["tool_name"],
                    action_content=content_as_text(msg)[:1000],
                    action_metadata=metadata,
                )
            if msg.metadata.get("claim_id"):
                return GuardCheckInput(
                    session_id=session_id,
                    action_type=GuardActionType.COMPLETION_CLAIM,
                    action_content=content_as_text(msg)[:1000],
                    action_metadata=dict(msg.metadata),
                )

        for msg in reversed(messages):
            if msg.role == "assistant":
                return GuardCheckInput(
                    session_id=session_id,
                    action_type=GuardActionType.MESSAGE_SEND,
                    action_content=content_as_text(msg)[:1000],
                    action_metadata=dict(msg.metadata),
                )

        combined = " ".join(content_as_text(m) for m in messages[-3:])
        return GuardCheckInput(
            session_id=session_id,
            action_type=GuardActionType.MESSAGE_SEND,
            action_content=combined[:1000],
        )

    def _finalize(
        self,
        outcome: GuardOutcome,
        layer_results: list[GuardLayerResult],
        autonomy_floor: GuardOutcome,
        state: _SessionGuardState,
        t0: float,
        action: GuardCheckInput | None = None,
        domain: str = "general",
    ) -> GuardResult:
        total_ms = (time.monotonic() - t0) * 1000
        triggered_layer = next((lr.layer for lr in layer_results if lr.definitive), None)
        matched_rules: list[str] = []
        explanation = ""
        for lr in layer_results:
            matched_rules.extend(lr.matched_rules)
            if lr.definitive:
                explanation = lr.explanation

        result = GuardResult(
            outcome=outcome,
            triggered_layer=triggered_layer,
            matched_rules=matched_rules,
            explanation=explanation,
            layer_results=layer_results,
            constraints_reinjected=state.session_constraints,
        )

        # Pre-generate guard event ID so HITL notify and stored event share it
        guard_event_id = uuid.uuid4()

        # Store GuardEvent in Redis
        asyncio.create_task(self._store_guard_event(state, result, action, domain, autonomy_floor.value, event_id=guard_event_id))

        # Emit trace — always include identity fields per GW-ID rules
        _trace_identity = {
            "session_key": state.session_key,
            "agent_key": f"{self._gateway_id}:{state.agent_id}" if state.agent_id else self._gateway_id,
            "session_id": state.session_id,
        }
        if outcome in (GuardOutcome.BLOCK, GuardOutcome.REQUIRE_APPROVAL, GuardOutcome.REQUIRE_EVIDENCE):
            asyncio.create_task(self._trace.append_event(TraceEvent(
                event_type=TraceEventType.GUARD_TRIGGERED,
                payload={"outcome": outcome.value, "layer": triggered_layer,
                         "rules": matched_rules, "decision_domain": domain,
                         "action_target": (action.action_target if action else None)},
                **_trace_identity,
            )))
        elif outcome == GuardOutcome.WARN:
            asyncio.create_task(self._trace.append_event(TraceEvent(
                event_type=TraceEventType.GUARD_NEAR_MISS,
                payload={"outcome": outcome.value, "rules": matched_rules, "decision_domain": domain},
                **_trace_identity,
            )))
        else:
            asyncio.create_task(self._trace.append_event(TraceEvent(
                event_type=TraceEventType.GUARD_PASSED,
                payload={"layers_checked": len(layer_results)},
                **_trace_identity,
            )))

        # HITL notification for INFORM/WARN outcomes
        if self._hitl and outcome in (GuardOutcome.INFORM, GuardOutcome.WARN):
            from elephantbroker.runtime.guards.hitl_client import NotificationIntent
            asyncio.create_task(self._hitl.notify(NotificationIntent(
                guard_event_id=guard_event_id,
                session_id=state.session_id,
                session_key=state.session_key,
                gateway_id=self._gateway_id,
                agent_key=f"{self._gateway_id}:{state.agent_id}" if state.agent_id else self._gateway_id,
                action_summary=(action.action_content[:500] if action else ""),
                decision_domain=domain,
                outcome=outcome.value,
                matched_rules=matched_rules,
                explanation=explanation,
            )))

        if self._metrics:
            self._metrics.inc_guard_check(outcome.value)
            self._metrics.observe_guard_latency(total_ms / 1000.0)

        self._log.info("Guard check: outcome=%s triggered_layer=%s latency=%.1fms",
                        outcome.value, triggered_layer, total_ms)
        return result

    async def _store_guard_event(
        self,
        state: _SessionGuardState,
        result: GuardResult,
        action: GuardCheckInput | None = None,
        domain: str = "general",
        autonomy_level: str | None = None,
        event_id: uuid.UUID | None = None,
    ) -> None:
        if not self._redis or not self._keys:
            return
        try:
            event = GuardEvent(
                id=event_id or uuid.uuid4(),
                session_id=state.session_id,
                input_summary=(f"{action.action_target or ''}: {action.action_content}"
                               [:self._config.input_summary_max_chars] if action else ""),
                outcome=result.outcome,
                triggered_layer=result.triggered_layer,
                matched_rules=result.matched_rules,
                explanation=result.explanation,
                action_target=action.action_target if action else None,
                decision_domain=domain,
                autonomy_level=autonomy_level,
            )
            key = self._keys.guard_history(state.session_key, str(state.session_id))
            await self._redis.lpush(key, event.model_dump_json())
            max_events = getattr(self._config, "max_history_events", 50)
            await self._redis.ltrim(key, 0, max_events - 1)
            await self._redis.expire(key, self._config.history_ttl_seconds)
        except Exception as exc:
            self._log.debug("Failed to store guard event: %s", exc)

    async def _count_recent_near_misses(self, state: _SessionGuardState, window: int) -> int:
        """Count WARN outcomes in the most recent *window* guard events from Redis."""
        if not self._redis or not self._keys:
            return 0
        try:
            key = self._keys.guard_history(state.session_key, str(state.session_id))
            # Guard history is newest-first (lpush). Read the last `window` events.
            raw_events = await self._redis.lrange(key, 0, window - 1)
            count = 0
            for raw in raw_events:
                data = raw if isinstance(raw, str) else raw.decode()
                event = GuardEvent.model_validate_json(data)
                if event.outcome == GuardOutcome.WARN:
                    count += 1
            return count
        except Exception as exc:
            self._log.debug("Failed to count near misses: %s", exc)
            return 0
