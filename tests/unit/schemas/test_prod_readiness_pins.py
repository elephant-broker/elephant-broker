"""TF-FN-019 G1-G10 (schema PROD-risk pins) — post-R2-P2 mixed state.

Post R2-P2 schema-validator batch, the fix state of each pin is:

RESOLVED (pin test flipped to assert validator):
* G1 #1135 — ApprovalRequest honours routing.timeout_seconds (default 300)
* G2 #1136 — effective_short_name renamed to effective_short_name_or_id;
  documented semantics + new effective_short_name_padded for fixed-width
* G4 #1140 — max_outcome raises TypeError on non-GuardOutcome inputs
* G6 #1147 — ScoringWeights penalty fields reject positive values
* G10 #1141 — decay_scope_multipliers includes all 8 Scope values

STILL PINNED (documents current behavior; fix deferred):
* G3 #1166 — ``MemoryStoreFacade.search(min_score=...)`` parameter is DEAD
* G5 #1184 — ``facade.decay(factor>1.0)`` can INCREASE confidence (clamped at 1.0)

POST-R2-P2.1 (#1146 now also RESOLVED):
* G7 #1146 — ProcedureDefinition now requires activation_modes OR is_manual_only
  (R2-P2.1 added is_manual_only flag + model_validator; reconstruction
  auto-infers is_manual_only=True for legacy data; G7 pin flipped to assert
  post-fix contract)
"""
from __future__ import annotations

import uuid
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest

from elephantbroker.runtime.memory.facade import MemoryStoreFacade
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.schemas.base import Scope
from elephantbroker.schemas.config import GatewayConfig
from elephantbroker.schemas.consolidation import ConsolidationConfig
from elephantbroker.schemas.guards import (
    ApprovalRequest, AutonomyLevel, GuardOutcome, max_outcome,
)
from elephantbroker.schemas.procedure import ProcedureDefinition
from elephantbroker.schemas.working_set import ScoringWeights
from tests.fixtures.factories import make_fact_assertion


# ---------------------------------------------------------------------------
# G1 #1135 — approval request hardcodes 300s timeout
# ---------------------------------------------------------------------------

def test_approval_request_honours_routing_timeout_seconds():
    """G1 (#1135 RESOLVED — R2-P2): ``ApprovalRequest`` now accepts a
    ``timeout_seconds`` kwarg (threaded from
    ``state.guard_policy.approval_routing.timeout_seconds`` in the guards
    engine). The prior hardcoded 300s is now the field DEFAULT — callers
    can pass policy-resolved values and ``timeout_at`` reflects them.

    Fix at ``schemas/guards.py:230-254`` (field + model_post_init) and
    ``runtime/guards/engine.py:528-542`` (caller passes routing timeout).

    Assertions:
      (1) Default (no timeout_seconds kwarg) -> 300s (preserves prior
          serialization shape; prior tests don't break).
      (2) Explicit timeout_seconds=600 -> 600s (the routing-configured
          value takes effect).
      (3) Below-floor timeout_seconds=10 raises ValidationError (field
          has ge=30 matching ApprovalRouting's constraint).
    """
    created = datetime(2026, 4, 25, 12, 0, 0, tzinfo=UTC)
    # (1) Default
    req_default = ApprovalRequest(created_at=created)
    assert req_default.timeout_at - created == timedelta(seconds=300)
    # (2) Routing-resolved value takes effect
    req_600 = ApprovalRequest(created_at=created, timeout_seconds=600)
    assert req_600.timeout_at - created == timedelta(seconds=600)
    # (3) Floor constraint rejects too-small values
    from pydantic import ValidationError
    with pytest.raises(ValidationError):
        ApprovalRequest(created_at=created, timeout_seconds=10)


# ---------------------------------------------------------------------------
# G2 #1136 — effective_short_name semantics on short gateway_ids
# ---------------------------------------------------------------------------

def test_effective_short_name_or_id_contract_and_padded_alternative():
    """G2 (#1136 RESOLVED — R2-P2): ``GatewayConfig.effective_short_name``
    was renamed to ``effective_short_name_or_id`` to make the "short_name
    if set, else gateway_id[:8]" semantics explicit. No padding is applied
    — a 3-char gateway_id yields a 3-char result.

    A new ``effective_short_name_padded`` property provides space-padded
    fixed-width output for callers that need column-aligned log / metric
    labels. This test pins both contracts.

    Fix at ``schemas/config.py:333-362`` (rename + new padded property).

    Historic: the prior name misled operators into expecting fixed-width
    truncation. The rename is intentional — no ``effective_short_name``
    alias is kept. Existing callers were updated in the same commit.
    """
    # Short gateway_id -> raw, no padding.
    cfg = GatewayConfig(gateway_id="abc")
    assert cfg.effective_short_name_or_id == "abc"
    assert cfg.effective_short_name_padded == "abc     "  # 5-char trailing space
    # Long gateway_id -> truncated to exactly 8.
    cfg_long = GatewayConfig(gateway_id="very-long-gateway-id")
    assert cfg_long.effective_short_name_or_id == "very-lon"
    assert cfg_long.effective_short_name_padded == "very-lon"  # already 8-wide
    # Explicit override wins regardless of length; padded still 8-wide.
    cfg_explicit = GatewayConfig(gateway_id="very-long-gateway-id", gateway_short_name="X")
    assert cfg_explicit.effective_short_name_or_id == "X"
    assert cfg_explicit.effective_short_name_padded == "X       "
    # The old name is removed — access raises AttributeError (rename, not alias).
    assert not hasattr(cfg, "effective_short_name")


# ---------------------------------------------------------------------------
# G3 #1166 — facade.search(min_score=...) parameter is DEAD
# ---------------------------------------------------------------------------

async def test_facade_search_min_score_logged_but_ignored_post_R2P9_fix(caplog):
    """G3 FLIPPED (#1166 RESOLVED — R2-P9): ``MemoryStoreFacade.search()``
    still accepts ``min_score`` (facade fallback path has no profile-
    driven scoring framework — that lives behind
    ``RetrievalOrchestrator``), but a non-zero ``min_score`` now emits a
    one-shot WARNING log directing the caller to pass ``profile_name``
    for real filtering.

    Pre-fix: the parameter was silently ignored — callers passing 0.99
    got the same results as callers passing 0.0 with no diagnostic.
    Post-fix: the warning surfaces the gap once per facade instance
    (flag-protected to avoid log flood under high-throughput callers).
    Subsequent calls with ``min_score>0`` from the same facade are
    silent.
    """
    import logging
    graph = AsyncMock()
    vector = AsyncMock()
    embeddings = AsyncMock()
    embeddings.embed_text = AsyncMock(return_value=[0.1] * 1024)
    facade = MemoryStoreFacade(
        graph, vector, embeddings, TraceLedger(), dataset_name="t",
    )
    fact = make_fact_assertion()
    graph.query_cypher = AsyncMock(return_value=[{
        "props": {
            "eb_id": str(fact.id), "text": fact.text, "category": "general",
            "scope": "session", "confidence": 1.0, "eb_created_at": 0,
            "eb_updated_at": 0, "use_count": 0, "successful_use_count": 0,
            "provenance_refs": [], "target_actor_ids": [], "goal_ids": [],
        },
    }])
    with caplog.at_level(logging.WARNING, logger="elephantbroker.memory.facade"):
        high = await facade.search("q", scope=Scope.SESSION, min_score=0.99)
        # Second call with min_score>0 must NOT re-warn (once-per-instance).
        caplog.clear()
        await facade.search("q", scope=Scope.SESSION, min_score=0.5)
        # min_score=0 path also stays silent.
        await facade.search("q", scope=Scope.SESSION, min_score=0.0)

    # Filter to ONLY min_score-related warnings — the facade emits other
    # unrelated WARNs (e.g., "Batch fetch of cognee_data_ids failed" from
    # the search() consumer when the mock dataset can't fulfill the
    # batch-fetch path) which would falsely fail a blanket
    # `new_warnings == []` assertion. The once-per-instance flag we're
    # pinning is specifically about the min_score gate, not about
    # silencing every WARN emitted from the facade module.
    new_min_score_warnings = [
        r for r in caplog.records
        if r.levelno == logging.WARNING and "min_score" in r.getMessage().lower()
    ]
    assert new_min_score_warnings == [], (
        f"Expected silent subsequent calls (min_score-related only); "
        f"got {len(new_min_score_warnings)} new min_score WARNs: "
        f"{[r.getMessage() for r in new_min_score_warnings]}"
    )
    # The instance flag is set after the first non-zero call.
    assert getattr(facade, "_min_score_warned", False) is True
    # The first call's warning must contain the documented hint.
    # (caplog.clear was called between calls; check the original message
    # via the facade's log adapter is hard to retrieve, so we just verify
    # the flag was flipped which proves the WARN branch ran.)


async def test_facade_search_min_score_warns_on_first_nonzero_call_post_R2P9_fix(caplog):
    """G3-bis (R2-P9): the first call with ``min_score>0`` against a
    fresh facade emits the documented WARN. Pins the message contents
    so a future log refactor that drops the ``profile_name`` hint
    surfaces here.
    """
    import logging
    graph = AsyncMock()
    vector = AsyncMock()
    embeddings = AsyncMock()
    embeddings.embed_text = AsyncMock(return_value=[0.1] * 1024)
    facade = MemoryStoreFacade(
        graph, vector, embeddings, TraceLedger(), dataset_name="t",
    )
    graph.query_cypher = AsyncMock(return_value=[])

    with caplog.at_level(logging.WARNING, logger="elephantbroker.memory.facade"):
        await facade.search("q", scope=Scope.SESSION, min_score=0.7)

    msgs = [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]
    assert any("min_score=0.70 ignored" in m for m in msgs), (
        f"Expected min_score=0.70 ignored warning; got: {msgs}"
    )
    assert any("Pass profile_name to enable RetrievalOrchestrator" in m for m in msgs)


# ---------------------------------------------------------------------------
# G4 #1140 — max_outcome accepts strings with .value silently
# ---------------------------------------------------------------------------

def test_max_outcome_rejects_non_guardoutcome_inputs():
    """G4 (#1140 RESOLVED — R2-P2): ``max_outcome`` now strictly requires
    ``GuardOutcome`` enum instances. Plain strings (even ones that
    coincidentally have a ``.value`` attribute via StrEnum base-class
    inheritance) are rejected with ``TypeError``.

    Fix at ``schemas/guards.py:87-106`` — replaced the
    ``hasattr(x, 'value')`` duck-typing with ``isinstance(x, GuardOutcome)``
    strict checks on both arguments.

    Assertions:
      (1) Two enum inputs still work (the happy path).
      (2) A plain string as 'a' raises TypeError.
      (3) A plain string as 'b' raises TypeError.
    """
    # (1) Happy path still works.
    assert max_outcome(GuardOutcome.PASS, GuardOutcome.BLOCK) == GuardOutcome.BLOCK
    # (2) String as 'a' rejected.
    with pytest.raises(TypeError, match="max_outcome.*expected GuardOutcome"):
        max_outcome("pass", GuardOutcome.BLOCK)  # type: ignore[arg-type]
    # (3) String as 'b' rejected.
    with pytest.raises(TypeError, match="max_outcome.*expected GuardOutcome"):
        max_outcome(GuardOutcome.PASS, "block")  # type: ignore[arg-type]


# ---------------------------------------------------------------------------
# G5 #1184 — decay(factor > 1.0) increases then clamps
# ---------------------------------------------------------------------------

async def test_decay_rejects_factor_above_one_post_R2P9_fix():
    """G5 FLIPPED (#1184 RESOLVED — R2-P9): ``MemoryStoreFacade.decay``
    now rejects factors outside ``[0.0, 1.0]`` with ``ValueError``.

    Pre-fix: ``factor > 1.0`` would multiply confidence above 1.0 and
    the ``min(1.0, ...)`` clamp would silently give the caller a
    1.0-confidence fact — contradicting the function name's
    monotonic-decrease semantics.

    Post-fix: explicit guard at the top of ``decay()`` raises
    ``ValueError`` for any factor outside the closed unit interval.
    Confidence increases require a separate API path (e.g.,
    ``promote_scope``); ``decay`` is monotonic-decrease only.
    """
    graph = AsyncMock()
    vector = AsyncMock()
    embeddings = AsyncMock()
    embeddings.embed_text = AsyncMock(return_value=[0.1] * 1024)
    facade = MemoryStoreFacade(
        graph, vector, embeddings, TraceLedger(), dataset_name="t",
    )
    fact = make_fact_assertion(confidence=0.4)
    # Above-range factor → ValueError.
    with pytest.raises(ValueError, match=r"decay factor must be in \[0\.0, 1\.0\]"):
        await facade.decay(fact.id, 3.0)
    # Below-range (negative) factor → ValueError.
    with pytest.raises(ValueError, match=r"decay factor must be in \[0\.0, 1\.0\]"):
        await facade.decay(fact.id, -0.1)


async def test_decay_accepts_boundary_values_post_R2P9_fix():
    """G5-boundary (R2-P9): factor=0.0 and factor=1.0 are inclusive
    bounds — both are accepted. Pins the boundaries so a future
    off-by-one (e.g., switching to a strict-less-than check) surfaces
    here.
    """
    graph = AsyncMock()
    vector = AsyncMock()
    embeddings = AsyncMock()
    embeddings.embed_text = AsyncMock(return_value=[0.1] * 1024)
    facade = MemoryStoreFacade(
        graph, vector, embeddings, TraceLedger(), dataset_name="t",
    )
    fact = make_fact_assertion(confidence=0.5)
    graph.get_entity = AsyncMock(return_value={
        "eb_id": str(fact.id), "text": fact.text, "category": "general",
        "scope": "session", "confidence": 0.5, "eb_created_at": 0,
        "eb_updated_at": 0, "use_count": 0, "successful_use_count": 0,
        "provenance_refs": [], "target_actor_ids": [], "goal_ids": [],
    })

    from unittest.mock import patch
    with patch("elephantbroker.runtime.memory.facade.add_data_points", new_callable=AsyncMock):
        # factor=0 → confidence drops to 0.
        zero_result = await facade.decay(fact.id, 0.0)
        assert zero_result.confidence == 0.0
        # factor=1 → confidence unchanged.
        one_result = await facade.decay(fact.id, 1.0)
        assert one_result.confidence == 0.5


# ---------------------------------------------------------------------------
# G6 #1147 — ScoringWeights accepts positive penalty values
# ---------------------------------------------------------------------------

def test_scoring_weights_rejects_positive_penalty_values():
    """G6 (#1147 RESOLVED — R2-P2): ``ScoringWeights.redundancy_penalty``,
    ``contradiction_penalty``, and ``cost_penalty`` now carry ``Field(le=0.0)``
    constraints. A positive value inverts the intent of "penalty" — facts
    that are redundant / contradictory / cost-heavy would BOOST their
    score rather than deprioritise. The validator now catches this at
    config load instead of letting a misconfigured profile silently ship.

    Fix at ``schemas/working_set.py:27-34`` — added ``le=0.0`` to all
    three penalty fields.

    Assertions:
      (1) Default negative values still load (happy path — all 5 ship
          profiles at ``runtime/profiles/presets.py`` use negative values).
      (2) Positive values raise ValidationError.
    """
    from pydantic import ValidationError
    # (1) Default negative values load fine.
    defaults = ScoringWeights()
    assert defaults.redundancy_penalty == -0.7
    assert defaults.contradiction_penalty == -1.0
    assert defaults.cost_penalty == -0.3
    # (2) Positive penalty values now rejected.
    with pytest.raises(ValidationError):
        ScoringWeights(redundancy_penalty=0.5)
    with pytest.raises(ValidationError):
        ScoringWeights(contradiction_penalty=0.3)
    with pytest.raises(ValidationError):
        ScoringWeights(cost_penalty=0.1)
    # Zero is still valid (boundary).
    ok = ScoringWeights(
        redundancy_penalty=0.0, contradiction_penalty=0.0, cost_penalty=0.0,
    )
    assert ok.redundancy_penalty == 0.0


# ---------------------------------------------------------------------------
# G7 #1146 — ProcedureDefinition accepts empty activation_modes
# ---------------------------------------------------------------------------

def test_procedure_definition_requires_activation_modes_or_manual_flag():
    """G7 (#1146 RESOLVED — R2-P2.1): ``ProcedureDefinition`` now enforces
    via ``@model_validator`` that every instance must either declare at
    least one ``activation_mode`` OR set ``is_manual_only=True``. A
    procedure with neither can never fire — the engine has no path to
    invoke it — so rejecting at schema-load surfaces the gap immediately.

    Fix at ``schemas/procedure.py:63-116`` — added ``is_manual_only: bool``
    field + ``_require_activation_or_manual_only`` model_validator.

    Backwards-compat at the reconstruction layer
    (``runtime/adapters/cognee/datapoints.py``
    ``ProcedureDataPoint.to_schema`` / ``to_schema_from_dict``): legacy
    procedures stored before R2-P2.1 don't have the is_manual_only flag
    or persisted activation_modes, so reconstruction unconditionally
    auto-infers ``is_manual_only=True``. See those methods' docstrings
    for the round-trip fidelity caveat (activation_modes_json not yet in
    storage schema — orthogonal follow-up).

    Assertions:
      (1) Empty activation_modes + is_manual_only=False → ValidationError.
      (2) Empty activation_modes + is_manual_only=True → succeeds (legitimate
          manual-only procedure).
      (3) Non-empty activation_modes + is_manual_only=False → succeeds
          (auto-triggered procedure, the normal case).
    """
    from pydantic import ValidationError
    from elephantbroker.schemas.procedure import ProcedureActivation
    # (1) Neither activation_modes nor is_manual_only → rejected.
    with pytest.raises(ValidationError, match="at least one activation_mode"):
        ProcedureDefinition(name="my_procedure")
    # Same with explicit empty + default is_manual_only=False.
    with pytest.raises(ValidationError, match="at least one activation_mode"):
        ProcedureDefinition(name="my_procedure_2", activation_modes=[])
    # (2) is_manual_only=True allows empty activation_modes.
    proc_manual = ProcedureDefinition(name="manual_runbook", is_manual_only=True)
    assert proc_manual.activation_modes == []
    assert proc_manual.is_manual_only is True
    # (3) Non-empty activation_modes with the flag left False is the
    # auto-triggered-procedure happy path.
    mode = ProcedureActivation(manual=False, trigger_word="deploy")
    proc_auto = ProcedureDefinition(name="deploy_proc", activation_modes=[mode])
    assert proc_auto.activation_modes == [mode]
    assert proc_auto.is_manual_only is False


# ---------------------------------------------------------------------------
# G10 #1141 — consolidation decay_scope_multipliers misses 3 scopes
# ---------------------------------------------------------------------------

def test_consolidation_decay_scope_multipliers_covers_all_scopes():
    """G10 (#1141 RESOLVED — R2-P2): ``ConsolidationConfig.decay_scope_multipliers``
    default dict now includes all 8 ``Scope`` enum values. The three
    previously missing (``task``, ``subagent``, ``artifact``) default to
    1.0 (base rate — no accelerated decay) pending operational data.

    Fix at ``schemas/consolidation.py:42-60`` — extended the default
    dict, added a ``description`` noting the all-8-scopes invariant.
    Decay consumer at ``consolidation/stages/decay.py:60`` still uses
    ``.get(scope_key, 1.0)`` as defense-in-depth, but the policy is now
    EXPLICIT in the schema instead of implicit in the fallback.

    Assertions:
      (1) All 8 Scope enum values are keys in the default dict.
      (2) The 3 new keys (task/subagent/artifact) default to 1.0.
      (3) Existing 5 keys retain their original values (no drift).
    """
    cfg = ConsolidationConfig()
    multipliers = cfg.decay_scope_multipliers
    all_scopes = {s.value for s in Scope}
    # (1) Coverage — no Scope value is missing.
    assert set(multipliers.keys()) == all_scopes
    missing = all_scopes - set(multipliers.keys())
    assert missing == set()
    # (2) New keys at base rate.
    assert multipliers["task"] == 1.0
    assert multipliers["subagent"] == 1.0
    assert multipliers["artifact"] == 1.0
    # (3) Existing keys unchanged.
    assert multipliers["session"] == 1.5
    assert multipliers["actor"] == 1.0
    assert multipliers["team"] == 0.8
    assert multipliers["organization"] == 0.7
    assert multipliers["global"] == 0.5
