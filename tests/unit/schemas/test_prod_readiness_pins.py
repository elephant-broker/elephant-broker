"""TF-FN-019 G1-G10 (schema PROD-risk pins).

Eight tests pinning current behavior on production-readiness risks that have
NOT been fixed in this PR (fixes deferred to the relevant phase rewrites).
When a fix lands, the corresponding pin test will FAIL and force an explicit
un-pin + contract update — the whole point of the pin pattern.

PROD items referenced:
* G1 #1135 — approval request hardcodes 300s timeout, ignores routing config
* G2 #1136 — ``effective_short_name`` doesn't actually truncate short gateway_ids
* G3 #1166 — ``MemoryStoreFacade.search(min_score=...)`` parameter is DEAD
* G4 #1140 — ``max_outcome`` silently accepts strings that happen to have ``.value``
* G5 #1184 — ``facade.decay(factor>1.0)`` can INCREASE confidence (only clamped at 1.0)
* G6 #1147 — ``ScoringWeights.redundancy_penalty`` accepts positive values (should be <=0)
* G7 #1146 — ``ProcedureDefinition.activation_modes`` accepts empty list (never fires)
* G10 #1141 — ``consolidation.decay_scope_multipliers`` default dict misses 3 scopes
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

def test_approval_request_hardcodes_300s_ignoring_routing_timeout():
    """G1 (#1135): ``ApprovalRequest.model_post_init`` (guards.py:248-250)
    hardcodes ``timeout_at = created_at + 300 seconds``.

    There is no way for the routing layer (where the approval request is
    created) to configure a longer or shorter timeout per rule / per
    environment. Any rule claiming "approval times out after 10 minutes"
    in docs is silently wrong today — the actual timeout is always 300s.

    Pin the literal 300-second delta so a future fix that routes per-rule
    timeouts through `ApprovalRequest` will flip this test and force
    explicit verification.
    """
    req = ApprovalRequest(
        request_id=uuid.uuid4(),
        action_id=uuid.uuid4(),
        rule_id="test.rule",
        created_at=datetime(2026, 4, 24, 12, 0, 0, tzinfo=UTC),
    )
    delta = req.timeout_at - req.created_at
    assert delta == timedelta(seconds=300), (
        f"Expected hardcoded 300s timeout, got {delta}. If a fix made the "
        "timeout configurable, update this test and the #1135 entry."
    )


# ---------------------------------------------------------------------------
# G2 #1136 — effective_short_name semantics on short gateway_ids
# ---------------------------------------------------------------------------

def test_effective_short_name_truncates_short_gateway_id():
    """G2 (#1136): ``GatewayConfig.effective_short_name`` (config.py:333-335)
    returns ``gateway_short_name or gateway_id[:8]``.

    ``gateway_id[:8]`` on a 3-char gateway_id returns the 3-char string
    unchanged (Python slice permissiveness). There is no padding, no
    validation, no warning. A gateway_id ``"gw"`` (passes A6 — no
    forbidden chars) produces short_name ``"gw"``. That might be the
    intent, but it is NOT a "truncation to 8 chars" as the field name
    implies — it's "at most 8 chars, no minimum."

    Pin: an undersized gateway_id of 3 chars yields a 3-char short_name.
    """
    # Omit gateway_short_name — schema defaults to "" which is the falsy branch
    # that triggers the `gateway_id[:8]` fallback.
    cfg = GatewayConfig(gateway_id="abc")
    assert cfg.effective_short_name == "abc"
    # Longer gateway_id: truncated to exactly 8.
    cfg_long = GatewayConfig(gateway_id="very-long-gateway-id")
    assert cfg_long.effective_short_name == "very-lon"
    # Explicit override wins regardless of length.
    cfg_explicit = GatewayConfig(gateway_id="very-long-gateway-id", gateway_short_name="X")
    assert cfg_explicit.effective_short_name == "X"


# ---------------------------------------------------------------------------
# G3 #1166 — facade.search(min_score=...) parameter is DEAD
# ---------------------------------------------------------------------------

async def test_facade_search_min_score_parameter_is_dead():
    """G3 (#1166): ``MemoryStoreFacade.search()`` accepts a ``min_score``
    parameter (facade.py:207) but the function body never reads it.

    The structural fallback query uses no similarity score filter; the
    semantic stage depends on Cognee which has its own score threshold.
    Callers passing ``min_score=0.9`` get back the same results as
    callers passing ``min_score=0.0``.

    Pin: passing min_score=0.99 vs min_score=0.0 against the same mock
    dataset returns the same number of facts. Fix would either wire
    min_score to a score-filter step or remove the dead parameter.
    """
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
        "relations": [],
    }])
    # High min_score and low min_score should yield the same count — proof
    # that the parameter is unused.
    high = await facade.search("q", scope=Scope.SESSION, min_score=0.99)
    low = await facade.search("q", scope=Scope.SESSION, min_score=0.0)
    assert len(high) == len(low), (
        f"min_score appears to filter results ({len(high)} vs {len(low)}) — "
        "the dead-parameter pin is obsolete; update this test and #1166."
    )


# ---------------------------------------------------------------------------
# G4 #1140 — max_outcome accepts strings with .value silently
# ---------------------------------------------------------------------------

def test_max_outcome_accepts_strings_with_value_attribute_silently():
    """G4 (#1140): ``max_outcome(a, b)`` (guards.py:87-96) uses
    ``hasattr(a, 'value')`` to decide whether to extract an enum value,
    falling back to ``a`` itself when there is no ``.value``.

    This means a string with a ``.value`` attribute (e.g., a namedtuple or
    a crafted mock) will be consumed as if it were a ``GuardOutcome``
    enum. The function does NOT type-check its inputs — it only checks
    whether the shape is enum-like.

    Pin: a plain string ``"pass"`` passes the ``_OUTCOME_ORDER.get(...)``
    lookup because ``_OUTCOME_ORDER`` is keyed by the enum string values.
    Mixed enum + string inputs are accepted silently with no warning.
    """
    # Plain string outcomes round-trip via _OUTCOME_ORDER without raising.
    result = max_outcome(GuardOutcome.PASS, "pass")
    # The function returns whichever arg has the higher rank — ties return
    # the second arg per `return a if oa >= ob else b` logic. Equal ranks
    # means b is returned.
    assert result == "pass", (
        f"Expected string 'pass' to be accepted as a valid GuardOutcome-like "
        f"input; got {result!r}. If max_outcome now validates types, update "
        "this test and remove the #1140 pin."
    )


# ---------------------------------------------------------------------------
# G5 #1184 — decay(factor > 1.0) increases then clamps
# ---------------------------------------------------------------------------

async def test_facade_decay_with_factor_above_1_increases_then_clamps():
    """G5 (#1184): ``MemoryStoreFacade.decay(fact_id, factor)``
    (facade.py:964 region) computes
    ``max(0.0, min(1.0, fact.confidence * factor))``.

    ``factor > 1.0`` would multiply confidence above 1.0; the ``min(1.0, ...)``
    clamps it back to 1.0. The function name "decay" implies monotonic
    decrease, but mathematically ``decay(0.5, factor=3.0)`` yields 1.0 —
    an INCREASE followed by a clamp. Callers intuitively passing a
    "scaling factor" above 1 don't get what the name suggests; they get
    full confidence.

    Pin: decay(factor=3.0) against a 0.4-confidence fact produces 1.0.
    Fix would either reject factor>1.0 with ValueError or rename to
    ``scale_confidence`` and document the two-way monotonicity.
    """
    graph = AsyncMock()
    vector = AsyncMock()
    embeddings = AsyncMock()
    embeddings.embed_text = AsyncMock(return_value=[0.1] * 1024)
    facade = MemoryStoreFacade(
        graph, vector, embeddings, TraceLedger(), dataset_name="t",
    )
    fact = make_fact_assertion(confidence=0.4)
    graph.get_entity = AsyncMock(return_value={
        "eb_id": str(fact.id), "text": fact.text, "category": "general",
        "scope": "session", "confidence": 0.4, "eb_created_at": 0,
        "eb_updated_at": 0, "use_count": 0, "successful_use_count": 0,
        "provenance_refs": [], "target_actor_ids": [], "goal_ids": [],
    })
    # Inline the add_data_points patch: we don't care about storage here,
    # only that the returned fact carries the clamped confidence.
    from unittest.mock import patch
    with patch("elephantbroker.runtime.memory.facade.add_data_points", new_callable=AsyncMock):
        result = await facade.decay(fact.id, 3.0)
    assert result.confidence == 1.0, (
        f"Expected decay(factor=3.0) on 0.4 to clamp at 1.0; got "
        f"{result.confidence}. If the function now rejects factor>1.0, "
        "update this test and #1184."
    )


# ---------------------------------------------------------------------------
# G6 #1147 — ScoringWeights accepts positive penalty values
# ---------------------------------------------------------------------------

def test_scoring_weights_accepts_positive_penalty_values():
    """G6 (#1147): ``ScoringWeights.redundancy_penalty`` and
    ``contradiction_penalty`` (working_set.py:27-28) have no
    ``Field(le=0)`` constraint. A positive value inverts the intent of
    "penalty" — facts that are redundant or contradictory would then
    INCREASE in score rather than decrease.

    Pin: constructing ScoringWeights with positive penalty values
    succeeds silently. Fix would add ``Field(..., le=0.0)`` with
    migration for any deployed profile configs that happen to have
    positive values.
    """
    # Default negative penalties load fine.
    defaults = ScoringWeights()
    assert defaults.redundancy_penalty == -0.7
    assert defaults.contradiction_penalty == -1.0
    # Positive values load WITHOUT raising — the pin.
    weights = ScoringWeights(
        redundancy_penalty=0.5,
        contradiction_penalty=0.3,
    )
    assert weights.redundancy_penalty == 0.5
    assert weights.contradiction_penalty == 0.3


# ---------------------------------------------------------------------------
# G7 #1146 — ProcedureDefinition accepts empty activation_modes
# ---------------------------------------------------------------------------

def test_procedure_definition_accepts_empty_activation_modes():
    """G7 (#1146): ``ProcedureDefinition.activation_modes`` (procedure.py:70)
    defaults to an empty list via ``Field(default_factory=list)``.

    A procedure with zero activation modes can never be activated by the
    procedure engine — it is silent dead weight. Pydantic doesn't warn
    about this because the list-default is structurally valid.

    Pin: constructing a ProcedureDefinition with no activation_modes
    kwarg yields an empty list with no error. Fix would enforce
    ``min_length=1`` or at least log a WARNING during ingest.
    """
    proc = ProcedureDefinition(name="my_procedure")
    assert proc.activation_modes == []
    # Explicit empty also passes.
    proc2 = ProcedureDefinition(name="my_procedure_2", activation_modes=[])
    assert proc2.activation_modes == []


# ---------------------------------------------------------------------------
# G10 #1141 — consolidation decay_scope_multipliers misses 3 scopes
# ---------------------------------------------------------------------------

def test_consolidation_decay_scope_multipliers_missing_three_scopes():
    """G10 (#1141): ``ConsolidationConfig.decay_scope_multipliers`` default
    dict (consolidation.py:42-48) contains 5 keys: ``session, actor,
    team, organization, global``. The ``Scope`` enum (schemas/base.py)
    defines EIGHT scopes — the three missing are ``task``, ``subagent``,
    ``artifact``.

    Consumed by ``consolidation/stages/decay.py:40`` as
    ``self._scope_multipliers = config.decay_scope_multipliers``. When
    decay walks a TASK / SUBAGENT / ARTIFACT fact, the multiplier lookup
    falls through to the default branch (whatever the decay stage's
    ``.get()`` default is — typically 1.0) — silently treating these
    facts as if they were base-rate.

    Pin: default dict has exactly 5 keys, and exactly 3 Scope values are
    missing from it. Fix would either (a) extend the default to all 8
    scopes with considered multipliers, or (b) document explicitly that
    unlisted scopes get base rate.
    """
    cfg = ConsolidationConfig()
    multipliers = cfg.decay_scope_multipliers
    assert set(multipliers.keys()) == {
        "session", "actor", "team", "organization", "global",
    }
    all_scopes = {s.value for s in Scope}
    missing = all_scopes - set(multipliers.keys())
    assert missing == {"task", "subagent", "artifact"}, (
        f"Expected exactly 3 missing scopes (task, subagent, artifact); "
        f"got missing={missing}. If the default dict was extended, update "
        "this test and the #1141 entry."
    )
