"""End-to-end unit tests for gateway identity flow."""
import uuid

import pytest

from elephantbroker.runtime.identity import deterministic_uuid_from
from elephantbroker.runtime.metrics import MetricsContext
from elephantbroker.runtime.redis_keys import RedisKeyBuilder
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.schemas.config import GatewayConfig
from elephantbroker.schemas.trace import TraceEvent, TraceEventType

# --- Identity model ---

def test_agent_key_is_gateway_plus_agent_id():
    gw = "gw-prod"
    agent_id = "main"
    assert f"{gw}:{agent_id}" == "gw-prod:main"


def test_agent_key_same_for_all_sessions_of_same_agent():
    agent_key = "gw-prod:main"
    id1 = deterministic_uuid_from(agent_key)
    id2 = deterministic_uuid_from(agent_key)
    assert id1 == id2


def test_different_agents_get_different_agent_keys():
    id1 = deterministic_uuid_from("gw-prod:main")
    id2 = deterministic_uuid_from("gw-prod:secondary")
    assert id1 != id2


def test_agent_actor_id_deterministic_from_agent_key():
    id1 = deterministic_uuid_from("gw-prod:main")
    id2 = deterministic_uuid_from("gw-prod:main")
    id3 = deterministic_uuid_from("gw-staging:main")
    assert id1 == id2
    assert id1 != id3
    assert isinstance(id1, uuid.UUID)


# --- TraceLedger auto-enrichment ---

@pytest.mark.asyncio
async def test_trace_ledger_auto_enriches_gateway_id():
    ledger = TraceLedger(gateway_id="gw-test")
    event = TraceEvent(event_type=TraceEventType.SESSION_BOUNDARY)
    assert event.gateway_id is None
    await ledger.append_event(event)
    assert event.gateway_id == "gw-test"


@pytest.mark.asyncio
async def test_trace_ledger_does_not_overwrite_explicit_gateway():
    ledger = TraceLedger(gateway_id="gw-default")
    event = TraceEvent(event_type=TraceEventType.SESSION_BOUNDARY, gateway_id="gw-explicit")
    await ledger.append_event(event)
    assert event.gateway_id == "gw-explicit"


@pytest.mark.asyncio
async def test_trace_ledger_no_enrichment_when_no_gateway():
    ledger = TraceLedger()
    event = TraceEvent(event_type=TraceEventType.SESSION_BOUNDARY)
    await ledger.append_event(event)
    assert event.gateway_id is None


# --- GatewayConfig ---

def test_gateway_config_defaults():
    cfg = GatewayConfig()
    assert cfg.gateway_id == "local"
    assert cfg.effective_short_name == "local"


def test_gateway_config_short_name_override():
    cfg = GatewayConfig(gateway_id="gw-prod-us-east-1", gateway_short_name="us-east")
    assert cfg.effective_short_name == "us-east"


def test_gateway_config_short_name_auto():
    cfg = GatewayConfig(gateway_id="gw-prod-us-east-1")
    assert cfg.effective_short_name == "gw-prod-"


# --- Multi-gateway Redis isolation ---

def test_multi_gateway_redis_isolation():
    keys_a = RedisKeyBuilder("gw-a")
    keys_b = RedisKeyBuilder("gw-b")
    assert keys_a.session_goals("agent:main:main") != keys_b.session_goals("agent:main:main")
    assert keys_a.session_goals("agent:main:main") == "eb:gw-a:session_goals:agent:main:main"
    assert keys_b.session_goals("agent:main:main") == "eb:gw-b:session_goals:agent:main:main"


# --- MetricsContext ---

def test_metrics_context_creation():
    ctx = MetricsContext("gw-test")
    assert ctx._gw == "gw-test"


# --- Schema gateway_id fields ---

def test_fact_assertion_has_gateway_id():
    from elephantbroker.schemas.fact import FactAssertion
    fact = FactAssertion(text="test", gateway_id="gw-1")
    assert fact.gateway_id == "gw-1"


def test_goal_state_has_gateway_id():
    from elephantbroker.schemas.goal import GoalState
    goal = GoalState(title="test", gateway_id="gw-1")
    assert goal.gateway_id == "gw-1"


def test_actor_ref_has_gateway_id():
    from elephantbroker.schemas.actor import ActorRef, ActorType
    actor = ActorRef(type=ActorType.WORKER_AGENT, display_name="test", gateway_id="gw-1")
    assert actor.gateway_id == "gw-1"


def test_trace_event_has_identity_fields():
    ev = TraceEvent(
        event_type=TraceEventType.SESSION_BOUNDARY,
        gateway_id="gw-1",
        agent_key="gw-1:main",
        agent_id="main",
        session_key="agent:main:main",
    )
    assert ev.gateway_id == "gw-1"
    assert ev.agent_key == "gw-1:main"
    assert ev.agent_id == "main"
    assert ev.session_key == "agent:main:main"


# --- DataPoint gateway_id propagation ---

def test_fact_datapoint_propagates_gateway_id():
    from elephantbroker.runtime.adapters.cognee.datapoints import FactDataPoint
    from elephantbroker.schemas.fact import FactAssertion
    fact = FactAssertion(text="test", gateway_id="gw-test")
    dp = FactDataPoint.from_schema(fact)
    assert dp.gateway_id == "gw-test"
    roundtrip = dp.to_schema()
    assert roundtrip.gateway_id == "gw-test"


def test_actor_datapoint_propagates_gateway_id():
    from elephantbroker.runtime.adapters.cognee.datapoints import ActorDataPoint
    from elephantbroker.schemas.actor import ActorRef, ActorType
    actor = ActorRef(type=ActorType.WORKER_AGENT, display_name="test", gateway_id="gw-test")
    dp = ActorDataPoint.from_schema(actor)
    assert dp.gateway_id == "gw-test"
    roundtrip = dp.to_schema()
    assert roundtrip.gateway_id == "gw-test"


def test_goal_datapoint_propagates_gateway_id():
    from elephantbroker.runtime.adapters.cognee.datapoints import GoalDataPoint
    from elephantbroker.schemas.goal import GoalState
    goal = GoalState(title="test", gateway_id="gw-test")
    dp = GoalDataPoint.from_schema(goal)
    assert dp.gateway_id == "gw-test"
    roundtrip = dp.to_schema()
    assert roundtrip.gateway_id == "gw-test"
