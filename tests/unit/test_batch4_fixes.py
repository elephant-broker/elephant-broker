"""Unit tests protecting Batch 4 cross-phase integration fixes.

Covers:
1. Gateway_id conditional assignment (issue #16)
2. GDPR delete ownership check (issue #18)
3. Search gateway isolation (issue #24)
4. Procedure completion gate — both paths (issue #21)
5. FactDataPoint session_key defaults (issue #34/#36)
6. Middleware default gateway_id (issue #15)
7. Trace event enrichment (gateway_id auto-stamp)
8. afterTurn message passing (Bug B — engine.ts tested separately)
9. LLM complete_json retry on empty response (issue #29)
10. Step/complete auto-creates claim (issue #25)
"""
from __future__ import annotations

import json
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from elephantbroker.runtime.adapters.cognee.datapoints import FactDataPoint
from elephantbroker.runtime.evidence.engine import EvidenceAndVerificationEngine
from elephantbroker.runtime.memory.facade import DedupSkipped, MemoryStoreFacade
from elephantbroker.runtime.trace.ledger import TraceLedger
from elephantbroker.schemas.evidence import ClaimRecord, ClaimStatus, EvidenceRef
from elephantbroker.schemas.fact import FactAssertion
from elephantbroker.schemas.trace import TraceEvent, TraceEventType
from tests.fixtures.factories import (
    make_claim_record,
    make_evidence_ref,
    make_fact_assertion,
)


# ---------------------------------------------------------------------------
# 1. Gateway_id conditional assignment (issue #16)
# ---------------------------------------------------------------------------


class TestGatewayIdConditionalAssignment:
    """Verify that modules use plugin-stamped gateway_id, config is fallback only."""

    def test_fact_datapoint_preserves_plugin_gateway_id(self):
        """When fact has gateway_id set, from_schema should preserve it."""
        fact = make_fact_assertion(gateway_id="gw-plugin")
        dp = FactDataPoint.from_schema(fact)
        assert dp.gateway_id == "gw-plugin"

    def test_fact_datapoint_falls_back_to_empty(self):
        """When fact has no gateway_id, from_schema should use empty string."""
        fact = make_fact_assertion()
        fact.gateway_id = ""
        dp = FactDataPoint.from_schema(fact)
        assert dp.gateway_id == ""

    async def test_facade_store_preserves_plugin_gateway(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """Facade.store() should use fact.gateway_id, not overwrite with config."""
        graph = AsyncMock()
        vector = AsyncMock()
        embeddings = AsyncMock()
        embeddings.embed_text = AsyncMock(return_value=[0.1] * 1024)
        ledger = TraceLedger()
        facade = MemoryStoreFacade(
            graph, vector, embeddings, ledger,
            dataset_name="test_ds", gateway_id="gw-config",
        )
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)

        fact = make_fact_assertion(gateway_id="gw-plugin")
        result = await facade.store(fact)
        assert result.gateway_id == "gw-plugin"

    async def test_facade_store_uses_config_as_fallback(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """When fact.gateway_id is empty, facade should stamp config value."""
        graph = AsyncMock()
        vector = AsyncMock()
        embeddings = AsyncMock()
        embeddings.embed_text = AsyncMock(return_value=[0.1] * 1024)
        ledger = TraceLedger()
        facade = MemoryStoreFacade(
            graph, vector, embeddings, ledger,
            dataset_name="test_ds", gateway_id="gw-config",
        )
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)

        fact = make_fact_assertion(gateway_id="")
        result = await facade.store(fact)
        assert result.gateway_id == "gw-config"


# ---------------------------------------------------------------------------
# 2. GDPR delete ownership check (issue #18)
# ---------------------------------------------------------------------------


class TestGDPRDeleteOwnership:
    """Verify that delete checks caller gateway_id against stored fact."""

    def _make_facade(self):
        graph = AsyncMock()
        vector = AsyncMock()
        embeddings = AsyncMock()
        ledger = TraceLedger()
        return MemoryStoreFacade(
            graph, vector, embeddings, ledger,
            dataset_name="test_ds", gateway_id="gw-config",
        ), graph, vector

    async def test_delete_blocks_wrong_gateway(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, vector = self._make_facade()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)

        graph.get_entity = AsyncMock(return_value={
            "eb_id": str(uuid.uuid4()), "text": "secret", "category": "general",
            "scope": "session", "confidence": 1.0, "eb_created_at": 0,
            "eb_updated_at": 0, "use_count": 0, "successful_use_count": 0,
            "provenance_refs": [], "target_actor_ids": [], "goal_ids": [],
            "gateway_id": "gw-owner",
        })
        with pytest.raises(PermissionError, match="gateway"):
            await facade.delete(uuid.uuid4(), caller_gateway_id="gw-attacker")

    async def test_delete_allows_correct_gateway(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, vector = self._make_facade()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)

        fact_id = uuid.uuid4()
        graph.get_entity = AsyncMock(return_value={
            "eb_id": str(fact_id), "text": "secret", "category": "general",
            "scope": "session", "confidence": 1.0, "eb_created_at": 0,
            "eb_updated_at": 0, "use_count": 0, "successful_use_count": 0,
            "provenance_refs": [], "target_actor_ids": [], "goal_ids": [],
            "gateway_id": "gw-owner",
        })
        graph.delete_entity = AsyncMock()
        vector.delete_embedding = AsyncMock()
        # Should not raise
        await facade.delete(fact_id, caller_gateway_id="gw-owner")

    async def test_delete_not_found_raises_key_error(self, monkeypatch, mock_add_data_points, mock_cognee):
        facade, graph, _ = self._make_facade()
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        graph.get_entity = AsyncMock(return_value=None)
        with pytest.raises(KeyError):
            await facade.delete(uuid.uuid4(), caller_gateway_id="gw-any")


# ---------------------------------------------------------------------------
# 3. Search gateway isolation (issue #24)
# ---------------------------------------------------------------------------


class TestSearchGatewayIsolation:
    """Verify that search filters by caller_gateway_id."""

    async def test_structural_query_uses_caller_gateway(self, monkeypatch, mock_cognee):
        graph = AsyncMock()
        vector = AsyncMock()
        embeddings = AsyncMock()
        ledger = TraceLedger()
        facade = MemoryStoreFacade(
            graph, vector, embeddings, ledger,
            dataset_name="test_ds", gateway_id="gw-config",
        )
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        graph.query_cypher = AsyncMock(return_value=[])

        await facade.search("test", caller_gateway_id="gw-caller")

        # Verify Cypher was called with caller gateway, not config
        call_args = graph.query_cypher.call_args
        cypher = call_args[0][0]
        params = call_args[0][1]
        assert "f.gateway_id = $gateway_id" in cypher
        assert params["gateway_id"] == "gw-caller"

    async def test_structural_query_falls_back_to_config(self, monkeypatch, mock_cognee):
        graph = AsyncMock()
        vector = AsyncMock()
        embeddings = AsyncMock()
        ledger = TraceLedger()
        facade = MemoryStoreFacade(
            graph, vector, embeddings, ledger,
            dataset_name="test_ds", gateway_id="gw-config",
        )
        monkeypatch.setattr("elephantbroker.runtime.memory.facade.cognee", mock_cognee)
        graph.query_cypher = AsyncMock(return_value=[])

        await facade.search("test", caller_gateway_id="")

        call_args = graph.query_cypher.call_args
        params = call_args[0][1]
        assert params["gateway_id"] == "gw-config"


# ---------------------------------------------------------------------------
# 4. Procedure completion gate (issue #21)
# ---------------------------------------------------------------------------


class TestProcedureCompletionGate:
    """Verify completion check requires per-step evidence."""

    def _make_engine(self):
        graph = AsyncMock()
        ledger = TraceLedger()
        return EvidenceAndVerificationEngine(graph, ledger, dataset_name="test_ds"), graph

    async def test_completion_incomplete_with_missing_steps(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        engine, graph = self._make_engine()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)

        proc_id = uuid.uuid4()
        step1_id = uuid.uuid4()
        step2_id = uuid.uuid4()

        # Only create claim for step 1
        claim = make_claim_record(procedure_id=proc_id, step_id=step1_id)
        await engine.record_claim(claim)
        ev = make_evidence_ref(type="tool_output")
        await engine.attach_evidence(claim.id, ev)
        await engine.verify(claim.id)

        # Graph returns procedure with 2 non-optional steps
        graph.query_cypher = AsyncMock(return_value=[{
            "p": {
                "eb_id": str(proc_id),
                "gateway_id": "local",
                "steps_json": json.dumps([
                    {"id": str(step1_id), "order": 0, "instruction": "Step 1", "required_evidence": [{"proof_type": "tool_output", "required": True, "description": "proof"}]},
                    {"id": str(step2_id), "order": 1, "instruction": "Step 2", "required_evidence": [{"proof_type": "tool_output", "required": True, "description": "proof"}]},
                ]),
            },
        }])

        result = await engine.check_completion_requirements(proc_id)
        assert result.complete is False
        assert len(result.missing_evidence) >= 1

    async def test_completion_complete_with_all_steps(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        engine, graph = self._make_engine()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)

        proc_id = uuid.uuid4()
        step1_id = uuid.uuid4()
        step2_id = uuid.uuid4()

        # Create claims for both steps
        for sid in [step1_id, step2_id]:
            claim = make_claim_record(procedure_id=proc_id, step_id=sid)
            await engine.record_claim(claim)
            ev = make_evidence_ref(type="tool_output")
            await engine.attach_evidence(claim.id, ev)
            await engine.verify(claim.id)

        graph.query_cypher = AsyncMock(return_value=[{
            "p": {
                "eb_id": str(proc_id),
                "gateway_id": "local",
                "steps_json": json.dumps([
                    {"id": str(step1_id), "order": 0, "instruction": "Step 1", "required_evidence": [{"proof_type": "tool_output", "required": True, "description": "proof"}]},
                    {"id": str(step2_id), "order": 1, "instruction": "Step 2", "required_evidence": [{"proof_type": "tool_output", "required": True, "description": "proof"}]},
                ]),
            },
        }])

        result = await engine.check_completion_requirements(proc_id)
        assert result.complete is True

    async def test_claim_step_id_must_match(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """A claim for step1 should NOT satisfy step2's requirement."""
        engine, graph = self._make_engine()
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)

        proc_id = uuid.uuid4()
        step1_id = uuid.uuid4()
        step2_id = uuid.uuid4()

        # Create claim only for step1
        claim = make_claim_record(procedure_id=proc_id, step_id=step1_id)
        await engine.record_claim(claim)
        ev = make_evidence_ref(type="tool_output")
        await engine.attach_evidence(claim.id, ev)
        await engine.verify(claim.id)

        graph.query_cypher = AsyncMock(return_value=[{
            "p": {
                "eb_id": str(proc_id),
                "gateway_id": "local",
                "steps_json": json.dumps([
                    {"id": str(step1_id), "order": 0, "instruction": "Step 1", "required_evidence": [{"proof_type": "tool_output", "required": True, "description": "proof"}]},
                    {"id": str(step2_id), "order": 1, "instruction": "Step 2", "required_evidence": [{"proof_type": "tool_output", "required": True, "description": "proof"}]},
                ]),
            },
        }])

        result = await engine.check_completion_requirements(proc_id)
        assert result.complete is False


# ---------------------------------------------------------------------------
# 5. FactDataPoint session_key defaults (issue #34/#36)
# ---------------------------------------------------------------------------


class TestFactDataPointDefaults:
    """Verify FactDataPoint uses empty string defaults, not None."""

    def test_session_key_default_is_empty_string(self):
        dp = FactDataPoint(id=uuid.uuid4(), text="test", category="general")
        assert dp.session_key == ""
        assert dp.session_id == ""
        assert dp.source_actor_id == ""

    def test_from_schema_coerces_none_to_empty(self):
        fact = make_fact_assertion()
        fact.session_key = None
        fact.session_id = None
        fact.source_actor_id = None
        dp = FactDataPoint.from_schema(fact)
        assert dp.session_key == ""
        assert dp.session_id == ""
        assert dp.source_actor_id == ""

    def test_from_schema_preserves_values(self):
        sid = uuid.uuid4()
        actor_id = uuid.uuid4()
        fact = make_fact_assertion(
            session_key="agent:main:main",
            session_id=sid,
            source_actor_id=actor_id,
        )
        dp = FactDataPoint.from_schema(fact)
        assert dp.session_key == "agent:main:main"
        assert dp.session_id == str(sid)
        assert dp.source_actor_id == str(actor_id)

    def test_to_schema_handles_empty_strings(self):
        dp = FactDataPoint(
            id=uuid.uuid4(), text="test", category="general",
            eb_id=str(uuid.uuid4()),
            session_key="", session_id="", source_actor_id="",
        )
        fact = dp.to_schema()
        assert fact.session_id is None
        assert fact.source_actor_id is None


# ---------------------------------------------------------------------------
# 6. Middleware default gateway_id (issue #15)
# ---------------------------------------------------------------------------


class TestMiddlewareDefaultGateway:
    """Verify GatewayIdentityMiddleware defaults and header override."""

    def test_default_gateway_is_local(self):
        """Missing X-EB-Gateway-ID header defaults to 'local'."""
        from elephantbroker.api.middleware.gateway import GatewayIdentityMiddleware

        middleware = GatewayIdentityMiddleware(app=None, default_gateway_id="local")
        assert middleware._default == "local"

    async def test_header_overrides_default(self):
        """Explicit header value takes precedence over default.

        R2-P1.1: middleware now rejects mismatched X-EB-Gateway-ID with 403.
        The "header overrides default" contract still holds when ``default``
        is empty (legacy single-tenant / dev fallback), since the reject
        branch requires ``self._default`` to be truthy. We flip the default
        to "" here so the test continues to exercise the
        header-takes-precedence path. For the cross-tenant reject contract
        post-R2-P1.1, see ``test_gateway_reject_mismatch.py``.
        """
        from elephantbroker.api.middleware.gateway import GatewayIdentityMiddleware
        from starlette.requests import Request
        from starlette.datastructures import State

        captured_gw = {}

        async def mock_call_next(request):
            captured_gw["value"] = request.state.gateway_id
            return MagicMock()

        # R2-P1.1: empty default disables the mismatch reject; header value
        # still takes precedence per legacy contract for empty-default config.
        middleware = GatewayIdentityMiddleware(app=None, default_gateway_id="")
        scope = {
            "type": "http", "method": "GET", "path": "/health",
            "headers": [(b"x-eb-gateway-id", b"gw-explicit")],
            "query_string": b"",
        }
        request = Request(scope)
        request._state = State()
        await middleware.dispatch(request, mock_call_next)
        assert captured_gw["value"] == "gw-explicit"


# ---------------------------------------------------------------------------
# 7. Trace event enrichment
# ---------------------------------------------------------------------------


class TestTraceEventEnrichment:
    """Verify TraceLedger auto-stamps gateway_id on events."""

    async def test_ledger_stamps_gateway_id(self):
        ledger = TraceLedger(gateway_id="gw-test")
        event = TraceEvent(event_type=TraceEventType.INPUT_RECEIVED)
        assert event.gateway_id is None
        await ledger.append_event(event)
        assert event.gateway_id == "gw-test"

    async def test_ledger_preserves_existing_gateway_id(self):
        ledger = TraceLedger(gateway_id="gw-default")
        event = TraceEvent(
            event_type=TraceEventType.INPUT_RECEIVED,
            gateway_id="gw-explicit",
        )
        await ledger.append_event(event)
        assert event.gateway_id == "gw-explicit"


# ---------------------------------------------------------------------------
# 8. LLM complete_json retry on empty response (issue #29)
# ---------------------------------------------------------------------------


class TestLLMRetryOnEmpty:
    """Verify complete_json retries once on empty LLM response."""

    async def test_retry_on_empty_content(self):
        from elephantbroker.runtime.adapters.llm.client import LLMClient

        config = MagicMock()
        config.max_tokens = 1000
        config.model = "test-model"
        config.endpoint = "http://localhost:8080"

        client = LLMClient.__new__(LLMClient)
        client._model = "test-model"
        client._endpoint = "http://localhost:8080"
        client._config = config

        call_count = 0

        async def mock_post(url, payload):
            nonlocal call_count
            call_count += 1
            resp = MagicMock()
            if call_count == 1:
                resp.json.return_value = {"choices": [{"message": {"content": ""}}]}
            else:
                resp.json.return_value = {"choices": [{"message": {"content": '{"facts": []}'}}]}
            return resp

        client._post_with_retry = mock_post
        result = await client.complete_json("system", "user")
        assert call_count == 2
        assert result == {"facts": []}

    async def test_no_retry_on_valid_content(self):
        from elephantbroker.runtime.adapters.llm.client import LLMClient

        config = MagicMock()
        config.max_tokens = 1000

        client = LLMClient.__new__(LLMClient)
        client._model = "test-model"
        client._endpoint = "http://localhost:8080"
        client._config = config

        call_count = 0

        async def mock_post(url, payload):
            nonlocal call_count
            call_count += 1
            resp = MagicMock()
            resp.json.return_value = {"choices": [{"message": {"content": '{"facts": [{"text": "hi"}]}'}}]}
            return resp

        client._post_with_retry = mock_post
        result = await client.complete_json("system", "user")
        assert call_count == 1


# ---------------------------------------------------------------------------
# 9. Extract facts basic behavior (issue #29)
# ---------------------------------------------------------------------------


class TestExtractFactsBasic:
    """Verify extract_facts returns parsed facts from LLM response."""

    async def test_extraction_returns_facts(self):
        """extract_facts should return structured facts from LLM output."""
        from elephantbroker.runtime.adapters.cognee.tasks.extract_facts import extract_facts

        llm = AsyncMock()
        llm.complete_json = AsyncMock(return_value={"facts": [{"text": "test fact", "category": "general", "source_turns": [0], "supersedes_index": -1}], "goal_status_hints": []})
        config = MagicMock()
        config.extraction_max_facts_per_batch = 10
        config.extraction_max_input_tokens = 4000
        config.extraction_max_output_tokens = 16384

        messages = [{"role": "user", "content": "The deployment is scheduled for Tuesday at 3pm UTC."}]

        result = await extract_facts(messages, [], llm, config)
        assert len(result["facts"]) == 1
        assert result["facts"][0]["text"] == "test fact"


# ---------------------------------------------------------------------------
# 10. Step/complete auto-creates claim (issue #25)
# ---------------------------------------------------------------------------


class TestStepCompleteCreatesClaim:
    """Verify that completing a step with proof auto-creates ClaimRecord + EvidenceRef."""

    async def test_evidence_engine_claim_and_verify(
        self, monkeypatch, mock_add_data_points, mock_cognee,
    ):
        """Simulate what the route does: record_claim → attach_evidence → verify."""
        graph = AsyncMock()
        graph.add_relation = AsyncMock()
        ledger = TraceLedger()
        engine = EvidenceAndVerificationEngine(graph, ledger, dataset_name="test_ds")
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.add_data_points", mock_add_data_points)
        monkeypatch.setattr("elephantbroker.runtime.evidence.engine.cognee", mock_cognee)

        proc_id = uuid.uuid4()
        step_id = uuid.uuid4()

        claim = ClaimRecord(
            claim_text=f"Step {step_id} completed with proof",
            procedure_id=proc_id,
            step_id=step_id,
            gateway_id="gw-test",
        )
        claim = await engine.record_claim(claim)
        assert claim.status == ClaimStatus.UNVERIFIED

        evidence = EvidenceRef(
            type="tool_output",
            ref_value="proof-value-123",
            gateway_id="gw-test",
        )
        claim = await engine.attach_evidence(claim.id, evidence)
        assert claim.status == ClaimStatus.SELF_SUPPORTED

        claim = await engine.verify(claim.id)
        assert claim.status == ClaimStatus.TOOL_SUPPORTED

        # Verify claim is findable by procedure_id
        claims = await engine.get_claims_for_procedure(proc_id)
        assert len(claims) == 1
        assert claims[0].step_id == step_id
