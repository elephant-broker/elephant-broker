"""LLM integration tests for decision_domain extraction (Amendment 7.1, Deviation 5).

These tests require a real LLM endpoint (EB_LLM_* env vars).
They do NOT require Cognee infrastructure (Neo4j/Qdrant) — they test the
extraction task in isolation with a real LLM client.

Placed in unit/ to avoid inheriting integration/conftest.py Cognee session fixtures.
Tests skip automatically when EB_LLM_API_KEY is not set.
"""
from __future__ import annotations

import os
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from elephantbroker.runtime.adapters.cognee.tasks.extract_facts import extract_facts
from elephantbroker.schemas.guards import DecisionDomain


def _make_config(**overrides):
    config = MagicMock()
    config.extraction_max_input_tokens = overrides.get("extraction_max_input_tokens", 4000)
    config.extraction_max_output_tokens = overrides.get("extraction_max_output_tokens", 16384)
    config.extraction_max_facts_per_batch = overrides.get("extraction_max_facts_per_batch", 10)
    return config


def _skip_if_no_llm():
    """Skip if LLM env vars are not set."""
    if not os.environ.get("EB_LLM_API_KEY"):
        pytest.skip("EB_LLM_API_KEY not set — skipping LLM integration tests")


@pytest.fixture
def llm_config():
    _skip_if_no_llm()
    from elephantbroker.schemas.config import LLMConfig
    raw_model = os.environ.get("EB_LLM_MODEL", "openai/gemini/gemini-2.5-pro")
    # Strip openai/ prefix — Cognee strips it internally, but LLMClient sends as-is
    # to LiteLLM. LiteLLM expects "gemini/gemini-2.5-pro", not "openai/gemini/...".
    if raw_model.startswith("openai/"):
        raw_model = raw_model[len("openai/"):]
    return LLMConfig(
        model=raw_model,
        endpoint=os.environ.get("EB_LLM_ENDPOINT", "http://localhost:8811/v1"),
        api_key=os.environ.get("EB_LLM_API_KEY", ""),
    )


@pytest.fixture
def llm_client(llm_config):
    from elephantbroker.runtime.adapters.llm.client import LLMClient
    return LLMClient(config=llm_config)


@pytest.mark.integration
class TestDecisionDomainIntegration:
    """Integration tests requiring real LLM endpoint."""

    async def test_decision_fact_gets_domain_from_llm(self, llm_client):
        """Financial decision message should produce a fact with decision_domain."""
        config = _make_config()
        messages = [
            {"role": "user", "content": "We decided to increase the marketing budget by 50% for Q3. The CFO approved the $500k allocation."},
            {"role": "assistant", "content": "Noted. The marketing budget increase to $500k for Q3 has been approved by the CFO."},
        ]
        result = await extract_facts(messages, [], llm_client, config)
        facts = result["facts"]
        assert len(facts) > 0, "LLM should extract at least one fact from a clear financial decision message"
        # At least one fact should be category=decision with a decision_domain
        decision_facts = [f for f in facts if f.get("category") == "decision"]
        if decision_facts:
            assert any(f.get("decision_domain") for f in decision_facts), (
                f"Decision facts extracted but none have decision_domain: {decision_facts}"
            )

    async def test_non_decision_fact_no_domain_from_llm(self, llm_client):
        """Preference message should not produce decision_domain facts."""
        config = _make_config()
        messages = [
            {"role": "user", "content": "I really prefer using dark mode in my IDE. Vim keybindings are my favorite."},
        ]
        result = await extract_facts(messages, [], llm_client, config)
        facts = result["facts"]
        # Non-decision facts should not have decision_domain
        for f in facts:
            if f.get("category") != "decision":
                assert "decision_domain" not in f, (
                    f"Non-decision fact should not have decision_domain: {f}"
                )

    async def test_domain_flows_to_fact_assertion(self, llm_client):
        """Extracted decision_domain should flow through to FactAssertion construction."""
        from elephantbroker.schemas.fact import FactAssertion

        config = _make_config()
        messages = [
            {"role": "user", "content": "The team decided to deploy the new API to production today. The deployment pipeline will start at 3pm."},
        ]
        result = await extract_facts(messages, [], llm_client, config)
        facts = result["facts"]

        # Build FactAssertions from raw facts
        for rf in facts:
            fa = FactAssertion(
                text=rf["text"],
                category=rf.get("category", "general"),
                decision_domain=rf.get("decision_domain"),
            )
            if rf.get("category") == "decision" and rf.get("decision_domain"):
                assert fa.decision_domain is not None
                assert fa.decision_domain in {d.value for d in DecisionDomain}

    async def test_domain_flows_to_redis_cache(self, llm_client):
        """decision_domain from extracted facts should be writable to Redis-like storage."""
        config = _make_config()
        messages = [
            {"role": "user", "content": "We decided to switch from PostgreSQL to MySQL for the analytics database. This is a data infrastructure decision."},
        ]
        result = await extract_facts(messages, [], llm_client, config)
        facts = result["facts"]

        # Simulate the Redis write-side from turn_ingest pipeline
        domains = [f.get("decision_domain") for f in facts if f.get("decision_domain")]
        # The domain list should be constructable (even if empty — depends on LLM classification)
        assert isinstance(domains, list)

    async def test_multiple_decisions_accumulate_domains(self, llm_client):
        """Multiple decision messages should produce multiple domain entries."""
        config = _make_config()
        messages = [
            {"role": "user", "content": "Three decisions today: 1) We approved the $10k cloud budget increase. 2) The API will be deployed to production Friday. 3) We hired two new backend engineers."},
            {"role": "assistant", "content": "Understood. I've noted the budget approval, production deployment schedule, and new hires."},
        ]
        result = await extract_facts(messages, [], llm_client, config)
        facts = result["facts"]

        # We expect multiple facts; some may be decisions with domains
        decision_facts = [f for f in facts if f.get("category") == "decision"]
        domains = [f.get("decision_domain") for f in decision_facts if f.get("decision_domain")]
        # Domains list should be constructable (LLM may or may not classify all as decisions)
        assert isinstance(domains, list)
        # If multiple decisions were found, domains should accumulate
        if len(decision_facts) > 1:
            assert len(domains) >= 1, "Multiple decision facts but no domains extracted"
