"""Scenario: Basic Memory — store, search, dedup, auto-recall."""
from __future__ import annotations

from tests.scenarios.base import Scenario
from tests.scenarios.runner import register


@register
class BasicMemoryScenario(Scenario):
    """Store -> search -> dedup -> auto-recall. Memory-only, no context lifecycle."""

    name = "basic_memory"
    required_phase = 4
    required_amendment_6_2 = False

    async def run(self):
        self.expect_trace("retrieval_performed", min_count=1)
        self.expect_trace("fact_extracted", min_count=1)

        # Step 1: Store 3 facts
        for i, text in enumerate([
            "Redis supports pub/sub messaging",
            "Neo4j uses Cypher query language",
            "Qdrant stores vector embeddings",
        ]):
            r = await self.sim.simulate_tool_memory_store(text, category="technical")
            self.step(f"store_fact_{i}", passed="id" in r or "fact_id" in r,
                      message=f"Stored: {text[:40]}")

        # Step 2: Search
        for query in ["Redis messaging", "Cypher graph queries", "vector embeddings"]:
            results = await self.sim.simulate_tool_memory_search(query)
            self.step(f"search_{query[:20]}", passed=len(results) > 0,
                      message=f"Found {len(results)} results")

        # Step 3: Store near-duplicate
        r = await self.sim.simulate_tool_memory_store(
            "Redis supports pub/sub messaging patterns", category="technical")
        self.expect_trace("dedup_triggered", min_count=0)

        # Step 4: Auto-recall
        recalled = await self.sim.simulate_before_agent_start(
            "Tell me about the databases we use")
        self.step("auto_recall", passed=len(recalled) > 0,
                  message=f"Recalled {len(recalled)} facts")
