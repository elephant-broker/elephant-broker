"""Smoke tests verifying test infrastructure is reachable."""
from __future__ import annotations

import asyncio

from neo4j import AsyncGraphDatabase


class TestInfraHealth:
    async def test_neo4j_reachable(self, cognee_config):
        """Verify Neo4j accepts authenticated connections.

        Uses a fresh driver per attempt because the async driver's connection
        pool can cache a failed handshake, making retries on the same driver
        ineffective.  Neo4j accepts TCP before auth is fully initialized.
        """
        last_exc = None
        for attempt in range(15):
            driver = AsyncGraphDatabase.driver(
                cognee_config.neo4j_uri,
                auth=(cognee_config.neo4j_user, cognee_config.neo4j_password),
            )
            try:
                await driver.verify_connectivity()
                return
            except Exception as exc:
                last_exc = exc
            finally:
                try:
                    await driver.close()
                except Exception:
                    pass
            await asyncio.sleep(1)
        raise AssertionError(f"Neo4j not reachable after 15 attempts: {last_exc}")

    async def test_qdrant_reachable(self, qdrant_client):
        info = await qdrant_client.get_collections()
        assert info is not None

    async def test_redis_reachable(self, redis_client):
        assert await redis_client.ping()
