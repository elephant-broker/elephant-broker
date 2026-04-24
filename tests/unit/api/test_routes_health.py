"""Tests for health routes."""
import logging
from unittest.mock import AsyncMock


class TestHealthRoutes:
    async def test_health_returns_ok(self, client):
        r = await client.get("/health/")
        assert r.status_code == 200
        assert r.json()["status"] == "ok"

    async def test_ready_returns_ok(self, client):
        r = await client.get("/health/ready")
        assert r.status_code == 200
        assert r.json()["ready"] is True

    async def test_ready_checks(self, client):
        """G1 extension: /ready reports per-component status for all 4 deep probes
        (Neo4j, Qdrant, Embedding, LLM). No Redis per D14 -- Redis is not part of
        the /ready contract; container.py implements graceful Redis-down degradation
        independently."""
        r = await client.get("/health/ready")
        data = r.json()
        assert "checks" in data
        for component in ["neo4j", "qdrant", "embedding", "llm"]:
            assert component in data["checks"], f"Missing check for {component}"
            assert "status" in data["checks"][component], f"Missing status key for {component}"

    async def test_health_returns_version(self, client):
        r = await client.get("/health/")
        data = r.json()
        assert data["version"] == "0.1.0"
        assert data["tier"].upper() == "FULL"

    async def test_health_live_returns_ok(self, client):
        r = await client.get("/health/live")
        assert r.status_code == 200
        data = r.json()
        assert data["alive"] is True

    # ------------------------------------------------------------------
    # TF-FN-012 additions
    # ------------------------------------------------------------------

    async def test_ready_neo4j_failure_logs_warning_and_reports_error(self, client, container, caplog):
        """G2: Neo4j probe failure emits a WARNING log and reports status=error in the response.

        Pins F3 fix from Step 0 (commit 3526837) -- operators tailing journal must see
        the failure instead of having to parse the /ready response JSON.
        """
        container.graph.query_cypher = AsyncMock(side_effect=ConnectionError("neo4j down"))
        with caplog.at_level(logging.WARNING, logger="elephantbroker.api.routes.health"):
            r = await client.get("/health/ready")
        data = r.json()
        assert data["checks"]["neo4j"]["status"] == "error"
        assert "neo4j down" in data["checks"]["neo4j"]["error"]
        assert "Neo4j health check failed: neo4j down" in caplog.text

    async def test_ready_qdrant_failure_logs_warning_and_reports_error(self, client, container, caplog):
        """G3: Qdrant probe failure emits a WARNING log and reports status=error.

        Pins F4 fix from Step 0 (commit 3526837).
        """
        mock_qdrant_client = AsyncMock()
        mock_qdrant_client.get_collections = AsyncMock(side_effect=Exception("qdrant down"))
        container.vector._get_client = AsyncMock(return_value=mock_qdrant_client)
        with caplog.at_level(logging.WARNING, logger="elephantbroker.api.routes.health"):
            r = await client.get("/health/ready")
        data = r.json()
        assert data["checks"]["qdrant"]["status"] == "error"
        assert "qdrant down" in data["checks"]["qdrant"]["error"]
        assert "Qdrant health check failed: qdrant down" in caplog.text

    async def test_ready_embedding_failure_logs_warning(self, client, container, caplog):
        """G4-a: Embedding probe failure emits a WARNING log (F3+F4 widening)."""
        container.embeddings.embed_text = AsyncMock(side_effect=Exception("embedding down"))
        with caplog.at_level(logging.WARNING, logger="elephantbroker.api.routes.health"):
            await client.get("/health/ready")
        assert "Embedding health check failed: embedding down" in caplog.text

    async def test_ready_llm_failure_logs_warning(self, client, container, caplog):
        """G4-b: LLM probe failure emits a WARNING log (F3+F4 widening)."""
        container.llm_client.complete = AsyncMock(side_effect=Exception("llm down"))
        with caplog.at_level(logging.WARNING, logger="elephantbroker.api.routes.health"):
            await client.get("/health/ready")
        assert "LLM health check failed: llm down" in caplog.text

    async def test_ready_invokes_llm_on_every_call_documented_prod_risk(self, client, container):
        """Pins documented PROD risk #9 -- /ready makes an LLM call (max_tokens=5) on every
        request. In a high-traffic k8s deployment this burns tokens. Future fix should cache
        LLM health with TTL or split into a slower 'deep-ready' endpoint. If changed, update
        this test, the TF-FN-012 plan, and file a TD entry.
        """
        container.llm_client.complete = AsyncMock(return_value="OK")
        await client.get("/health/ready")
        await client.get("/health/ready")
        assert container.llm_client.complete.await_count == 2

    async def test_ready_returns_200_even_when_unhealthy_documented_prod_risk(self, client, container):
        """Pins documented PROD risk #11 -- /ready returns 200 always (FastAPI default),
        even when ready=False. K8s readinessProbe won't detect unhealthy state; traffic
        continues to broken pods. SHIP-BLOCKER for production: should return 503 when
        unhealthy. If fixed (return JSONResponse(..., status_code=503 if not ready else 200)),
        update this test, the plan, and file a TD.
        """
        container.graph.query_cypher = AsyncMock(side_effect=Exception("down"))
        r = await client.get("/health/ready")
        assert r.status_code == 200
        assert r.json()["ready"] is False

    async def test_ready_uses_vector_private_get_client_documented_coupling(self, client, container):
        """Pins documented coupling to private method (#1189). If a public accessor is added
        on VectorAdapter, update this test.
        """
        mock_qdrant = AsyncMock()
        mock_qdrant.get_collections = AsyncMock()
        container.vector._get_client = AsyncMock(return_value=mock_qdrant)
        await client.get("/health/ready")
        assert container.vector._get_client.await_count == 1

    async def test_health_response_does_not_include_gateway_id_documented_prod_risk(self, client):
        """Pins documented PROD risk #1505 -- health response has no operational verification
        of active gateway. If gateway_id is added to the response, update this test.
        """
        r = await client.get("/health/")
        data = r.json()
        assert "gateway_id" not in data
