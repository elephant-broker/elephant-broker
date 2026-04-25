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

        Pins F4 fix from Step 0 (commit 3526837). R2-P4: route now calls
        ``container.vector.ping()`` (public) instead of reaching into
        ``_get_client()``; the mock target updated accordingly.
        """
        container.vector.ping = AsyncMock(side_effect=Exception("qdrant down"))
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

    async def test_ready_caches_llm_probe_60s_post_R2P4_fix(self, client, container):
        """G6 FLIPPED (#9 RESOLVED — R2-P4): /ready caches the LLM probe
        per-gateway for 60s so K8s readinessProbe loops (default 1×/sec)
        don't burn tokens.

        Pre-fix: every request invoked ``llm_client.complete()`` —
        ~3,600 LLM calls/hour per pod purely on health checks.

        Post-fix: 2 sequential /ready calls within 60s only invoke the
        LLM probe ONCE; the second call reads from the module-level
        ``_llm_probe_cache``.
        """
        container.llm_client.complete = AsyncMock(return_value="OK")
        await client.get("/health/ready")
        await client.get("/health/ready")
        assert container.llm_client.complete.await_count == 1

    async def test_ready_returns_503_when_subcheck_fails_post_R2P4_fix(self, client, container):
        """G7 FLIPPED (#11 RESOLVED — R2-P4): /ready returns HTTP 503 when
        any sub-check fails so K8s readinessProbe can detect unhealthy
        pods.

        Pre-fix: route always returned 200 (FastAPI default), even when
        ``ready=False``. K8s would happily route traffic to broken pods.

        Post-fix: response wrapped in ``JSONResponse(status_code=503)``
        when ``all_ok`` is False; happy path remains 200.
        """
        container.graph.query_cypher = AsyncMock(side_effect=Exception("down"))
        r = await client.get("/health/ready")
        assert r.status_code == 503
        body = r.json()
        assert body["ready"] is False
        assert body["status"] == "unhealthy"

    async def test_ready_uses_vector_ping_public_method_post_R2P4_fix(self, client, container):
        """G8 FLIPPED (#1189 RESOLVED — R2-P4): /ready calls the public
        ``container.vector.ping()`` method instead of reaching into
        ``_get_client()`` (leading-underscore = internal API per
        convention).

        Pre-fix: route did ``await container.vector._get_client()`` then
        ``await client.get_collections()`` — coupled the health endpoint
        to VectorAdapter implementation details. A future refactor that
        renamed/removed ``_get_client`` would silently break the probe.

        Post-fix: route calls ``await container.vector.ping()``; the
        public method on VectorAdapter encapsulates the connectivity
        check (currently ``_get_client + get_collections``) and is part
        of the documented adapter contract.
        """
        container.vector.ping = AsyncMock()
        await client.get("/health/ready")
        assert container.vector.ping.await_count == 1

    async def test_health_response_includes_gateway_id_post_R2P4_fix(self, client, container):
        """G9 FLIPPED (#1505 RESOLVED — R2-P4): /health response now
        includes ``gateway_id`` for operational verification of which
        tenant this pod is bound to.

        Pre-fix: response had no gateway identity — operators had to
        infer the binding from logs / env. Post-fix: ``gateway_id`` is
        a top-level field on both /health and /health/ready response
        bodies.
        """
        r = await client.get("/health/")
        data = r.json()
        assert "gateway_id" in data
        assert data["gateway_id"] == container.gateway_id == "local"

    async def test_health_ready_response_includes_gateway_id_post_R2P4_fix(self, client, container):
        """G9-bis (R2-P4): same gateway_id surfacing on /health/ready.
        The /ready response also gains a top-level ``status`` string
        ("ready" / "unhealthy") to complement the existing boolean.
        """
        r = await client.get("/health/ready")
        data = r.json()
        assert "gateway_id" in data
        assert data["gateway_id"] == "local"
        assert data["status"] == ("ready" if data["ready"] else "unhealthy")
