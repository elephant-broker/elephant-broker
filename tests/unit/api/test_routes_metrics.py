"""Tests for Prometheus metrics endpoint."""


class TestMetricsRoutes:
    async def test_metrics_endpoint_returns_text(self, client):
        r = await client.get("/metrics")
        assert r.status_code == 200
        # Should return some text content (prometheus format or unavailable message)
        assert len(r.text) > 0
