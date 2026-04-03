"""Tests for health routes."""


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
        r = await client.get("/health/ready")
        data = r.json()
        assert "checks" in data

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
