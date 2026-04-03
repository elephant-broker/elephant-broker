"""Tests for stats routes."""


class TestStatsRoutes:
    async def test_get_stats_by_profile(self, client):
        r = await client.get("/stats/by-profile/coding")
        assert r.status_code == 200
