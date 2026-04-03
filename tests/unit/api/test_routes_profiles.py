"""Tests for profile routes."""


class TestProfileRoutes:
    async def test_get_profile(self, client):
        r = await client.get("/profiles/coding")
        assert r.status_code == 200
        assert r.json()["id"] == "coding"

    async def test_resolve_profile(self, client):
        r = await client.get("/profiles/coding/resolve")
        assert r.status_code == 200
        assert "weights" in r.json()

    async def test_get_unknown_profile(self, client):
        r = await client.get("/profiles/nonexistent_xyz/resolve")
        # ProfileRegistry raises KeyError for unknown profiles -> 404 via middleware
        assert r.status_code == 404
