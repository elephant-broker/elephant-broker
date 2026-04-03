"""Tests for claims routes."""


class TestClaimsRoutes:
    async def test_create_claim(self, client):
        body = {"claim_text": "The sky is blue"}
        r = await client.post("/claims/", json=body)
        assert r.status_code == 200

    async def test_attach_evidence(self, client):
        # First create a claim
        claim_r = await client.post("/claims/", json={"claim_text": "Test claim"})
        claim_id = claim_r.json()["id"]
        body = {"type": "tool_output", "ref_value": "test-ref"}
        r = await client.post(f"/claims/{claim_id}/evidence", json=body)
        assert r.status_code == 200

    async def test_verify(self, client):
        claim_r = await client.post("/claims/", json={"claim_text": "Test claim"})
        claim_id = claim_r.json()["id"]
        r = await client.post(f"/claims/{claim_id}/verify")
        assert r.status_code == 200

    async def test_create_claim_missing_body_422(self, client):
        r = await client.post("/claims/", json={})
        assert r.status_code == 422

    async def test_create_claim_when_evidence_disabled(self, client, container):
        container.evidence_engine = None
        r = await client.post("/claims/", json={"claim_text": "test"})
        assert r.status_code == 500

    async def test_attach_evidence_nonexistent_claim_404(self, client):
        import uuid
        r = await client.post(
            f"/claims/{uuid.uuid4()}/evidence",
            json={"type": "tool_output", "ref_value": "ref"},
        )
        assert r.status_code == 404

    async def test_verify_nonexistent_claim_404(self, client):
        import uuid
        r = await client.post(f"/claims/{uuid.uuid4()}/verify")
        assert r.status_code == 404
