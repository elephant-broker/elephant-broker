"""Rerank API routes."""
from __future__ import annotations

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from elephantbroker.api.deps import get_container

router = APIRouter()


class RerankRequest(BaseModel):
    query: str
    documents: list[str] = Field(min_length=1)
    top_n: int | None = None


@router.post("/")
async def rerank(body: RerankRequest, request: Request):
    """Rerank documents by relevance to query."""
    container = get_container(request)
    rerank_orch = getattr(container, "rerank", None)
    if rerank_orch is None:
        raise HTTPException(status_code=501, detail="Reranker not available")

    from elephantbroker.runtime.interfaces.retrieval import RetrievalCandidate
    from elephantbroker.schemas.fact import FactAssertion

    candidates = [
        RetrievalCandidate(
            fact=FactAssertion(text=doc),
            source="api",
            score=1.0 / (i + 1),
        )
        for i, doc in enumerate(body.documents)
    ]
    reranked = await rerank_orch.rerank(candidates, body.query, top_n=body.top_n)
    return {
        "results": [
            {"index": i, "text": c.fact.text, "score": c.score}
            for i, c in enumerate(reranked)
        ]
    }
