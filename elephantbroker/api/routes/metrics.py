"""Prometheus metrics endpoint."""
from fastapi import APIRouter
from fastapi.responses import Response

router = APIRouter()


@router.get("/metrics")
async def prometheus_metrics():
    try:
        from prometheus_client import CONTENT_TYPE_LATEST, generate_latest
        return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
    except ImportError:
        return Response(content="# metrics unavailable\n", media_type="text/plain")
