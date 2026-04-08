"""HITL Middleware entrypoint.

Canonical invocation is the `hitl-middleware` console script, installed by
`uv sync` from the `[project.scripts]` entry in `hitl-middleware/pyproject.toml`
(added in Bucket E1 when hitl-middleware became a uv workspace member). The
systemd unit (`deploy/systemd/elephantbroker-hitl.service`) launches the
service via that console script. `python -m hitl_middleware` still works as
an alternative when the console script is not on PATH (e.g. ad-hoc dev runs
inside an activated venv).
"""
import uvicorn

from hitl_middleware.app import create_app
from hitl_middleware.config import HitlMiddlewareConfig


def main() -> None:
    config = HitlMiddlewareConfig.from_env()
    app = create_app(config)
    uvicorn.run(app, host=config.host, port=config.port, log_level=config.log_level.lower())


if __name__ == "__main__":
    main()
