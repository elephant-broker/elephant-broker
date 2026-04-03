"""HITL Middleware entrypoint — run with `python -m hitl_middleware`."""
import uvicorn

from hitl_middleware.app import create_app
from hitl_middleware.config import HitlMiddlewareConfig


def main() -> None:
    config = HitlMiddlewareConfig.from_env()
    app = create_app(config)
    uvicorn.run(app, host=config.host, port=config.port, log_level=config.log_level.lower())


if __name__ == "__main__":
    main()
