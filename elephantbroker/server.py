"""ElephantBroker server entry point — the FastAPI/uvicorn process.

Usage:
    elephantbroker serve [--config path] [--host 0.0.0.0] [--port 8420]
    elephantbroker health-check [--host localhost] [--port 8420]
    elephantbroker migrate
"""
from __future__ import annotations

import sys

import click


@click.group()
def cli() -> None:
    """ElephantBroker — Unified Cognitive Runtime (server)."""


@cli.command()
@click.option("--host", default="0.0.0.0", help="Bind host")
@click.option("--port", default=8420, type=int, help="Bind port")
@click.option("--log-level", default="info", help="Log level")
@click.option("--config", type=click.Path(exists=True), default=None, help="YAML config file path")
def serve(host: str, port: int, log_level: str, config: str | None) -> None:
    """Start the ElephantBroker API server."""
    import asyncio

    import uvicorn

    from elephantbroker.runtime.container import RuntimeContainer
    from elephantbroker.schemas.config import ElephantBrokerConfig

    async def _build_and_run() -> None:
        if config:
            eb_config = ElephantBrokerConfig.from_yaml(config)
        else:
            eb_config = ElephantBrokerConfig.from_env()
        container = await RuntimeContainer.from_config(eb_config)

        from elephantbroker.api.app import create_app
        app = create_app(container)

        # Map "verbose" to "info" for uvicorn (it doesn't know our custom level)
        uvicorn_level = "info" if log_level.lower() == "verbose" else log_level
        server_config = uvicorn.Config(app, host=host, port=port, log_level=uvicorn_level)
        server = uvicorn.Server(server_config)
        await server.serve()

    asyncio.run(_build_and_run())


@cli.command("health-check")
@click.option("--host", default="localhost", help="Target host")
@click.option("--port", default=8420, type=int, help="Target port")
def health_check(host: str, port: int) -> None:
    """Check if the server is healthy."""
    import httpx

    try:
        r = httpx.get(f"http://{host}:{port}/health/ready", timeout=5.0)
        if r.status_code == 200:
            click.echo("OK")
            sys.exit(0)
        else:
            click.echo(f"UNHEALTHY: {r.status_code}")
            sys.exit(1)
    except Exception as exc:
        click.echo(f"UNREACHABLE: {exc}")
        sys.exit(1)


@cli.command()
def migrate() -> None:
    """Run database migrations (placeholder)."""
    click.echo("No migrations needed.")


def main() -> None:
    """Entry point for ``elephantbroker`` console script."""
    cli()


if __name__ == "__main__":
    main()
