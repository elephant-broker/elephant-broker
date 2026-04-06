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
@click.option("--config", "config_path", type=click.Path(exists=True), default=None, help="YAML config file path")
@click.option("--revision", default="head", help="Alembic revision target (default: head)")
def migrate(config_path: str | None, revision: str) -> None:
    """Run database migrations (Alembic upgrade)."""
    import os
    import pathlib

    from alembic import command as alembic_command
    from alembic.config import Config as AlembicConfig

    # Resolve EB_POSTGRES_DSN from config file or env
    if config_path:
        import yaml
        from elephantbroker.schemas.config import ElephantBrokerConfig
        eb_config = ElephantBrokerConfig.from_yaml(config_path)
        os.environ.setdefault("EB_POSTGRES_DSN", eb_config.audit.postgres_dsn)
    elif not os.environ.get("EB_POSTGRES_DSN"):
        click.echo(
            "ERROR: EB_POSTGRES_DSN is not set. "
            "Pass --config or set EB_POSTGRES_DSN environment variable.",
            err=True,
        )
        sys.exit(1)

    alembic_ini = pathlib.Path(__file__).parent / "db" / "alembic.ini"
    if not alembic_ini.exists():
        click.echo(f"ERROR: Alembic config not found at {alembic_ini}", err=True)
        sys.exit(1)

    click.echo(f"Running Alembic migrations → {revision} ...")
    alembic_cfg = AlembicConfig(str(alembic_ini))
    alembic_command.upgrade(alembic_cfg, revision)
    click.echo("Migrations complete.")


def main() -> None:
    """Entry point for ``elephantbroker`` console script."""
    cli()


if __name__ == "__main__":
    main()
