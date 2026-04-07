"""ElephantBroker server entry point — the FastAPI/uvicorn process.

Usage:
    elephantbroker serve [--config path] [--host 0.0.0.0] [--port 8420]
    elephantbroker health-check [--host localhost] [--port 8420]
    elephantbroker migrate
    elephantbroker config validate [--config path]
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
        eb_config = ElephantBrokerConfig.load(config)
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


# ---------------------------------------------------------------------------
# C4 (TODO-3-013): `config validate` subcommand
# ---------------------------------------------------------------------------
# install.sh's old smoke test only verified that `elephantbroker --help`
# returned non-zero — it could not catch a malformed default.yaml, an unknown
# YAML key (extra="forbid" violation), an env-binding type coercion failure,
# or a cross-field validator rejection (e.g. F9's embedding model/dim mismatch).
# Operators routinely shipped a config that passed --help but blew up at
# `systemctl start` time, leaving them debugging a service-fail loop instead
# of a clear pre-install error.
#
# This subcommand calls ElephantBrokerConfig.load() — the SAME loader the
# runtime uses at startup — so any structural failure that would crash the
# real serve command also surfaces here. install.sh runs it BEFORE
# `systemctl enable` so the operator gets a clear error in the install log
# instead of a confusing journalctl failure later.

@cli.group("config")
def config_group() -> None:
    """Configuration management."""


@config_group.command("validate")
@click.option(
    "--config",
    type=click.Path(exists=True),
    default=None,
    help="YAML config path to validate (default: packaged default.yaml)",
)
def config_validate(config: str | None) -> None:
    """Validate a config file by running it through the runtime loader.

    Loads the YAML, applies all env-var overrides from ENV_OVERRIDE_BINDINGS,
    and runs every Pydantic validator in the schema tree. Exits 0 on success
    and 1 on any validation failure with the error printed to stderr.

    Used by deploy/install.sh as a pre-systemd-enable smoke test so structural
    config errors fail the install instead of the service start.
    """
    from elephantbroker.schemas.config import ElephantBrokerConfig

    target = config or "(packaged default.yaml)"
    try:
        ElephantBrokerConfig.load(config)
    except Exception as exc:
        click.echo(f"INVALID: {target}: {type(exc).__name__}: {exc}", err=True)
        sys.exit(1)
    click.echo(f"OK: {target} validates against the runtime schema")


def main() -> None:
    """Entry point for ``elephantbroker`` console script."""
    cli()


if __name__ == "__main__":
    main()
