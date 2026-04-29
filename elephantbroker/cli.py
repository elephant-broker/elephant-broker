"""ebrun — ElephantBroker admin CLI tool.

All commands call the runtime API over HTTP (requires runtime to be running).
Actor identity: --actor-id flag > EB_ACTOR_ID env > ~/.elephantbroker/config.json.

Usage:
    ebrun bootstrap --org-name "Acme" --team-name "Backend" --admin-name "Admin"
    ebrun org create --name "Acme"
    ebrun team create --name "Backend" --org-id <uuid>
    ebrun actor create --display-name "Maria" --type human_operator
    ebrun profile list
    ebrun authority list
    ebrun goal create --title "Q1 Roadmap" --scope organization
    ebrun config set actor-id <uuid>
"""
from __future__ import annotations

import json
import os
import sys

import click


def _config_path() -> str:
    return os.path.expanduser("~/.elephantbroker/config.json")


def _load_config() -> dict:
    path = _config_path()
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}


def _save_config(data: dict) -> None:
    path = _config_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def _resolve_actor_id(ctx_actor_id: str | None) -> str:
    """Resolve actor_id: flag > env > config file."""
    if ctx_actor_id:
        return ctx_actor_id
    env_val = os.environ.get("EB_ACTOR_ID")
    if env_val:
        return env_val
    cfg = _load_config()
    if cfg.get("actor_id"):
        return cfg["actor_id"]
    return ""


def _resolve_runtime_url(ctx_url: str | None) -> str:
    """Resolve runtime URL: flag > env > config file > default."""
    if ctx_url:
        return ctx_url
    env_val = os.environ.get("EB_RUNTIME_URL")
    if env_val:
        return env_val
    cfg = _load_config()
    return cfg.get("runtime_url", "http://localhost:8420")


def _api(method: str, url: str, actor_id: str, body: dict | None = None) -> dict:
    """Make an HTTP request to the runtime API."""
    import httpx
    headers = {"Content-Type": "application/json"}
    if actor_id:
        headers["X-EB-Actor-Id"] = actor_id
    # 60s timeout: admin ops may trigger cognify() or graph writes that take tens of seconds
    try:
        if method == "GET":
            r = httpx.get(url, headers=headers, timeout=60.0)
        elif method == "POST":
            r = httpx.post(url, headers=headers, json=body or {}, timeout=60.0)
        elif method == "PUT":
            r = httpx.put(url, headers=headers, json=body or {}, timeout=60.0)
        elif method == "DELETE":
            r = httpx.delete(url, headers=headers, timeout=60.0)
        else:
            click.echo(f"Unknown method: {method}")
            sys.exit(1)
        if r.status_code >= 400:
            click.echo(f"Error {r.status_code}: {r.text}")
            sys.exit(1)
        return r.json()
    except httpx.ConnectError:
        click.echo("Cannot connect to runtime. Is it running?")
        sys.exit(1)


@click.group()
@click.option("--actor-id", default=None, envvar="EB_ACTOR_ID", help="Actor UUID for authorization")
@click.option("--runtime-url", default=None, envvar="EB_RUNTIME_URL", help="Runtime API URL")
@click.pass_context
def cli(ctx: click.Context, actor_id: str | None, runtime_url: str | None) -> None:
    """ebrun — ElephantBroker admin CLI."""
    ctx.ensure_object(dict)
    ctx.obj["actor_id"] = _resolve_actor_id(actor_id)
    ctx.obj["runtime_url"] = _resolve_runtime_url(runtime_url)


# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

@cli.group("config")
def config_group() -> None:
    """Manage CLI configuration."""


@config_group.command("set")
@click.argument("key")
@click.argument("value")
def config_set(key: str, value: str) -> None:
    """Set a config value (actor-id, runtime-url)."""
    cfg = _load_config()
    key_map = {"actor-id": "actor_id", "runtime-url": "runtime_url"}
    cfg[key_map.get(key, key)] = value
    _save_config(cfg)
    click.echo(f"Set {key} = {value}")


@config_group.command("show")
def config_show() -> None:
    """Show current CLI configuration."""
    cfg = _load_config()
    click.echo(json.dumps(cfg, indent=2))


# ---------------------------------------------------------------------------
# Bootstrap
# ---------------------------------------------------------------------------

@cli.command()
@click.option("--org-name", required=True, help="Organization name")
@click.option("--org-label", default="", help="Short display label")
@click.option("--team-name", required=True, help="Team name")
@click.option("--team-label", default="", help="Short display label")
@click.option("--admin-name", required=True, help="Admin actor display name")
@click.option("--admin-authority", default=90, type=int, help="Admin authority level")
@click.option("--admin-handles", multiple=True, help="Admin handles (e.g. email:admin@acme.com)")
@click.pass_context
def bootstrap(ctx: click.Context, org_name: str, org_label: str, team_name: str,
              team_label: str, admin_name: str, admin_authority: int, admin_handles: tuple) -> None:
    """Bootstrap: create first org, team, and admin actor (requires empty graph)."""
    url = ctx.obj["runtime_url"]

    # Check bootstrap status
    status = _api("GET", f"{url}/admin/bootstrap-status", "")
    if not status.get("bootstrap_mode"):
        click.echo("Bootstrap mode is not active (actors already exist). Aborting.")
        sys.exit(1)

    # Create org
    org = _api("POST", f"{url}/admin/organizations", "", {
        "name": org_name, "display_label": org_label or org_name[:20],
    })
    org_id = org["org_id"]
    click.echo(f"Organization created: org_id={org_id}")

    # Create team
    team = _api("POST", f"{url}/admin/teams", "", {
        "name": team_name, "display_label": team_label or team_name[:20], "org_id": org_id,
    })
    team_id = team["team_id"]
    click.echo(f"Team created: team_id={team_id}")

    # Create admin actor
    actor = _api("POST", f"{url}/admin/actors", "", {
        "type": "human_coordinator",
        "display_name": admin_name,
        "authority_level": admin_authority,
        "org_id": org_id,
        "team_ids": [team_id],
        "handles": list(admin_handles),
    })
    actor_id = actor.get("id", "unknown")
    click.echo(f"Admin actor created: actor_id={actor_id} (authority_level={admin_authority})")

    # Save to config
    _save_config({"actor_id": actor_id, "runtime_url": url})
    click.echo(f"Config saved: actor-id={actor_id}, runtime-url={url}")

    click.echo(f"\n{'='*60}")
    click.echo(f"ACTION REQUIRED: Set EB_ORG_ID in your environment")
    click.echo(f"  EB_ORG_ID={org_id}")
    click.echo(f"")
    click.echo(f"  Add to /etc/elephantbroker/env (or default.yaml gateway.org_id)")
    click.echo(f"  Then restart: sudo systemctl restart elephantbroker")
    click.echo(f"{'='*60}\n")


# ---------------------------------------------------------------------------
# Org
# ---------------------------------------------------------------------------

@cli.group("org")
def org_group() -> None:
    """Organization management."""


@org_group.command("create")
@click.option("--name", required=True)
@click.option("--label", default="")
@click.pass_context
def org_create(ctx: click.Context, name: str, label: str) -> None:
    """Create an organization."""
    result = _api("POST", f"{ctx.obj['runtime_url']}/admin/organizations", ctx.obj["actor_id"],
                  {"name": name, "display_label": label})
    click.echo(json.dumps(result, indent=2))


@org_group.command("list")
@click.pass_context
def org_list(ctx: click.Context) -> None:
    """List all organizations."""
    result = _api("GET", f"{ctx.obj['runtime_url']}/admin/organizations", ctx.obj["actor_id"])
    click.echo(json.dumps(result, indent=2))


# ---------------------------------------------------------------------------
# Team
# ---------------------------------------------------------------------------

@cli.group("team")
def team_group() -> None:
    """Team management."""


@team_group.command("create")
@click.option("--name", required=True)
@click.option("--label", default="")
@click.option("--org-id", required=True)
@click.pass_context
def team_create(ctx: click.Context, name: str, label: str, org_id: str) -> None:
    """Create a team."""
    result = _api("POST", f"{ctx.obj['runtime_url']}/admin/teams", ctx.obj["actor_id"],
                  {"name": name, "display_label": label, "org_id": org_id})
    click.echo(json.dumps(result, indent=2))


@team_group.command("list")
@click.option("--org-id", default=None)
@click.pass_context
def team_list(ctx: click.Context, org_id: str | None) -> None:
    """List teams."""
    url = f"{ctx.obj['runtime_url']}/admin/teams"
    if org_id:
        url += f"?org_id={org_id}"
    result = _api("GET", url, ctx.obj["actor_id"])
    click.echo(json.dumps(result, indent=2))


@team_group.command("add-member")
@click.argument("team_id")
@click.argument("actor_id")
@click.pass_context
def team_add_member(ctx: click.Context, team_id: str, actor_id: str) -> None:
    """Add actor to team."""
    result = _api("POST", f"{ctx.obj['runtime_url']}/admin/teams/{team_id}/members",
                  ctx.obj["actor_id"], {"actor_id": actor_id})
    click.echo(json.dumps(result, indent=2))


@team_group.command("remove-member")
@click.argument("team_id")
@click.argument("actor_id")
@click.pass_context
def team_remove_member(ctx: click.Context, team_id: str, actor_id: str) -> None:
    """Remove actor from team."""
    result = _api("DELETE", f"{ctx.obj['runtime_url']}/admin/teams/{team_id}/members/{actor_id}",
                  ctx.obj["actor_id"])
    click.echo(json.dumps(result, indent=2))


@team_group.command("members")
@click.argument("team_id")
@click.pass_context
def team_members(ctx: click.Context, team_id: str) -> None:
    """List team members."""
    result = _api("GET", f"{ctx.obj['runtime_url']}/admin/teams/{team_id}/members", ctx.obj["actor_id"])
    click.echo(json.dumps(result, indent=2))


# ---------------------------------------------------------------------------
# Actor
# ---------------------------------------------------------------------------

@cli.group("actor")
def actor_group() -> None:
    """Actor management."""


@actor_group.command("create")
@click.option("--display-name", required=True)
@click.option("--type", "actor_type", default="human_operator")
@click.option("--authority-level", default=0, type=int)
@click.option("--org-id", default=None)
@click.option("--team-ids", multiple=True)
@click.option("--handles", multiple=True)
@click.pass_context
def actor_create(ctx: click.Context, display_name: str, actor_type: str,
                 authority_level: int, org_id: str | None, team_ids: tuple, handles: tuple) -> None:
    """Register a new actor."""
    result = _api("POST", f"{ctx.obj['runtime_url']}/admin/actors", ctx.obj["actor_id"], {
        "type": actor_type, "display_name": display_name, "authority_level": authority_level,
        "org_id": org_id, "team_ids": list(team_ids), "handles": list(handles),
    })
    click.echo(json.dumps(result, indent=2))


@actor_group.command("list")
@click.option("--org-id", default=None)
@click.pass_context
def actor_list(ctx: click.Context, org_id: str | None) -> None:
    """List actors."""
    url = f"{ctx.obj['runtime_url']}/admin/actors"
    if org_id:
        url += f"?org_id={org_id}"
    result = _api("GET", url, ctx.obj["actor_id"])
    click.echo(json.dumps(result, indent=2))


@actor_group.command("merge")
@click.argument("canonical_id")
@click.argument("duplicate_id")
@click.pass_context
def actor_merge(ctx: click.Context, canonical_id: str, duplicate_id: str) -> None:
    """Merge duplicate actor into canonical."""
    result = _api("POST", f"{ctx.obj['runtime_url']}/admin/actors/{canonical_id}/merge",
                  ctx.obj["actor_id"], {"duplicate_id": duplicate_id})
    click.echo(json.dumps(result, indent=2))


# ---------------------------------------------------------------------------
# Profile
# ---------------------------------------------------------------------------

@cli.group("profile")
def profile_group() -> None:
    """Profile management."""


@profile_group.command("list")
@click.pass_context
def profile_list(ctx: click.Context) -> None:
    """List available profiles."""
    _api("GET", f"{ctx.obj['runtime_url']}/profiles/coding", ctx.obj.get("actor_id", ""))
    click.echo("Available profiles: coding, research, managerial, worker, personal_assistant")


@profile_group.command("resolve")
@click.argument("profile_id")
@click.pass_context
def profile_resolve(ctx: click.Context, profile_id: str) -> None:
    """Show resolved profile weights."""
    result = _api("GET", f"{ctx.obj['runtime_url']}/profiles/{profile_id}/resolve", ctx.obj.get("actor_id", ""))
    click.echo(json.dumps(result, indent=2))


@profile_group.command("override-set")
@click.argument("org_id")
@click.argument("profile_id")
@click.argument("overrides_json")
@click.pass_context
def profile_override_set(ctx: click.Context, org_id: str, profile_id: str, overrides_json: str) -> None:
    """Set org profile override (JSON string)."""
    overrides = json.loads(overrides_json)
    result = _api("PUT", f"{ctx.obj['runtime_url']}/admin/profiles/overrides/{org_id}/{profile_id}",
                  ctx.obj["actor_id"], {"overrides": overrides})
    click.echo(json.dumps(result, indent=2))


@profile_group.command("override-list")
@click.argument("org_id")
@click.pass_context
def profile_override_list(ctx: click.Context, org_id: str) -> None:
    """List org profile overrides."""
    result = _api("GET", f"{ctx.obj['runtime_url']}/admin/profiles/overrides/{org_id}", ctx.obj["actor_id"])
    click.echo(json.dumps(result, indent=2))


# ---------------------------------------------------------------------------
# Authority
# ---------------------------------------------------------------------------

@cli.group("authority")
def authority_group() -> None:
    """Authority rules management."""


@authority_group.command("list")
@click.pass_context
def authority_list(ctx: click.Context) -> None:
    """List all authority rules."""
    result = _api("GET", f"{ctx.obj['runtime_url']}/admin/authority-rules", ctx.obj.get("actor_id", ""))
    click.echo(json.dumps(result, indent=2))


@authority_group.command("set")
@click.argument("action")
@click.option("--min-level", required=True, type=int)
@click.option("--require-matching-org", is_flag=True)
@click.option("--require-matching-team", is_flag=True)
@click.option("--matching-exempt-level", default=None, type=int)
@click.pass_context
def authority_set(ctx: click.Context, action: str, min_level: int,
                  require_matching_org: bool, require_matching_team: bool,
                  matching_exempt_level: int | None) -> None:
    """Update an authority rule."""
    rule: dict = {"min_authority_level": min_level}
    if require_matching_org:
        rule["require_matching_org"] = True
    if require_matching_team:
        rule["require_matching_team"] = True
    if matching_exempt_level is not None:
        rule["matching_exempt_level"] = matching_exempt_level
    result = _api("PUT", f"{ctx.obj['runtime_url']}/admin/authority-rules/{action}",
                  ctx.obj["actor_id"], rule)
    click.echo(json.dumps(result, indent=2))


# ---------------------------------------------------------------------------
# Goal
# ---------------------------------------------------------------------------

@cli.group("goal")
def goal_group() -> None:
    """Persistent goal management."""


@goal_group.command("create")
@click.option("--title", required=True)
@click.option("--scope", default="actor", type=click.Choice(["actor", "team", "organization", "global"]))
@click.option("--org-id", default=None)
@click.option("--team-id", default=None)
@click.option("--description", default="")
@click.pass_context
def goal_create(ctx: click.Context, title: str, scope: str, org_id: str | None,
                team_id: str | None, description: str) -> None:
    """Create a persistent goal."""
    result = _api("POST", f"{ctx.obj['runtime_url']}/admin/goals", ctx.obj["actor_id"], {
        "title": title, "scope": scope, "org_id": org_id, "team_id": team_id, "description": description,
    })
    click.echo(json.dumps(result, indent=2))


@goal_group.command("list")
@click.option("--scope", default=None)
@click.option("--org-id", default=None)
@click.pass_context
def goal_list(ctx: click.Context, scope: str | None, org_id: str | None) -> None:
    """List persistent goals."""
    url = f"{ctx.obj['runtime_url']}/admin/goals"
    params = []
    if scope:
        params.append(f"scope={scope}")
    if org_id:
        params.append(f"org_id={org_id}")
    if params:
        url += "?" + "&".join(params)
    result = _api("GET", url, ctx.obj["actor_id"])
    click.echo(json.dumps(result, indent=2))


def main() -> None:
    """Entry point for ``ebrun`` console script."""
    cli()


if __name__ == "__main__":
    main()
