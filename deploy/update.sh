#!/usr/bin/env bash
# =============================================================================
# ElephantBroker DB-VM updater
# =============================================================================
#
# In-place upgrade for an existing install. Pulls the latest source from git,
# syncs the venv to match the lockfile via `uv sync`, re-chowns the install
# tree, and restarts both systemd services.
#
# Usage:
#   sudo /opt/elephantbroker/deploy/update.sh           # default: uv sync --frozen
#   sudo /opt/elephantbroker/deploy/update.sh --upgrade # regenerate uv.lock first
#
# This script is idempotent and runs entirely as root (no sudo -u switching).
# It refuses to run on a dirty git tree so the operator does not lose
# uncommitted local changes.
#
# Default behavior (no --upgrade flag):
#   1. git pull --ff-only origin <current-branch>
#   2. uv sync --frozen --no-dev (installs EXACTLY what uv.lock specifies)
#   3. verify EB_HITL_CALLBACK_SECRET is populated in env + hitl.env
#      (Bucket C-R2, TODO-3-623 — detects upgrades from a pre-F11 install
#      that never got the auto-gen, warn with the fix command)
#   4. config validate against the runtime schema (matches install.sh C4
#      — Bucket C-R2, TODO-3-621; hard-dies on failure so a broken
#      upgrade never reaches `systemctl restart`)
#   5. re-chown ONLY the Cognee writable subdirs (NOT a recursive $PREFIX
#      chown — see C3/TODO-3-010); $PREFIX itself stays root-owned
#   6. re-install systemd unit files from $PREFIX/deploy/systemd/ (Bucket
#      C-R2, TODO-3-622 — ensures unit-file edits in the repo actually
#      land on target hosts; skipped if no unit file is currently
#      registered, mirroring the `--no-systemd` install path)
#   7. restart services
#
# With --upgrade:
#   - git pull
#   - uv lock --upgrade (regenerates uv.lock from current pyproject.toml,
#     picking the latest versions allowed by the constraints)
#   - uv sync --no-dev (installs the new lockfile)
#   - then steps 3-7 same as above
#   - This is the path for "I bumped a version in pyproject.toml" workflows.
#     See deploy/UPDATING-DEPS.md for the full upgrade procedure.
#
# Flags:
#   --upgrade        Regenerate uv.lock before syncing. Use when a new
#                    dependency was added or a version was bumped in
#                    pyproject.toml.
#   --no-restart     Do not restart systemd services after install (useful for
#                    multi-step upgrades, or when running on a host with no
#                    systemd units installed).
#   --prefix PATH    Override the install prefix (default: /opt/elephantbroker)
#   --help           Show this message
# =============================================================================

set -euo pipefail

# --- Defaults ---
PREFIX="/opt/elephantbroker"
SERVICE_USER="elephantbroker"
SERVICE_GROUP="elephantbroker"
CONFIG_DIR="/etc/elephantbroker"
UPGRADE_LOCK=0
RESTART=1

# --- Parse flags ---
while [[ $# -gt 0 ]]; do
    case "$1" in
        --upgrade) UPGRADE_LOCK=1; shift ;;
        --no-restart) RESTART=0; shift ;;
        --prefix) PREFIX="$2"; shift 2 ;;
        --help|-h)
            cat <<'HELP'
ElephantBroker DB-VM updater

Usage:
  sudo ./update.sh [--upgrade] [--no-restart] [--prefix PATH]

Flags:
  --upgrade        Regenerate uv.lock before syncing. Use when a new dependency
                   was added or a version was bumped in pyproject.toml. WITHOUT
                   --upgrade, the script installs EXACTLY what uv.lock specifies
                   (frozen mode) — the safe default for in-place updates.
  --no-restart     Do not restart systemd services after install.
  --prefix PATH    Override install prefix (default: /opt/elephantbroker)
  --help, -h       Show this message

Default behavior (no --upgrade):
  1. git pull --ff-only origin <current-branch>
  2. uv sync --frozen --no-dev (installs EXACTLY what uv.lock specifies)
  3. verify EB_HITL_CALLBACK_SECRET is populated in env + hitl.env
  4. config validate against the runtime schema (hard-dies on failure)
  5. chown ONLY the Cognee writable subdirs (.cognee_system, .data_storage,
     .anon_id) to elephantbroker:elephantbroker — $PREFIX itself stays
     root-owned for defense in depth (see install.sh step 6 + C3 comment)
  6. re-install systemd unit files from $PREFIX/deploy/systemd/
  7. systemctl restart elephantbroker elephantbroker-hitl

With --upgrade:
  1. git pull --ff-only origin <current-branch>
  2. uv lock --upgrade (regenerate the lockfile) + uv sync --no-dev
  3-7. Same as default behavior above

The script refuses to run on a dirty git tree.
See deploy/UPDATING-DEPS.md for the full dep upgrade procedure.
HELP
            exit 0
            ;;
        *)
            echo "ERROR: unknown flag: $1" >&2
            echo "Run with --help for usage." >&2
            exit 1
            ;;
    esac
done

# --- Helpers ---
log()  { printf "\033[1;34m==>\033[0m %s\n" "$*"; }
warn() { printf "\033[1;33m!!\033[0m  %s\n" "$*" >&2; }
die()  { printf "\033[1;31mXX\033[0m  %s\n" "$*" >&2; exit 1; }

# --- Pre-flight ---
[[ $EUID -eq 0 ]] || die "must run as root (use sudo)"
[[ -d "$PREFIX/.git" ]] || die "$PREFIX is not a git working tree — was the repo cloned in place?"
[[ -d "$PREFIX/.venv" ]] || die "$PREFIX/.venv not found — run install.sh first"
command -v uv &>/dev/null || die "uv not found in PATH — run install.sh first (it installs uv)"

cd "$PREFIX"

# Refuse dirty tree (operator might lose uncommitted changes)
if ! git diff-index --quiet HEAD -- 2>/dev/null; then
    warn "Working tree at $PREFIX has uncommitted changes:"
    git status --short
    warn ""
    warn "TODO-3-634: if uv.lock is listed above and you are recovering"
    warn "from a previous --upgrade run that failed validation, revert"
    warn "uv.lock first:"
    warn "    sudo git -C $PREFIX checkout uv.lock"
    warn "then re-run this script."
    die "refusing to update on a dirty tree — commit or stash your changes first"
fi

CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
log "Install prefix: $PREFIX"
log "Current branch: $CURRENT_BRANCH"

# =============================================================================
log "Step 1/7: git pull"
# =============================================================================
BEFORE_SHA="$(git rev-parse HEAD)"
git pull --ff-only origin "$CURRENT_BRANCH"
AFTER_SHA="$(git rev-parse HEAD)"
if [[ "$BEFORE_SHA" == "$AFTER_SHA" ]]; then
    log "  already up to date ($AFTER_SHA)"
else
    log "  $BEFORE_SHA -> $AFTER_SHA"
fi

# =============================================================================
log "Step 2/7: uv sync"
# =============================================================================
if [[ "$UPGRADE_LOCK" -eq 1 ]]; then
    log "  --upgrade flag: regenerating uv.lock from pyproject.toml"
    uv lock --upgrade
    log "  uv sync --no-dev"
    uv sync --no-dev
else
    log "  uv sync --frozen --no-dev (installs exactly what uv.lock specifies)"
    uv sync --frozen --no-dev
fi

# Workspace mode: hitl-middleware is a [tool.uv.workspace] member of the
# root pyproject.toml, so the `uv sync` above already covers it. Before
# the workspace conversion this script ran a separate `uv pip install` —
# that bypassed the lockfile entirely and let the HITL service drift
# from the runtime on every update.

# Cognee writable directories: re-create in case a fresh sync wiped them.
#
# C8 (TODO-3-325): resolve the venv site-packages dir via Python's stdlib
# instead of the brittle `find ... | head -n 1` form (matches the same
# rewrite in install.sh step 4). The new approach asks the venv's own
# Python where its site-packages live — authoritative, no maxdepth guess.
SITE_PACKAGES=$(uv run python -c 'import site; print(site.getsitepackages()[0])' 2>/dev/null || true)
if [[ -n "$SITE_PACKAGES" && -d "$SITE_PACKAGES" ]]; then
    COGNEE_DIR="$SITE_PACKAGES/cognee"
    if [[ -d "$COGNEE_DIR" ]]; then
        mkdir -p "$COGNEE_DIR/.cognee_system/databases" "$COGNEE_DIR/.data_storage"
        # H4 (TODO-3-606): re-touch the .anon_id sentinel after every sync.
        # A fresh `uv sync` may reinstall cognee without (re)creating its
        # telemetry sentinel; without this, Cognee starts up unable to
        # write its anon_id and the chown step below silently skips it
        # (its existing `[[ -e "$ANON_ID_PATH" ]]` guard hides the gap).
        # Mirrors install.sh step 4c, same form, same path resolution.
        ANON_ID_PATH="$SITE_PACKAGES/.anon_id"
        touch "$ANON_ID_PATH"
        chmod 644 "$ANON_ID_PATH"
        log "  cognee anon_id touched: $ANON_ID_PATH"
    fi
else
    warn "  could not resolve venv site-packages dir — Cognee writable paths may be stale"
    SITE_PACKAGES=""
    COGNEE_DIR=""
fi

# =============================================================================
log "Step 3/7: verify EB_HITL_CALLBACK_SECRET is populated"
# =============================================================================
# TODO-3-623 (Bucket C-R2): on update.sh we do NOT auto-generate the HITL
# secret (install.sh F11 owns that — auto-gen on fresh install only, to
# avoid clobbering an existing operator-rotated value on subsequent runs).
# But an upgrade path from a pre-F11 install.sh version, or from a manual
# clone-and-go deployment, can leave the placeholder intact in one or
# both files. If that happens the runtime starts cleanly but every HITL
# approval callback fails HMAC verification with an opaque "signature
# mismatch" error that's painful to diagnose. Detect the placeholder
# here and warn loudly — the fix is a one-liner but it has to be applied
# before the restart below or the failure is invisible until a HITL
# request arrives.
for env_file in "$CONFIG_DIR/env" "$CONFIG_DIR/hitl.env"; do
    if [[ ! -f "$env_file" ]]; then
        warn "  $env_file missing — re-run install.sh to populate it"
        continue
    fi
    if grep -q "^EB_HITL_CALLBACK_SECRET=$" "$env_file"; then
        warn "  $env_file still has the bare EB_HITL_CALLBACK_SECRET= placeholder."
        warn "  HITL HMAC verification will fail until this is populated."
        warn "  Fix: set the SAME random value in BOTH env + hitl.env, e.g."
        warn "      secret=\$(openssl rand -hex 32)"
        warn "      sudo sed -i \"s|^EB_HITL_CALLBACK_SECRET=\$|EB_HITL_CALLBACK_SECRET=\$secret|\" \\"
        warn "          $CONFIG_DIR/env $CONFIG_DIR/hitl.env"
        warn "      sudo systemctl restart elephantbroker-hitl"
    fi
done
log "  EB_HITL_CALLBACK_SECRET presence check complete"

# =============================================================================
log "Step 4/7: validate $CONFIG_DIR/default.yaml against the runtime schema"
# =============================================================================
# TODO-3-621 (Bucket C-R2): mirror install.sh C4 (TODO-3-013, TODO-3-222)
# — validate the on-disk config BEFORE restarting services. This calls the
# same ElephantBrokerConfig.load() the runtime uses at startup, so any
# structural failure surfaces here as a clear update-log error instead of
# a confusing journalctl failure 30 seconds after `systemctl restart`.
#
# This matters MORE for update.sh than for install.sh: on update, the
# previous (working) version of the runtime is still running. If we
# restart into a broken config, we take down a working production
# service. Hard-die BEFORE the restart so the operator fixes the config
# while the old process is still serving traffic.
if [[ ! -f "$CONFIG_DIR/default.yaml" ]]; then
    warn "  $CONFIG_DIR/default.yaml is MISSING entirely."
    warn ""
    warn "  This is strictly worse than a schema violation:"
    warn "    - a schema violation means the YAML is broken but exists"
    warn "      (fixable by editing the file)"
    warn "    - a missing YAML means the runtime will start with ZERO"
    warn "      on-disk config and fall through to env vars + compiled"
    warn "      defaults. Any operator-specific YAML tuning (gateway_id,"
    warn "      org_id, team_id, profile weights, cognee: block, etc.)"
    warn "      is silently lost on restart."
    warn ""
    warn "  Recovery (TODO-3-635):"
    warn "    - run install.sh to repopulate $CONFIG_DIR from the template"
    warn "    - restore any operator edits to default.yaml from backup"
    warn "    - re-run $PREFIX/deploy/update.sh"
    warn ""
    warn "  The OLD runtime is still running — this failure did NOT"
    warn "  restart any services, so traffic is still being served."
    die "$CONFIG_DIR/default.yaml missing — refusing to restart services (TODO-3-635)"
else
    if "$PREFIX/.venv/bin/elephantbroker" config validate \
            --config "$CONFIG_DIR/default.yaml" 2>/tmp/eb-validate.err; then
        log "  config validate ✓ ($CONFIG_DIR/default.yaml)"
    else
        warn "  config validate FAILED — dumping errors:"
        while IFS= read -r line; do warn "    $line"; done < /tmp/eb-validate.err
        warn ""
        warn "  Common causes:"
        warn "    - upgraded runtime rejects an old config field (extra='forbid')"
        warn "    - embedding model / dimension drifted in the cognee: block"
        warn "    - env var referenced in YAML is no longer exported"
        warn ""
        warn "  Recovery:"
        warn "    - edit $CONFIG_DIR/default.yaml to match the new schema"
        warn "    - re-run $PREFIX/deploy/update.sh (idempotent)"
        warn "    - the OLD runtime is still running — this failure did NOT"
        warn "      restart any services, so traffic is still being served"
        if [[ "$UPGRADE_LOCK" -eq 1 ]]; then
            warn ""
            warn "  NOTE (TODO-3-634): this run used --upgrade, so uv.lock was"
            warn "  regenerated in Step 2. The updated uv.lock is now dirty in"
            warn "  the working tree and will block the next update.sh run at"
            warn "  the dirty-tree check. To recover AFTER fixing the config:"
            warn "      sudo git -C $PREFIX checkout uv.lock    # revert uv.lock"
            warn "      sudo $PREFIX/deploy/update.sh --upgrade  # retry"
            warn "  (or, if you verified the lock upgrade is correct, commit"
            warn "  uv.lock first and then re-run update.sh without --upgrade)"
        fi
        die "config validate failed — refusing to restart services with a broken config"
    fi
fi

# =============================================================================
log "Step 5/7: re-apply ownership of writable subdirs only"
# =============================================================================
# C3 (TODO-3-010): the previous version did `chown -R $SERVICE_USER $PREFIX`
# which gave the runtime user write access to its own source code and venv
# binaries. The narrowed model (matching install.sh step 6) only chowns the
# Cognee runtime subdirs to the service user; everything else stays root-
# owned and is read+executed via "other" file mode bits (644/755).
#
# `uv sync` may have re-created the .cognee_system / .data_storage paths if
# Cognee was upgraded (the new install includes a fresh tree). Re-chown
# exactly the same set of paths install.sh chowns in its step 6.
if [[ -n "$COGNEE_DIR" && -d "$COGNEE_DIR" ]]; then
    ANON_ID_PATH="$SITE_PACKAGES/.anon_id"
    chown -R "$SERVICE_USER:$SERVICE_GROUP" "$COGNEE_DIR/.cognee_system"
    chown -R "$SERVICE_USER:$SERVICE_GROUP" "$COGNEE_DIR/.data_storage"
    if [[ -e "$ANON_ID_PATH" ]]; then
        chown "$SERVICE_USER:$SERVICE_GROUP" "$ANON_ID_PATH"
    fi
    log "  chowned $COGNEE_DIR/.cognee_system  → $SERVICE_USER:$SERVICE_GROUP"
    log "  chowned $COGNEE_DIR/.data_storage   → $SERVICE_USER:$SERVICE_GROUP"
    log "  $PREFIX itself remains root-owned (defense in depth)"
else
    warn "  COGNEE_DIR was not located in step 2 — skipping targeted chown"
    warn "  re-run install.sh if Cognee paths are missing from the venv"
fi

# =============================================================================
log "Step 6/7: re-install systemd unit files"
# =============================================================================
# TODO-3-622 (Bucket C-R2): unit-file edits in $PREFIX/deploy/systemd/ are
# pulled by `git pull` in step 1 but never land on /etc/systemd/system/
# without an explicit re-install. This matters whenever we change hardening
# options (MemoryMax, ProtectSystem, ReadWritePaths, CAPABILITY drops) or
# the ExecStart line for a new CLI entry point — the repo has the new unit,
# but systemd keeps serving the old one until daemon-reload sees a fresh
# file on disk.
#
# Only re-install if the unit is ALREADY registered. Operators who
# installed with --no-systemd don't want update.sh sneaking a systemd
# unit back in behind their backs; mirroring the "is the unit registered"
# guard in step 7 keeps the two paths symmetric.
SYSTEMD_TOUCHED=0
if systemctl list-unit-files elephantbroker.service &>/dev/null; then
    if [[ -f "$PREFIX/deploy/systemd/elephantbroker.service" ]]; then
        install -o root -g root -m 644 \
            "$PREFIX/deploy/systemd/elephantbroker.service" \
            /etc/systemd/system/elephantbroker.service
        log "  re-installed /etc/systemd/system/elephantbroker.service"
        SYSTEMD_TOUCHED=1
    else
        warn "  $PREFIX/deploy/systemd/elephantbroker.service missing in repo"
    fi
else
    log "  elephantbroker.service not registered — skipping (--no-systemd install?)"
fi
if systemctl list-unit-files elephantbroker-hitl.service &>/dev/null; then
    if [[ -f "$PREFIX/deploy/systemd/elephantbroker-hitl.service" ]]; then
        install -o root -g root -m 644 \
            "$PREFIX/deploy/systemd/elephantbroker-hitl.service" \
            /etc/systemd/system/elephantbroker-hitl.service
        log "  re-installed /etc/systemd/system/elephantbroker-hitl.service"
        SYSTEMD_TOUCHED=1
    else
        warn "  $PREFIX/deploy/systemd/elephantbroker-hitl.service missing in repo"
    fi
else
    log "  elephantbroker-hitl.service not registered — skipping"
fi
if [[ "$SYSTEMD_TOUCHED" -eq 1 ]]; then
    systemctl daemon-reload
    log "  systemctl daemon-reload"
fi

# =============================================================================
log "Step 7/7: restart services"
# =============================================================================
if [[ "$RESTART" -eq 0 ]]; then
    log "  --no-restart flag set — skipping (run 'systemctl restart elephantbroker' manually)"
else
    if systemctl list-unit-files elephantbroker.service &>/dev/null; then
        systemctl restart elephantbroker
        log "  restarted elephantbroker"
    else
        warn "  elephantbroker.service not installed — skipping"
    fi
    if systemctl list-unit-files elephantbroker-hitl.service &>/dev/null; then
        systemctl restart elephantbroker-hitl
        log "  restarted elephantbroker-hitl"
    else
        warn "  elephantbroker-hitl.service not installed — skipping"
    fi
fi

# =============================================================================
log "Update complete."
# =============================================================================
cat <<EOF

Verify:
  systemctl status elephantbroker elephantbroker-hitl
  curl http://localhost:8420/health/    # note trailing slash
  curl http://localhost:8421/health
  journalctl -u elephantbroker -n 50

EOF
