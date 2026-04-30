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
#   3. rebuild TypeScript plugins (openclaw-plugins/*) — runs `npm ci`
#      + `npm run build` per plugin. Skipped with warning if npm is
#      not on PATH (dedicated DB-VM deployments don't need plugins
#      rebuilt here; co-located deployments do).
#   4. verify EB_HITL_CALLBACK_SECRET is populated in env + hitl.env
#      (Bucket C-R2, TODO-3-623 — detects upgrades from a pre-F11 install
#      that never got the auto-gen, warn with the fix command)
#   5. config validate against the runtime schema (matches install.sh C4
#      — Bucket C-R2, TODO-3-621; hard-dies on failure so a broken
#      upgrade never reaches `systemctl restart`)
#   6. re-chown ONLY the Cognee writable subdirs (NOT a recursive $PREFIX
#      chown — see C3/TODO-3-010); $PREFIX itself stays root-owned
#   7. re-install systemd unit files from $PREFIX/deploy/systemd/ (Bucket
#      C-R2, TODO-3-622 — ensures unit-file edits in the repo actually
#      land on target hosts; skipped if no unit file is currently
#      registered, mirroring the `--no-systemd` install path)
#   8. restart services
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
#   --skip-plugins   Skip the TypeScript plugin rebuild (Step 3). Intended as
#                    an operator escape hatch for dedicated DB-VM runs where
#                    the plugin rebuild is known-irrelevant, or for recovery
#                    scenarios where npm is broken on the target host and the
#                    plugin rebuild would stall the update. Plugins retain
#                    whatever dist/ they had before the run.
#   --prefix PATH    Override the install prefix (default: /opt/elephantbroker)
#   --help           Show this message
# =============================================================================

set -euo pipefail

# --- Defaults ---
PREFIX="/opt/elephantbroker"
SERVICE_USER="elephantbroker"
SERVICE_GROUP="elephantbroker"
# Systemd unit names. Override precedence: --service-name flag > EB_SERVICE_NAME
# env > default ("elephantbroker"). HITL_SERVICE_NAME defaults to
# "${SERVICE_NAME}-hitl" so `--service-name foo` automatically targets the
# "foo-hitl" HITL unit unless --hitl-service-name (or EB_HITL_SERVICE_NAME)
# explicitly overrides. The HITL name is left as a sentinel here and resolved
# AFTER flag parsing so the auto-derive sees the flag-set SERVICE_NAME.
# An operator who installed with custom names must pass the same flags here
# (or set the same env vars) so the script targets the matching unit files.
SERVICE_NAME="${EB_SERVICE_NAME:-elephantbroker}"
HITL_SERVICE_NAME="${EB_HITL_SERVICE_NAME:-}"  # sentinel; resolved post-parse
CONFIG_DIR="/etc/elephantbroker"
UPGRADE_LOCK=0
RESTART=1
SKIP_PLUGINS=0

# --- Parse flags ---
while [[ $# -gt 0 ]]; do
    case "$1" in
        --upgrade) UPGRADE_LOCK=1; shift ;;
        --no-restart) RESTART=0; shift ;;
        --skip-plugins) SKIP_PLUGINS=1; shift ;;
        --prefix) PREFIX="$2"; shift 2 ;;
        --service-name) SERVICE_NAME="$2"; shift 2 ;;
        --hitl-service-name) HITL_SERVICE_NAME="$2"; shift 2 ;;
        --help|-h)
            cat <<'HELP'
ElephantBroker DB-VM updater

Usage:
  sudo ./update.sh [--upgrade] [--no-restart] [--skip-plugins] [--prefix PATH]
                   [--service-name NAME] [--hitl-service-name NAME]

Flags:
  --upgrade        Regenerate uv.lock before syncing. Use when a new dependency
                   was added or a version was bumped in pyproject.toml. WITHOUT
                   --upgrade, the script installs EXACTLY what uv.lock specifies
                   (frozen mode) — the safe default for in-place updates.
  --no-restart     Do not restart systemd services after install.
  --skip-plugins   Skip the TypeScript plugin rebuild step. Escape hatch for
                   dedicated DB-VM runs or npm-broken recovery scenarios.
  --prefix PATH    Override install prefix (default: /opt/elephantbroker)
  --service-name NAME
                   Override the runtime systemd unit name (must match the name
                   used at install time). Default: "elephantbroker"; env:
                   EB_SERVICE_NAME.
  --hitl-service-name NAME
                   Override the HITL systemd unit name. Default:
                   "<SERVICE_NAME>-hitl"; env: EB_HITL_SERVICE_NAME.
  --help, -h       Show this message

Default behavior (no --upgrade):
  1. git pull --ff-only origin <current-branch>
  2. uv sync --frozen --no-dev (installs EXACTLY what uv.lock specifies)
  3. rebuild TypeScript plugins (npm ci + npm run build per plugin dir);
     skipped with warning if npm is not on PATH
  4. verify EB_HITL_CALLBACK_SECRET is populated in env + hitl.env
  5. config validate against the runtime schema (hard-dies on failure)
  6. chown ONLY the Cognee writable subdirs (.cognee_system, .data_storage,
     .anon_id) to elephantbroker:elephantbroker — $PREFIX itself stays
     root-owned for defense in depth (see install.sh step 6 + C3 comment)
  7. re-install systemd unit files from $PREFIX/deploy/systemd/
  8. systemctl restart elephantbroker elephantbroker-hitl

With --upgrade:
  1. git pull --ff-only origin <current-branch>
  2. uv lock --upgrade (regenerate the lockfile) + uv sync --no-dev
  3-8. Same as default behavior above

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

# Resolve HITL_SERVICE_NAME from the final SERVICE_NAME if neither
# --hitl-service-name nor EB_HITL_SERVICE_NAME was specified. This is what
# makes `--service-name foo` automatically target a "foo-hitl" HITL unit.
[[ -z "$HITL_SERVICE_NAME" ]] && HITL_SERVICE_NAME="${SERVICE_NAME}-hitl"

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
log "Step 1/8: git pull"
# =============================================================================
BEFORE_SHA="$(git rev-parse HEAD)"
git pull --ff-only origin "$CURRENT_BRANCH"
AFTER_SHA="$(git rev-parse HEAD)"
if [[ "$BEFORE_SHA" == "$AFTER_SHA" ]]; then
    log "  already up to date ($AFTER_SHA)"
else
    log "  $BEFORE_SHA -> $AFTER_SHA"
fi

# --- Self-update guard ---
# When update.sh runs, bash loads the OLD script into memory at startup.
# git pull (above) replaces the file on disk, but bash keeps executing the
# stale in-memory copy. Any fix that lands IN update.sh itself (e.g. the
# --all-packages fix in ec7ee67) only takes effect on a SECOND run — the
# first run still executes the pre-pull version. We hit this during the
# 2026-04-09 staging deploy: ec7ee67 was on disk but the in-memory old
# script ran without --all-packages, dropping hitl-middleware and crashing
# elephantbroker-hitl with 203/EXEC.
#
# Fix: after git pull, check if deploy/update.sh itself was modified in the
# pulled commits. If yes AND the EB_UPDATE_REEXECED env guard is unset,
# re-execute the script from the new on-disk version via exec "$0" "$@".
# The guard env var prevents infinite recursion — the re-exec'd instance
# sees EB_UPDATE_REEXECED=1 and skips the check.
#
# HEAD@{1} may not exist on a fresh clone (no reflog entry yet). In that
# case, skip the self-exec check — fresh clones don't have a stale script
# in memory since the user just cloned the latest version.
if [[ -z "${EB_UPDATE_REEXECED:-}" ]]; then
    PREV_HEAD="$(git rev-parse --verify HEAD@{1} 2>/dev/null || true)"
    if [[ -n "$PREV_HEAD" ]] && git diff --name-only "$PREV_HEAD"..HEAD -- deploy/update.sh | grep -q .; then
        log "update.sh changed in this pull — re-executing from new version"
        export EB_UPDATE_REEXECED=1
        exec "$0" "$@"
    fi
fi

# =============================================================================
log "Step 2/8: uv sync"
# =============================================================================
# --all-packages is REQUIRED because hitl-middleware is a workspace member but
# not a dependency of the root elephantbroker project; without this flag
# `uv sync` skips it and /opt/elephantbroker/.venv/bin/hitl-middleware is
# never created, breaking the elephantbroker-hitl systemd unit with
# status=203/EXEC on every fresh install. Discovered during the first
# staging install of PR #3 (post-merge) on 2026-04-09.
if [[ "$UPGRADE_LOCK" -eq 1 ]]; then
    log "  --upgrade flag: regenerating uv.lock from pyproject.toml"
    uv lock --upgrade
    log "  uv sync --no-dev --all-packages"
    uv sync --no-dev --all-packages
else
    log "  uv sync --frozen --no-dev --all-packages (installs exactly what uv.lock specifies)"
    uv sync --frozen --no-dev --all-packages
fi

# Workspace mode: hitl-middleware is a [tool.uv.workspace] member of the
# root pyproject.toml, so the `uv sync --all-packages` above covers it in
# one invocation. Before the workspace conversion this script ran a
# separate `uv pip install` — that bypassed the lockfile entirely and let
# the HITL service drift from the runtime on every update. Without
# `--all-packages` the workspace member is silently skipped and the HITL
# binary is never installed; see the inline comment on the `uv sync`
# invocations above for the full regression history.

# =============================================================================
log "Step 3/8: rebuild TypeScript plugins"
# =============================================================================
# After git pull, any TS plugin source changes (openclaw-plugins/*/src/*.ts)
# need their bundled dist/ rebuilt — OpenClaw loads from dist/index.js per
# each plugin's package.json `main` + `openclaw.extensions`. Without this
# step, operators have to remember to cd into each plugin dir and run
# `npm ci && npm run build` manually after every pull. Surfaced during
# TD-60 rollout (2026-04-19): the memory plugin envelope-strip fix was in
# src/ but dist/ was stale until a manual rebuild — the fix appeared to
# not land.
#
# dist/ is in .gitignore (per plugin .gitignore), so a fresh clone never
# has it. npm ci builds deterministically from package-lock.json.
#
# This step is best-effort: if npm is not on PATH (dedicated DB-VM with
# no Node toolchain — OpenClaw runs on a separate host), warn and skip
# rather than fail the update. Co-located deployments (where the EB
# runtime and the OpenClaw gateway share one host) need the rebuild to
# land here; dedicated DB-VM deployments build plugins on the gateway
# host via its own update path.
PLUGINS_DIR="$PREFIX/openclaw-plugins"
PLUGIN_FAILURES=()  # TODO-5-108: track failures so the summary at end of Step 3
                    # is loud and enumerated, not silent-continue.
if [[ "$SKIP_PLUGINS" -eq 1 ]]; then
    # TODO-5-311: operator escape hatch. Used when the target host genuinely
    # has no working npm, or when the operator explicitly wants to keep the
    # existing dist/ (e.g. recovering from a botched upstream release).
    warn "  --skip-plugins flag set — skipping plugin rebuild entirely"
    warn "  plugins retain whatever dist/ they had before this run"
elif ! command -v npm &>/dev/null; then
    # TODO-5-007: npm-missing is a plausible state (dedicated DB-VM builds
    # plugins on a separate gateway host) — do not fail-fast. But do make
    # the warning loud enough that a co-located deployment operator cannot
    # miss it, since in that topology a stale dist/ is a real bug.
    warn "================================================================"
    warn "  npm NOT FOUND on PATH — plugin rebuild SKIPPED"
    warn "================================================================"
    warn "  dist/ in $PLUGINS_DIR/elephantbroker-* MAY BE STALE."
    warn ""
    warn "  This is expected on dedicated DB-VM deployments (plugins are"
    warn "  built on the OpenClaw gateway host via its own update path)."
    warn ""
    warn "  On a CO-LOCATED deployment (EB runtime + OpenClaw gateway on"
    warn "  one host), this means TS source changes in this pull have NOT"
    warn "  been bundled. Install Node.js and re-run update.sh, or pass"
    warn "  --skip-plugins to acknowledge the skip is intentional."
    warn "================================================================"
elif [[ ! -d "$PLUGINS_DIR" ]]; then
    warn "  $PLUGINS_DIR missing — skipping plugin rebuild"
else
    for plugin in "$PLUGINS_DIR"/elephantbroker-*; do
        [[ -d "$plugin" ]] || continue
        [[ -f "$plugin/package.json" ]] || continue
        plugin_name=$(basename "$plugin")
        log "  building $plugin_name"
        # TODO-5-208: npm runs as root here. The entire script enforces
        # `[[ $EUID -eq 0 ]]` at pre-flight, so there is no non-root
        # invocation path — node_modules/ and dist/ end up root-owned,
        # consistent with the rest of the git tree (install.sh clones as
        # root too). Plugins are read-only at runtime, so root ownership
        # is fine. Previous comment here claimed "Run as non-root" — that
        # was aspirational, never matched the code, and is now corrected.
        #
        # TODO-5-108: previous version used `... --silent 2>&1 | tail -5`
        # which suppressed npm's own progress AND truncated its error
        # output to the last 5 lines on failure — dropping the real
        # error message mid-stream. New shape: capture full output to a
        # per-plugin tmp file, echo concise tail on success, dump FULL
        # contents on failure so operators can diagnose without having
        # to re-run manually.
        BUILD_LOG=$(mktemp -t "eb-plugin-${plugin_name}.XXXXXX")
        if ! (cd "$plugin" && npm ci) >"$BUILD_LOG" 2>&1; then
            warn "    npm ci FAILED in $plugin_name — dumping full output:"
            while IFS= read -r line; do warn "      $line"; done < "$BUILD_LOG"
            PLUGIN_FAILURES+=("$plugin_name (npm ci)")
            rm -f "$BUILD_LOG"
            continue
        fi
        if ! (cd "$plugin" && npm run build) >"$BUILD_LOG" 2>&1; then
            warn "    npm run build FAILED in $plugin_name — dist/ is stale. Full output:"
            while IFS= read -r line; do warn "      $line"; done < "$BUILD_LOG"
            PLUGIN_FAILURES+=("$plugin_name (npm run build)")
            rm -f "$BUILD_LOG"
            continue
        fi
        rm -f "$BUILD_LOG"
        log "    ✓ $plugin_name/dist/index.js rebuilt"
    done
    # TODO-5-108: summary after the loop. If any plugin failed the summary
    # is unmissable (multi-line banner), and the failing plugin names are
    # enumerated so the operator knows exactly which dist/ is stale.
    if [[ ${#PLUGIN_FAILURES[@]} -gt 0 ]]; then
        warn "================================================================"
        warn "  PLUGIN REBUILD FAILURES (${#PLUGIN_FAILURES[@]}):"
        for failure in "${PLUGIN_FAILURES[@]}"; do
            warn "    - $failure"
        done
        warn "  Affected dist/ bundles are STALE. Fix the underlying error"
        warn "  (npm output dumped above) and re-run update.sh, or pass"
        warn "  --skip-plugins if the staleness is acceptable for this run."
        warn "================================================================"
    fi
fi

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
log "Step 4/8: verify EB_HITL_CALLBACK_SECRET is populated"
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
        warn "      sudo systemctl restart $HITL_SERVICE_NAME"
    fi
done
log "  EB_HITL_CALLBACK_SECRET presence check complete"

# =============================================================================
log "Step 5/8: validate $CONFIG_DIR/default.yaml against the runtime schema"
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
log "Step 6/8: re-apply ownership of writable subdirs only"
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
log "Step 7/8: re-install systemd unit files"
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
# Sed-transform mirrors install.sh Step 7: line-anchored patterns rewrite the
# live SyslogIdentifier / After= / Wants= directives ONLY (comments referencing
# the default names are left untouched). User=/Group= remain controlled by
# SERVICE_USER / SERVICE_GROUP — separate concern, not rewritten here.
if systemctl list-unit-files "${SERVICE_NAME}.service" &>/dev/null; then
    if [[ -f "$PREFIX/deploy/systemd/elephantbroker.service" ]]; then
        sed \
            -e "s|^SyslogIdentifier=elephantbroker$|SyslogIdentifier=$SERVICE_NAME|" \
            "$PREFIX/deploy/systemd/elephantbroker.service" \
            > "/etc/systemd/system/${SERVICE_NAME}.service"
        chmod 644 "/etc/systemd/system/${SERVICE_NAME}.service"
        chown root:root "/etc/systemd/system/${SERVICE_NAME}.service"
        log "  re-installed /etc/systemd/system/${SERVICE_NAME}.service"
        SYSTEMD_TOUCHED=1
    else
        warn "  $PREFIX/deploy/systemd/elephantbroker.service missing in repo"
    fi
else
    log "  ${SERVICE_NAME}.service not registered — skipping (--no-systemd install?)"
fi
if systemctl list-unit-files "${HITL_SERVICE_NAME}.service" &>/dev/null; then
    if [[ -f "$PREFIX/deploy/systemd/elephantbroker-hitl.service" ]]; then
        sed \
            -e "s|^SyslogIdentifier=elephantbroker-hitl$|SyslogIdentifier=$HITL_SERVICE_NAME|" \
            -e "s|^After=elephantbroker\.service|After=${SERVICE_NAME}.service|" \
            -e "s|^Wants=\(.*\)elephantbroker\.service|Wants=\1${SERVICE_NAME}.service|" \
            "$PREFIX/deploy/systemd/elephantbroker-hitl.service" \
            > "/etc/systemd/system/${HITL_SERVICE_NAME}.service"
        chmod 644 "/etc/systemd/system/${HITL_SERVICE_NAME}.service"
        chown root:root "/etc/systemd/system/${HITL_SERVICE_NAME}.service"
        log "  re-installed /etc/systemd/system/${HITL_SERVICE_NAME}.service"
        SYSTEMD_TOUCHED=1
    else
        warn "  $PREFIX/deploy/systemd/elephantbroker-hitl.service missing in repo"
    fi
else
    log "  ${HITL_SERVICE_NAME}.service not registered — skipping"
fi
if [[ "$SYSTEMD_TOUCHED" -eq 1 ]]; then
    systemctl daemon-reload
    log "  systemctl daemon-reload"
fi

# =============================================================================
log "Step 8/8: restart services"
# =============================================================================
if [[ "$RESTART" -eq 0 ]]; then
    log "  --no-restart flag set — skipping (run 'systemctl restart $SERVICE_NAME' manually)"
else
    if systemctl list-unit-files "${SERVICE_NAME}.service" &>/dev/null; then
        systemctl restart "$SERVICE_NAME"
        log "  restarted $SERVICE_NAME"
    else
        warn "  ${SERVICE_NAME}.service not installed — skipping"
    fi
    if systemctl list-unit-files "${HITL_SERVICE_NAME}.service" &>/dev/null; then
        systemctl restart "$HITL_SERVICE_NAME"
        log "  restarted $HITL_SERVICE_NAME"
    else
        warn "  ${HITL_SERVICE_NAME}.service not installed — skipping"
    fi
fi

# =============================================================================
log "Update complete."
# =============================================================================
cat <<EOF

Verify:
  systemctl status $SERVICE_NAME $HITL_SERVICE_NAME
  curl http://localhost:8420/health/    # note trailing slash
  curl http://localhost:8421/health
  journalctl -u $SERVICE_NAME -n 50

EOF
