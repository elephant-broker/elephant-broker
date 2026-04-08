# ElephantBroker Cognitive Runtime — multi-stage Docker build using uv.
#
# IMPORTANT: per CLAUDE.md, the runtime is officially deployed as a native
# Python venv via deploy/install.sh, NOT in Docker. This Dockerfile is kept
# for development convenience and CI/sandbox use, NOT production deployments.
# Production deployments should use deploy/install.sh on a real host.
#
# Build:    docker build -t elephantbroker:dev .
# Run:      docker run --rm -e EB_GATEWAY_ID=gw-dev elephantbroker:dev

FROM python:3.11-slim AS builder

# Install uv (Astral's Python package manager) — single binary, ~30MB
COPY --from=ghcr.io/astral-sh/uv:0.11.3 /uv /uvx /usr/local/bin/

WORKDIR /app

# H1 (TODO-3-314, TODO-3-215): split COPY+sync into two phases so Docker's
# layer cache actually works the way the old comment promised. The previous
# form copied source BEFORE running a single `uv sync`, which meant any
# source-only edit busted the dep cache. Now:
#   1. copy lockfile  ->  uv sync deps only (cached layer)
#   2. copy source    ->  uv sync project on top (cheap re-run)
#
# H-R2 (TODO-3-344, TODO-3-620): the root pyproject.toml declares
# `[tool.uv.workspace] members = ["hitl-middleware"]` (see E1 workspace
# conversion, Bucket E commit 112cccd), but the previous Dockerfile only
# copied `elephantbroker/` into the build context. The second `uv sync
# --frozen --no-dev` therefore saw the workspace declaration with no
# source behind it — uv exited 0 without installing hitl-middleware and
# the image shipped HITL-less (no /app/.venv/bin/hitl-middleware binary).
# The native-install path (deploy/install.sh) worked only because
# `uv sync` ran at the repo root where both package directories exist.
#
# Fix: copy the workspace member pyproject BEFORE the first `uv sync`
# (so workspace resolution is consistent in the cached deps layer) AND
# copy the workspace member source BEFORE the second `uv sync` (so the
# install actually has code to install). Both copies sit alongside
# their elephantbroker/ counterparts at the matching layer position so
# the cache boundaries stay correct: lockfile + both pyprojects in the
# deps layer (cached unless lockfile changes), both source trees in
# the project layer (cached unless source changes).
COPY pyproject.toml uv.lock ./
COPY hitl-middleware/pyproject.toml hitl-middleware/pyproject.toml

# uv sync --frozen installs EXACTLY what uv.lock specifies — same versions
# as a native install via deploy/install.sh, no transitive drift between
# Docker and bare-metal deployments.
#
# --no-dev: skip pytest/ruff/etc — production image only
# --no-install-project: install deps but defer the project itself; we
#   sync it again after copying source so editable installs resolve right.
RUN uv sync --frozen --no-install-project --no-dev

COPY elephantbroker/ elephantbroker/
COPY hitl-middleware/ hitl-middleware/

# Second sync installs the project AND the hitl-middleware workspace
# member on top of the cached dep layer. Source-only edits invalidate
# this layer but not the (much larger) dep layer above it.
RUN uv sync --frozen --no-dev

# ---

FROM python:3.11-slim AS runtime

# Copy uv binary into the runtime image too — needed if you want to do
# in-container ad-hoc package operations. Comment out if you want a smaller
# image and don't care about ad-hoc uv usage.
COPY --from=ghcr.io/astral-sh/uv:0.11.3 /uv /uvx /usr/local/bin/

WORKDIR /app

# Copy the venv and source from the builder stage. The venv lives at
# /app/.venv (uv's default location). The elephantbroker entry point is
# at /app/.venv/bin/elephantbroker, and the hitl-middleware entry point
# is at /app/.venv/bin/hitl-middleware.
#
# H-R2 (TODO-3-344, TODO-3-620): /app/hitl-middleware must also be
# preserved in the runtime stage. `uv sync` installs workspace members
# as editable by default, so .venv/lib/python3.11/site-packages/
# contains a .pth file that points at /app/hitl-middleware/hitl_middleware/
# (and similarly at /app/elephantbroker/elephantbroker/). Without the
# source directory in the runtime image, the editable link is dead and
# `import hitl_middleware` fails at import time. This mirrors the
# pre-existing `COPY --from=builder /app/elephantbroker` line.
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/elephantbroker /app/elephantbroker
COPY --from=builder /app/hitl-middleware /app/hitl-middleware
COPY --from=builder /app/pyproject.toml /app/pyproject.toml

# Bake in default config so the container has something to read at startup.
# In production, mount a real config file at /etc/elephantbroker/default.yaml.
COPY elephantbroker/config/default.yaml /etc/elephantbroker/default.yaml

# H2 (TODO-3-321): create a non-root service user. The native install
# (deploy/install.sh) runs the runtime as a dedicated `elephantbroker`
# system user; the Docker image mirrors that posture instead of running
# as root.
#
# C3 / TODO-3-625 (Bucket C-R2): chown narrowing. The previous form
# `chown -R elephantbroker:elephantbroker /app` transferred ownership of
# the entire install tree (venv + source + binaries) to the runtime
# user, matching the exact anti-pattern C3 removed from install.sh
# (TODO-3-010). A compromised runtime process could then rewrite its
# own code, the cognee binaries, or pyproject.toml — the whole point
# of a dedicated unprivileged service user vanishes.
#
# The native-install narrowed model chowns ONLY the Cognee writable
# subdirs:
#   * <site-packages>/cognee/.cognee_system  — Cognee runtime SQLite + state
#   * <site-packages>/cognee/.data_storage   — Cognee chunk/artifact storage
#   * <site-packages>/.anon_id               — Cognee telemetry id file
# Everything else stays root-owned. Default file modes from `uv sync`
# are 644/755 (other-readable + other-executable for dirs), so the
# service user reads and traverses the venv without owning it.
#
# In the Docker image, site-packages lives at
# /app/.venv/lib/python3.11/site-packages/. We resolve it via the venv's
# own Python and chown just the three Cognee paths. /etc/elephantbroker/
# default.yaml stays root-owned at default 644 (the service user only
# reads it).
#
# NOTE: this Dockerfile is for dev/sandbox/CI use only — production
# deployments use deploy/install.sh on a real host (see header comment).
RUN useradd --system --no-create-home --shell /usr/sbin/nologin elephantbroker \
    && SITE_PACKAGES=$(/app/.venv/bin/python -c 'import site; print(site.getsitepackages()[0])') \
    && COGNEE_DIR="$SITE_PACKAGES/cognee" \
    && mkdir -p "$COGNEE_DIR/.cognee_system/databases" "$COGNEE_DIR/.data_storage" \
    && touch "$SITE_PACKAGES/.anon_id" \
    && chmod 644 "$SITE_PACKAGES/.anon_id" \
    && chown -R elephantbroker:elephantbroker "$COGNEE_DIR/.cognee_system" \
    && chown -R elephantbroker:elephantbroker "$COGNEE_DIR/.data_storage" \
    && chown elephantbroker:elephantbroker "$SITE_PACKAGES/.anon_id"
USER elephantbroker

# Required at runtime: EB_GATEWAY_ID
# Optional: EB_ORG_ID, EB_TEAM_ID, EB_NEO4J_URI, EB_QDRANT_URL, EB_REDIS_URL
# See elephantbroker/config/env.example for the full list.

# Use the venv's binary directly so PYTHONPATH and import resolution work
# the same as a bare-metal install. uv puts entry points in .venv/bin/.
ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8420

ENTRYPOINT ["elephantbroker", "serve", "--config", "/etc/elephantbroker/default.yaml"]
