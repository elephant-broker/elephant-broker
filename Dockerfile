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

# Copy pyproject.toml + uv.lock first so Docker can cache the dep layer
# separately from source code changes.
COPY pyproject.toml uv.lock ./
COPY elephantbroker/ elephantbroker/

# uv sync --frozen installs EXACTLY what uv.lock specifies — same versions
# as a native install via deploy/install.sh, no transitive drift between
# Docker and bare-metal deployments.
#
# --no-dev: skip pytest/ruff/etc — production image only
# --no-install-project: install deps but defer the project itself (we sync
#   it after copying source so editable installs work right)
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
# at /app/.venv/bin/elephantbroker.
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/elephantbroker /app/elephantbroker
COPY --from=builder /app/pyproject.toml /app/pyproject.toml

# Bake in default config so the container has something to read at startup.
# In production, mount a real config file at /etc/elephantbroker/default.yaml.
COPY elephantbroker/config/default.yaml /etc/elephantbroker/default.yaml

# Required at runtime: EB_GATEWAY_ID
# Optional: EB_ORG_ID, EB_TEAM_ID, EB_NEO4J_URI, EB_QDRANT_URL, EB_REDIS_URL
# See elephantbroker/config/env.example for the full list.

# Use the venv's binary directly so PYTHONPATH and import resolution work
# the same as a bare-metal install. uv puts entry points in .venv/bin/.
ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8420

ENTRYPOINT ["elephantbroker", "serve", "--config", "/etc/elephantbroker/default.yaml"]
