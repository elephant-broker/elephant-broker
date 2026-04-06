# ElephantBroker Cognitive Runtime — multi-stage Docker build
FROM python:3.11-slim AS builder

WORKDIR /app
COPY uv.lock pyproject.toml .python-version ./
COPY elephantbroker/ elephantbroker/
# Use a bind mount for uv cache to speed up builds
RUN --mount=type=cache,target=/root/.cache/uv \
    curl -LsSf https://astral.sh/uv/install.sh | sh && \
    /root/.local/bin/uv sync --frozen --no-dev

# ---

FROM python:3.11-slim AS runtime
WORKDIR /app
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app/elephantbroker /app/elephantbroker

# Bake in default config
COPY elephantbroker/config/default.yaml /app/config/default.yaml

# Required: EB_GATEWAY_ID must be set at runtime
# Optional: EB_ORG_ID, EB_TEAM_ID, EB_ACTOR_ID, EB_NEO4J_URI, EB_QDRANT_URL, EB_REDIS_URL, EB_POSTGRES_DSN

# Add python venv bin to path
ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8420

HEALTHCHECK --interval=10s --timeout=5s --start-period=30s --retries=3 \
  CMD ["elephantbroker", "health-check", "--host", "localhost", "--port", "8420"]

ENTRYPOINT ["elephantbroker", "serve", "--config", "/app/config/default.yaml"]
