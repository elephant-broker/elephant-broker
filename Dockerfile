# ElephantBroker Cognitive Runtime — multi-stage Docker build
FROM python:3.11-slim AS builder

WORKDIR /app
COPY pyproject.toml .
COPY elephantbroker/ elephantbroker/
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir . && \
    pip install --no-cache-dir --force-reinstall --no-deps 'mistralai>=1.0'

# ---

FROM python:3.11-slim AS runtime

WORKDIR /app
COPY --from=builder /usr/local/lib/python3.11/site-packages /usr/local/lib/python3.11/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin
COPY --from=builder /app/elephantbroker /app/elephantbroker

# Bake in default config
COPY elephantbroker/config/default.yaml /app/config/default.yaml

# Required: EB_GATEWAY_ID must be set at runtime
# Optional: EB_ORG_ID, EB_TEAM_ID, EB_ACTOR_ID, EB_NEO4J_URI, EB_QDRANT_URL, EB_REDIS_URL

EXPOSE 8420

ENTRYPOINT ["elephantbroker", "serve", "--config", "/app/config/default.yaml"]
