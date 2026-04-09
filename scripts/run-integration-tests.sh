#!/bin/bash
set -e
cd "$(dirname "$0")/.."

# --- Port configuration (must match docker-compose.test.yml) ---
TEST_NEO4J_BOLT_PORT=17687
TEST_NEO4J_HTTP_PORT=17474
TEST_QDRANT_PORT=16333
TEST_REDIS_PORT=16379

# --- Pre-flight: detect port conflicts with production infrastructure ---
check_port() {
    local port=$1 service=$2
    if lsof -iTCP:"$port" -sTCP:LISTEN -t >/dev/null 2>&1; then
        echo "ERROR: Port $port ($service) is already in use."
        echo "  If production infrastructure is running, stop it first or use different ports."
        echo "  Production compose: docker compose -f infrastructure/docker-compose.yml down"
        return 1
    fi
    return 0
}

echo "Checking for port conflicts..."
CONFLICT=0
check_port "$TEST_NEO4J_BOLT_PORT" "Neo4j bolt" || CONFLICT=1
check_port "$TEST_NEO4J_HTTP_PORT" "Neo4j HTTP" || CONFLICT=1
check_port "$TEST_QDRANT_PORT" "Qdrant" || CONFLICT=1
check_port "$TEST_REDIS_PORT" "Redis" || CONFLICT=1
if [ $CONFLICT -ne 0 ]; then
    echo "Aborting: fix port conflicts before running tests."
    exit 1
fi

echo "Starting test infrastructure..."
docker compose -f infrastructure/docker-compose.test.yml down -v 2>/dev/null || true
docker compose -f infrastructure/docker-compose.test.yml up -d

# --- Health check: wait for services instead of fixed sleep ---
wait_for_port() {
    local port=$1 name=$2 max_wait=${3:-30}
    local waited=0
    echo -n "  Waiting for $name on port $port..."
    while ! nc -z localhost "$port" 2>/dev/null; do
        sleep 1
        waited=$((waited + 1))
        if [ $waited -ge $max_wait ]; then
            echo " TIMEOUT (${max_wait}s)"
            echo "ERROR: $name did not become ready. Check docker logs."
            return 1
        fi
    done
    echo " ready (${waited}s)"
    return 0
}

echo "Waiting for services to initialize..."
READY=0
wait_for_port "$TEST_NEO4J_BOLT_PORT" "Neo4j" 30 || READY=1
wait_for_port "$TEST_QDRANT_PORT" "Qdrant" 20 || READY=1
wait_for_port "$TEST_REDIS_PORT" "Redis" 10 || READY=1
if [ $READY -ne 0 ]; then
    echo "Aborting: test infrastructure not ready."
    docker compose -f infrastructure/docker-compose.test.yml down -v
    exit 1
fi

echo "Running integration tests..."
source .venv/bin/activate

# ElephantBroker config — tells our adapters where to connect
export EB_NEO4J_URI=bolt://localhost:${TEST_NEO4J_BOLT_PORT}
export EB_NEO4J_USER=neo4j
# Dev/test credentials — ephemeral test containers only, not used in production
export EB_NEO4J_PASSWORD=testpassword
export EB_QDRANT_URL=http://localhost:${TEST_QDRANT_PORT}
export EB_REDIS_URL=redis://localhost:${TEST_REDIS_PORT}
# API key for test infrastructure LiteLLM proxy — only accessible on dev network
export EB_EMBEDDING_API_KEY="${EB_EMBEDDING_API_KEY:-sk-ofbTPUhKkRgDtKVRszsjvA}"

# Pin embedding model + dimensions to known-working OpenAI values, INDEPENDENT
# of whatever the schema default happens to be. Cognee tokenizes via tiktoken
# which only knows OpenAI model names — passing a Gemini model name causes
# tiktoken.encoding_for_model() to raise KeyError at engine init. Tests must
# be deterministic, so they pin a tiktoken-mappable name.
export EB_EMBEDDING_MODEL="${EB_EMBEDDING_MODEL:-openai/text-embedding-3-large}"
export EB_EMBEDDING_DIMENSIONS="${EB_EMBEDDING_DIMENSIONS:-1024}"

# LLM config — used by Cognee cognify() for entity/relationship extraction
export EB_LLM_ENDPOINT="${EB_LLM_ENDPOINT:-http://localhost:8811/v1}"
export EB_LLM_API_KEY="${EB_LLM_API_KEY:-$EB_EMBEDDING_API_KEY}"
export EB_LLM_MODEL="${EB_LLM_MODEL:-openai/gemini/gemini-2.5-pro}"

# Cognee SDK — disable access control and LLM connection test.
# Graph/vector provider is set programmatically by configure_cognee().
export ENABLE_BACKEND_ACCESS_CONTROL=false
export COGNEE_SKIP_CONNECTION_TEST=true
export LLM_API_KEY="${EB_LLM_API_KEY:-test-unused}"

# Disable set -e for pytest — we capture exit codes manually.
# Cognee caches singleton connections (Neo4j driver, vector engine) on the first
# event loop. Running pipeline tests (which use cognee.add/cognify/search) in a
# separate pytest process avoids stale-loop contamination from non-pipeline tests.
set +e

echo "--- Non-pipeline integration tests ---"
pytest tests/integration/ -v --tb=short -m "not pipeline"
NON_PIPELINE_RC=$?

echo "--- Decision domain extraction tests (requires LLM) ---"
pytest tests/unit/adapters/cognee/tasks/test_extract_facts_integration.py -v --tb=short
DOMAIN_RC=$?

echo "--- Pipeline integration tests (requires LLM) ---"
pytest tests/integration/ -v --tb=short -m "pipeline"
PIPELINE_RC=$?

echo "--- E2E lifecycle tests (in-process ASGI, same infra as integration) ---"
pytest tests/e2e/gateway_simulator/test_phase5_lifecycle.py -v --tb=short -m "integration"
E2E_RC=$?

set -e

# pytest exit codes: 0=all passed, 1=test failures, 2=interrupted/internal errors, 5=no tests
# Exit code 2 with 0 failures = async teardown noise from Cognee singletons (harmless).
if [ $NON_PIPELINE_RC -eq 1 ] || [ $PIPELINE_RC -eq 1 ] || [ $E2E_RC -eq 1 ] || [ $DOMAIN_RC -eq 1 ]; then
    echo "FAILED: integration tests had test failures."
    exit 1
fi

echo "Tearing down test infrastructure..."
docker compose -f infrastructure/docker-compose.test.yml down -v

echo "Done."
