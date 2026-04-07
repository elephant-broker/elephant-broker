"""Top-level pytest fixtures shared by unit + integration tests."""
import os


# ---------------------------------------------------------------------------
# Opt-in to dev/test behavior for the safety guards added in the deploy
# config-templates work (Bucket A: A3/A4/A5 in PR #3). The runtime refuses
# to boot with sentinel gateway_id, empty neo4j_password, or attempted
# dataset rename — but the entire test suite uses default configs, so we
# unconditionally set the opt-out env vars at session start.
#
# Production hosts MUST NOT set these. Tests that want to verify the guards
# fire (test_container_startup_safety.py, test_container.py::TestStartup*)
# clear them locally inside the test body.
# ---------------------------------------------------------------------------
os.environ.setdefault("EB_ALLOW_DEFAULT_GATEWAY_ID", "true")
os.environ.setdefault("EB_DEV_MODE", "true")
os.environ.setdefault("EB_ALLOW_DATASET_CHANGE", "true")
