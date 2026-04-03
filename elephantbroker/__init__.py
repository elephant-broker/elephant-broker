import os

# Disable Cognee's built-in usage telemetry before any Cognee imports.
# Must be set at package init time because Cognee reads this env var at import time.
# See local/DEPLOYMENT-FIXES.md §30 (and §17 for original telemetry disable).
os.environ.setdefault("COGNEE_DISABLE_TELEMETRY", "true")
