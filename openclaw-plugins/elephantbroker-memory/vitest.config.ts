import { defineConfig } from "vitest/config";

// Vitest configuration for the ElephantBroker Memory Plugin.
//
// `setupFiles` runs once before each test file, BEFORE the test file imports
// any modules. This is critical because the plugin source (src/client.ts)
// reads EB_GATEWAY_ID at construction time and throws if it is unset — so
// the env var must exist before any test file imports the client.
//
// See tests/setup.ts for the env vars that get stubbed.
export default defineConfig({
  test: {
    environment: "node",
    setupFiles: ["./tests/setup.ts"],
  },
});
