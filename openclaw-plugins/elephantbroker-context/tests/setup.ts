// Vitest setup file — runs before each test file imports its modules.
//
// The plugin's ElephantBrokerClient constructor reads EB_GATEWAY_ID from
// either the constructor argument or process.env, and throws if neither
// is set. Tests that exercise the register() flow or instantiate the
// client directly need a default value to be present at module init time.
//
// We use `??=` (nullish coalescing assignment) so that any value the
// operator has actually set in their shell environment is preserved.
// Only undefined values get the test default.

process.env.EB_GATEWAY_ID ??= "test-gateway";
process.env.EB_GATEWAY_SHORT_NAME ??= "test";
process.env.EB_RUNTIME_URL ??= "http://localhost:8420";
