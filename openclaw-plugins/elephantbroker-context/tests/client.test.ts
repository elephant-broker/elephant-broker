/**
 * ContextEngineClient unit tests (TODO-6-503).
 *
 * Narrow coverage for the `getConfig()` profile-forwarding wiring — the
 * broader client/engine flow is exercised in engine.test.ts via a mocked
 * client. These tests drill into the actual HTTP URL construction by
 * stubbing `globalThis.fetch` and asserting the URL argument.
 *
 * Background: pre-fix, `client.getConfig()` called `/context/config`
 * unconditionally, ignoring the P6 `?profile=X` query param the Python
 * endpoint honors. Effect: TS plugin's buffer-flush decision silently
 * used the global `ingest_batch_size` instead of the per-profile override.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";
import { ContextEngineClient } from "../src/client.js";

describe("ContextEngineClient.getConfig() — TODO-6-503 profile forwarding", () => {
  // A tiny Response-like object is enough — getConfig() only calls `.ok`
  // and `.json()`. We don't need a full fetch polyfill.
  function okResponse(body: Record<string, unknown>): Response {
    return {
      ok: true,
      status: 200,
      json: async () => body,
    } as unknown as Response;
  }

  let fetchSpy: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    fetchSpy = vi.fn().mockResolvedValue(
      okResponse({ ingest_batch_size: 6, ingest_batch_timeout_ms: 60000 }),
    );
    vi.stubGlobal("fetch", fetchSpy);
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.restoreAllMocks();
  });

  it("forwards profileName as ?profile= query param when set", async () => {
    const client = new ContextEngineClient(
      "http://localhost:8420",
      "gw-test",
      "gw",
      "coding",
    );
    await client.getConfig();
    expect(fetchSpy).toHaveBeenCalledTimes(1);
    const urlArg = fetchSpy.mock.calls[0][0] as string;
    expect(urlArg).toBe("http://localhost:8420/context/config?profile=coding");
  });

  it("omits query param when profileName is empty (backward-compat fallback)", async () => {
    // No profileName passed → client holds empty string → no ?profile=
    // segment → Python endpoint returns the global default per the
    // backward-compat contract from commit 72a5afc.
    const client = new ContextEngineClient(
      "http://localhost:8420",
      "gw-test",
      "gw",
      // no profileName
    );
    await client.getConfig();
    expect(fetchSpy).toHaveBeenCalledTimes(1);
    const urlArg = fetchSpy.mock.calls[0][0] as string;
    expect(urlArg).toBe("http://localhost:8420/context/config");
    expect(urlArg).not.toContain("profile=");
  });

  it("URL-encodes special chars in profileName", async () => {
    // Guards against profile names with `&` / `=` / spaces / `/` / Unicode
    // that would corrupt query-string parsing if appended raw.
    const client = new ContextEngineClient(
      "http://localhost:8420",
      "gw-test",
      "gw",
      "my profile/v2",
    );
    await client.getConfig();
    expect(fetchSpy).toHaveBeenCalledTimes(1);
    const urlArg = fetchSpy.mock.calls[0][0] as string;
    expect(urlArg).toBe(
      "http://localhost:8420/context/config?profile=my%20profile%2Fv2",
    );
  });
});
