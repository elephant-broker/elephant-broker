import { describe, it, expect, vi, beforeEach } from "vitest";
import { ElephantBrokerClient } from "../src/client.js";

// Mock fetch globally
const mockFetch = vi.fn();
vi.stubGlobal("fetch", mockFetch);

describe("ElephantBrokerClient", () => {
  let client: ElephantBrokerClient;

  beforeEach(() => {
    client = new ElephantBrokerClient("http://test:8420");
    mockFetch.mockReset();
  });

  it("search sends POST to /memory/search", async () => {
    mockFetch.mockResolvedValueOnce({ ok: true, json: async () => [] });
    await client.search({ query: "test" });
    expect(mockFetch).toHaveBeenCalledWith(
      "http://test:8420/memory/search",
      expect.objectContaining({ method: "POST" })
    );
  });

  it("store sends POST to /memory/store", async () => {
    mockFetch.mockResolvedValueOnce({ ok: true, json: async () => ({ id: "abc" }) });
    await client.store({ fact: { text: "test" } });
    expect(mockFetch).toHaveBeenCalledWith(
      "http://test:8420/memory/store",
      expect.objectContaining({ method: "POST" })
    );
  });

  it("getById sends GET", async () => {
    mockFetch.mockResolvedValueOnce({ ok: true, status: 200, json: async () => ({ id: "abc" }) });
    const result = await client.getById("abc");
    expect(result).toEqual({ id: "abc" });
  });

  it("getById returns null on 404", async () => {
    mockFetch.mockResolvedValueOnce({ ok: false, status: 404 });
    const result = await client.getById("missing");
    expect(result).toBeNull();
  });

  it("forget sends DELETE", async () => {
    mockFetch.mockResolvedValueOnce({ ok: true, status: 204 });
    await client.forget("abc");
    expect(mockFetch).toHaveBeenCalledWith(
      "http://test:8420/memory/abc",
      expect.objectContaining({ method: "DELETE" })
    );
  });

  it("update sends PATCH", async () => {
    mockFetch.mockResolvedValueOnce({ ok: true, status: 200, json: async () => ({ id: "abc" }) });
    await client.update("abc", { text: "new" });
    expect(mockFetch).toHaveBeenCalledWith(
      "http://test:8420/memory/abc",
      expect.objectContaining({ method: "PATCH" })
    );
  });

  it("ingestMessages sends POST", async () => {
    mockFetch.mockResolvedValueOnce({ ok: true });
    await client.ingestMessages({
      session_key: "sk", session_id: "sid", messages: [{ role: "user", content: "hi" }]
    });
    expect(mockFetch).toHaveBeenCalledWith(
      "http://test:8420/memory/ingest-messages",
      expect.objectContaining({ method: "POST" })
    );
  });

  it("sessionStart sends POST", async () => {
    mockFetch.mockResolvedValueOnce({ ok: true });
    await client.sessionStart({ session_key: "sk", session_id: "sid" });
    expect(mockFetch).toHaveBeenCalledWith(
      "http://test:8420/sessions/start",
      expect.objectContaining({ method: "POST" })
    );
  });

  it("sessionEnd sends POST", async () => {
    mockFetch.mockResolvedValueOnce({ ok: true, json: async () => ({}) });
    await client.sessionEnd({ session_key: "sk", session_id: "sid" });
    expect(mockFetch).toHaveBeenCalledWith(
      "http://test:8420/sessions/end",
      expect.objectContaining({ method: "POST" })
    );
  });

  it("caches session keys", () => {
    client.cacheSessionKey("sk", "sid");
    expect(client.getCachedSessionId("sk")).toBe("sid");
  });

  it("search throws on non-ok response", async () => {
    mockFetch.mockResolvedValueOnce({ ok: false, status: 500 });
    await expect(client.search({ query: "test" })).rejects.toThrow();
  });

  it("forget throws on 404", async () => {
    mockFetch.mockResolvedValueOnce({ ok: false, status: 404 });
    await expect(client.forget("missing")).rejects.toThrow();
  });
});
