import { describe, it, expect, vi, beforeEach } from "vitest";
import { ElephantBrokerClient, HttpStatusError } from "../src/client.js";

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

  // 5-603: client must throw HttpStatusError (not plain Error) so that
  // tools can discriminate backend signals by status code.

  it("forget throws HttpStatusError(404) on 404", async () => {
    mockFetch.mockResolvedValueOnce({ ok: false, status: 404 });
    await expect(client.forget("missing")).rejects.toBeInstanceOf(HttpStatusError);
    mockFetch.mockResolvedValueOnce({ ok: false, status: 404 });
    await expect(client.forget("missing")).rejects.toMatchObject({ status: 404 });
  });

  it("forget throws HttpStatusError(403) on 403", async () => {
    mockFetch.mockResolvedValueOnce({ ok: false, status: 403 });
    await expect(client.forget("cross-tenant")).rejects.toMatchObject({ status: 403 });
  });

  it("forget throws HttpStatusError(500) on 500", async () => {
    mockFetch.mockResolvedValueOnce({ ok: false, status: 500 });
    await expect(client.forget("boom")).rejects.toMatchObject({ status: 500 });
  });

  it("update throws HttpStatusError(404) on 404", async () => {
    mockFetch.mockResolvedValueOnce({ ok: false, status: 404 });
    await expect(client.update("missing", { text: "x" })).rejects.toMatchObject({ status: 404 });
  });

  it("update throws HttpStatusError(403) on 403", async () => {
    mockFetch.mockResolvedValueOnce({ ok: false, status: 403 });
    await expect(client.update("cross-tenant", { text: "x" })).rejects.toMatchObject({ status: 403 });
  });

  it("update throws HttpStatusError(422) on 422", async () => {
    mockFetch.mockResolvedValueOnce({ ok: false, status: 422 });
    await expect(client.update("abc", { category: 42 })).rejects.toMatchObject({ status: 422 });
  });

  it("update throws HttpStatusError(500) on 500", async () => {
    mockFetch.mockResolvedValueOnce({ ok: false, status: 500 });
    await expect(client.update("abc", { text: "x" })).rejects.toMatchObject({ status: 500 });
  });

  // H3: procedures.create must send is_manual_only to pass the R2-P2.1 validator
  it("createProcedure sends is_manual_only: true by default", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({ id: "proc-1", name: "test" }),
    });
    await client.createProcedure({
      name: "deploy",
      steps: [{ order: 1, instruction: "run deploy" }],
    });
    const body = JSON.parse(mockFetch.mock.calls[0][1].body);
    expect(body.is_manual_only).toBe(true);
    expect(body.activation_modes).toBeUndefined();
  });

  it("createProcedure forwards explicit activation_modes", async () => {
    mockFetch.mockResolvedValueOnce({
      ok: true,
      json: async () => ({ id: "proc-2", name: "test" }),
    });
    await client.createProcedure({
      name: "deploy",
      steps: [{ order: 1, instruction: "run deploy" }],
      is_manual_only: false,
      activation_modes: ["on_goal_active"],
    });
    const body = JSON.parse(mockFetch.mock.calls[0][1].body);
    expect(body.is_manual_only).toBe(false);
    expect(body.activation_modes).toEqual(["on_goal_active"]);
  });
});
