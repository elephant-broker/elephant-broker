import { HttpStatusError, type ElephantBrokerClient } from "../client.js";

function updateErrorResult(err: unknown) {
  // Discriminate on HTTP status so the agent-tool-error contract can
  // distinguish security signals (403), invalid-input (422), and backend
  // failures (5xx) from plain not-found. Bare `catch {}` previously
  // masked all of these as "not found".
  if (err instanceof HttpStatusError) {
    if (err.status === 404) {
      return { updated: null, reason: "not_found" };
    }
    if (err.status === 403) {
      return { updated: null, reason: "forbidden", detail: err.message };
    }
    if (err.status === 422) {
      return { updated: null, reason: "invalid_input", detail: err.message };
    }
    if (err.status >= 500) {
      return { updated: null, reason: "backend_error", status: err.status, detail: err.message };
    }
    return { updated: null, reason: "error", status: err.status, detail: err.message };
  }
  return { updated: null, reason: "error", detail: err instanceof Error ? err.message : String(err) };
}

export function createMemoryUpdateTool(client: ElephantBrokerClient) {
  return {
    id: "memory_update",
    name: "memory_update",
    description: "Update an existing memory. Provide fact_id for direct update, or query to search-then-update.",
    parameters: {
      type: "object",
      properties: {
        fact_id: { type: "string", description: "Direct fact ID to update" },
        query: { type: "string", description: "Search query to find fact to update" },
        new_text: { type: "string", description: "New text content" },
        updates: { type: "object", description: "Other field updates (confidence, category, etc.)" },
      },
    },
    async execute(toolCallId: string, params: { fact_id?: string; query?: string; new_text?: string; updates?: Record<string, unknown> }, signal?: AbortSignal) {
      const updateBody: Record<string, unknown> = { ...params.updates };
      if (params.new_text) updateBody.text = params.new_text;

      let targetId = params.fact_id;
      if (!targetId && params.query) {
        const results = await client.search({ query: params.query, max_results: 1 });
        if (results.length > 0 && results[0].score > 0.7) {
          targetId = results[0].id;
        } else {
          return {
            content: [{ type: "text", text: JSON.stringify({ updated: null, reason: "no match above threshold" }) }],
          };
        }
      }
      if (!targetId) return {
        content: [{ type: "text", text: JSON.stringify({ updated: null, reason: "provide fact_id or query" }) }],
      };

      try {
        const result = await client.update(targetId, updateBody);
        return {
          content: [{ type: "text", text: JSON.stringify({ updated: targetId, fact: result }) }],
        };
      } catch (err) {
        return {
          content: [{ type: "text", text: JSON.stringify(updateErrorResult(err)) }],
        };
      }
    },
  };
}
