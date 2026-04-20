import { HttpStatusError, type ElephantBrokerClient } from "../client.js";

function forgetErrorResult(err: unknown) {
  // Discriminate on HTTP status so the agent-tool-error contract can
  // distinguish security signals (403) and backend failures (5xx) from
  // plain not-found. Bare `catch {}` previously masked all of these as
  // "not found" — a 500 looked identical to a missing fact.
  if (err instanceof HttpStatusError) {
    if (err.status === 404) {
      return { deleted: null, reason: "not_found" };
    }
    if (err.status === 403) {
      return { deleted: null, reason: "forbidden", detail: err.message };
    }
    if (err.status >= 500) {
      return { deleted: null, reason: "backend_error", status: err.status, detail: err.message };
    }
    return { deleted: null, reason: "error", status: err.status, detail: err.message };
  }
  return { deleted: null, reason: "error", detail: err instanceof Error ? err.message : String(err) };
}

export function createMemoryForgetTool(client: ElephantBrokerClient) {
  return {
    id: "memory_forget",
    name: "memory_forget",
    description: "Delete a memory. Provide fact_id for direct delete, or query to search-then-delete.",
    parameters: {
      type: "object",
      properties: {
        fact_id: { type: "string", description: "Direct fact ID to delete" },
        query: { type: "string", description: "Search query to find fact to delete" },
      },
    },
    async execute(toolCallId: string, params: { fact_id?: string; query?: string }, signal?: AbortSignal) {
      if (params.fact_id) {
        try {
          await client.forget(params.fact_id);
          return {
            content: [{ type: "text", text: JSON.stringify({ deleted: params.fact_id }) }],
          };
        } catch (err) {
          return {
            content: [{ type: "text", text: JSON.stringify(forgetErrorResult(err)) }],
          };
        }
      }
      if (params.query) {
        const results = await client.search({ query: params.query, max_results: 1 });
        if (results.length > 0 && results[0].score > 0.7) {
          try {
            await client.forget(results[0].id);
            return {
              content: [{ type: "text", text: JSON.stringify({ deleted: results[0].id, text: results[0].text.slice(0, 80) }) }],
            };
          } catch (err) {
            return {
              content: [{ type: "text", text: JSON.stringify(forgetErrorResult(err)) }],
            };
          }
        }
        return {
          content: [{ type: "text", text: JSON.stringify({ deleted: null, reason: "no match above threshold" }) }],
        };
      }
      return {
        content: [{ type: "text", text: JSON.stringify({ deleted: null, reason: "provide fact_id or query" }) }],
      };
    },
  };
}
