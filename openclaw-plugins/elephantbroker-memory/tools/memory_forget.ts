import type { ElephantBrokerClient } from "../client.js";

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
        } catch {
          return {
            content: [{ type: "text", text: JSON.stringify({ deleted: null, reason: "not found" }) }],
          };
        }
      }
      if (params.query) {
        const results = await client.search({ query: params.query, max_results: 1 });
        if (results.length > 0 && results[0].score > 0.7) {
          await client.forget(results[0].id);
          return {
            content: [{ type: "text", text: JSON.stringify({ deleted: results[0].id, text: results[0].text.slice(0, 80) }) }],
          };
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
