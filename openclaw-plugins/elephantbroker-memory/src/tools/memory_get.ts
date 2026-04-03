import type { ElephantBrokerClient } from "../client.js";

export function createMemoryGetTool(client: ElephantBrokerClient) {
  return {
    id: "memory_get",
    name: "memory_get",
    description: "Get a specific memory fact by its ID.",
    parameters: {
      type: "object",
      properties: {
        fact_id: { type: "string", description: "The fact ID to retrieve" },
      },
      required: ["fact_id"],
    },
    async execute(toolCallId: string, params: { fact_id: string }, signal?: AbortSignal) {
      const result = await client.getById(params.fact_id);
      return {
        content: [{ type: "text", text: JSON.stringify(result) }],
      };
    },
  };
}
