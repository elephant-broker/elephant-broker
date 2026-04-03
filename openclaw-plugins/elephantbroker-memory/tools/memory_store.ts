import type { ElephantBrokerClient } from "../client.js";

export function createMemoryStoreTool(client: ElephantBrokerClient) {
  return {
    id: "memory_store",
    name: "memory_store",
    description: "Store a new fact in long-term memory.",
    parameters: {
      type: "object",
      properties: {
        text: { type: "string", description: "The fact text to store" },
        category: { type: "string", description: "Category (preference, decision, event, etc.)" },
        scope: { type: "string", description: "Visibility scope" },
        confidence: { type: "number", description: "Confidence 0.0-1.0" },
      },
      required: ["text"],
    },
    async execute(toolCallId: string, params: { text: string; category?: string; scope?: string; confidence?: number }, signal?: AbortSignal) {
      const sid = client.getSessionId();
      const result = await client.store({
        fact: {
          text: params.text,
          category: params.category || "general",
          scope: params.scope,
          confidence: params.confidence,
        },
        session_key: client.getSessionKey(),
        ...(sid ? { session_id: sid } : {}),
      });
      if (result === null) {
        return {
          content: [{ type: "text", text: "Fact not stored: near-duplicate already exists in memory." }],
        };
      }
      return {
        content: [{ type: "text", text: JSON.stringify(result) }],
      };
    },
  };
}
