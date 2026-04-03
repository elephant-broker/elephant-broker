import type { ElephantBrokerClient } from "../client.js";

export function createGuardsListTool(client: ElephantBrokerClient) {
  return {
    id: "guards_list",
    name: "guards_list",
    description:
      "List active guard rules, pending approval requests, and recent guard events " +
      "for the current session. Use this to understand what safety constraints are " +
      "in effect and whether any actions are awaiting human approval.",
    parameters: {
      type: "object" as const,
      properties: {},
    },
    async execute(toolCallId: string, params: Record<string, never>, signal?: AbortSignal) {
      const result = await client.getActiveGuards();
      return {
        content: [{ type: "text", text: JSON.stringify(result) }],
      };
    },
  };
}
