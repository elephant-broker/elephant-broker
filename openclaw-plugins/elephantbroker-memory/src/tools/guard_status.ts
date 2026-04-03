import type { ElephantBrokerClient } from "../client.js";

export function createGuardStatusTool(client: ElephantBrokerClient) {
  return {
    id: "guard_status",
    name: "guard_status",
    description:
      "Get detailed information about a specific guard event, including the matched " +
      "rules, outcome, and approval status if applicable. Use the guard_event_id from " +
      "guards_list results.",
    parameters: {
      type: "object" as const,
      properties: {
        guard_event_id: {
          type: "string",
          description: "The guard event ID to look up (from guards_list results)",
        },
      },
      required: ["guard_event_id"],
    },
    async execute(toolCallId: string, params: { guard_event_id: string }, signal?: AbortSignal) {
      const result = await client.getGuardEventDetail(params.guard_event_id);
      return {
        content: [{ type: "text", text: JSON.stringify(result) }],
      };
    },
  };
}
