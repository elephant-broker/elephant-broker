import type { ElephantBrokerClient } from "../client.js";

export function createSessionGoalsListTool(client: ElephantBrokerClient) {
  return {
    id: "session_goals_list",
    name: "session_goals_list",
    description:
      "Returns the full tree of session goals with their IDs, status, blockers, " +
      "sub-goals, and confidence. Check this before creating goals to avoid duplicates.",
    parameters: {
      type: "object",
      properties: {},
    },
    async execute(toolCallId: string, params: Record<string, never>, signal?: AbortSignal) {
      const result = await client.listSessionGoals();
      return {
        content: [{ type: "text", text: JSON.stringify(result) }],
      };
    },
  };
}
