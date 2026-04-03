import type { ElephantBrokerClient } from "../client.js";

export function createSessionGoalsAddBlockerTool(client: ElephantBrokerClient) {
  return {
    id: "session_goals_add_blocker",
    name: "session_goals_add_blocker",
    description:
      "Report a blocker on a session goal. Blocked goals get elevated priority — " +
      "they are always injected into your context. Get goal_id from session_goals_list.",
    parameters: {
      type: "object",
      properties: {
        goal_id: { type: "string", description: "UUID from session_goals_list" },
        blocker: { type: "string", description: "What is blocking progress" },
      },
      required: ["goal_id", "blocker"],
    },
    async execute(toolCallId: string, params: { goal_id: string; blocker: string }, signal?: AbortSignal) {
      const result = await client.addSessionGoalBlocker(params.goal_id, {
        blocker: params.blocker,
      });
      return {
        content: [{ type: "text", text: JSON.stringify(result) }],
      };
    },
  };
}
