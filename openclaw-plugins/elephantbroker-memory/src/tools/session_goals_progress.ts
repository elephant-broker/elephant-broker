import type { ElephantBrokerClient } from "../client.js";

export function createSessionGoalsProgressTool(client: ElephantBrokerClient) {
  return {
    id: "session_goals_progress",
    name: "session_goals_progress",
    description:
      "Record meaningful progress on a session goal. Increases the goal's confidence. " +
      "Get goal_id from session_goals_list.",
    parameters: {
      type: "object",
      properties: {
        goal_id: { type: "string", description: "UUID from session_goals_list" },
        evidence: { type: "string", description: "What progress was made" },
      },
      required: ["goal_id", "evidence"],
    },
    async execute(toolCallId: string, params: { goal_id: string; evidence: string }, signal?: AbortSignal) {
      const result = await client.recordSessionGoalProgress(params.goal_id, {
        evidence: params.evidence,
      });
      return {
        content: [{ type: "text", text: JSON.stringify(result) }],
      };
    },
  };
}
