import type { ElephantBrokerClient } from "../client.js";

export function createSessionGoalsUpdateStatusTool(client: ElephantBrokerClient) {
  return {
    id: "session_goals_update_status",
    name: "session_goals_update_status",
    description:
      "Update a session goal's status. Get the goal_id from session_goals_list first. " +
      "Use 'completed' with evidence when done, 'paused' to switch context, 'abandoned' when dropped.",
    parameters: {
      type: "object",
      properties: {
        goal_id: { type: "string", description: "UUID from session_goals_list" },
        status: {
          type: "string",
          enum: ["completed", "paused", "abandoned"],
          description: "New status for the goal",
        },
        evidence: { type: "string", description: "What was accomplished or why status changed" },
      },
      required: ["goal_id", "status"],
    },
    async execute(toolCallId: string, params: { goal_id: string; status: string; evidence?: string }, signal?: AbortSignal) {
      const result = await client.updateSessionGoalStatus(params.goal_id, {
        status: params.status,
        evidence: params.evidence,
      });
      return {
        content: [{ type: "text", text: JSON.stringify(result) }],
      };
    },
  };
}
