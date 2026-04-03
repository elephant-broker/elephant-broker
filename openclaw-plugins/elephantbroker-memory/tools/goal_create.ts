import type { ElephantBrokerClient } from "../client.js";

/**
 * Session goal creation tool — always creates session-scoped goals in Redis.
 * Persistent goals are created exclusively via admin API.
 */
export function createGoalCreateTool(client: ElephantBrokerClient) {
  return {
    id: "goal_create",
    name: "goal_create",
    description:
      "Create a session goal (ephemeral, current session only). " +
      "Goals are stored in Redis for fast access during the session and flushed to " +
      "long-term memory on session end. Persistent goals are created via admin tools only. " +
      "Always call session_goals_list first to avoid duplicates.",
    parameters: {
      type: "object",
      properties: {
        title: { type: "string", description: "Clear, specific goal title" },
        description: { type: "string", description: "What needs to be done and why" },
        parent_goal_id: {
          type: "string",
          description: "UUID of parent goal (for sub-tasks)",
        },
        success_criteria: {
          type: "array",
          items: { type: "string" },
          description: "How to know this goal is done",
        },
      },
      required: ["title"],
    },
    async execute(toolCallId: string, params: {
      title: string;
      description?: string;
      parent_goal_id?: string;
      success_criteria?: string[];
    }, signal?: AbortSignal) {
      // Agent tools always create session-scoped goals (NEW-3)
      const result = await client.createSessionGoal({
        title: params.title,
        description: params.description,
        parent_goal_id: params.parent_goal_id,
        success_criteria: params.success_criteria || [],
      });
      if (result === null) {
        return {
          content: [{ type: "text", text: JSON.stringify({ error: "duplicate", message: "A goal with this title already exists in the session" }) }],
        };
      }
      return {
        content: [{ type: "text", text: JSON.stringify(result) }],
      };
    },
  };
}
