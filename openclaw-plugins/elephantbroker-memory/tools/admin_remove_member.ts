import type { ElephantBrokerClient } from "../client.js";

export function createAdminRemoveMemberTool(client: ElephantBrokerClient) {
  return {
    id: "admin_remove_member",
    name: "admin_remove_member",
    description: "Remove an actor from a team. Requires authority_level >= 50 (team lead) and matching team.",
    parameters: {
      type: "object",
      properties: {
        team_id: { type: "string", description: "Team UUID" },
        actor_id: { type: "string", description: "Actor UUID to remove" },
      },
      required: ["team_id", "actor_id"],
    },
    async execute(toolCallId: string, params: { team_id: string; actor_id: string }, signal?: AbortSignal) {
      const result = await client.adminRemoveMember(params.team_id, params.actor_id);
      return {
        content: [{ type: "text", text: JSON.stringify(result) }],
      };
    },
  };
}
