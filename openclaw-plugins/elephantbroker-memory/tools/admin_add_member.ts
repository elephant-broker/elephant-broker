import type { ElephantBrokerClient } from "../client.js";

export function createAdminAddMemberTool(client: ElephantBrokerClient) {
  return {
    id: "admin_add_member",
    name: "admin_add_member",
    description: "Add an actor to a team. Requires authority_level >= 50 (team lead) and matching team.",
    parameters: {
      type: "object",
      properties: {
        team_id: { type: "string", description: "Team UUID" },
        actor_id: { type: "string", description: "Actor UUID to add" },
      },
      required: ["team_id", "actor_id"],
    },
    async execute(toolCallId: string, params: { team_id: string; actor_id: string }, signal?: AbortSignal) {
      const result = await client.adminAddMember(params.team_id, params.actor_id);
      return {
        content: [{ type: "text", text: JSON.stringify(result) }],
      };
    },
  };
}
