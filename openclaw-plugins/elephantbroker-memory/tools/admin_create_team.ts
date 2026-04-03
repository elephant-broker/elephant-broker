import type { ElephantBrokerClient } from "../client.js";

export function createAdminCreateTeamTool(client: ElephantBrokerClient) {
  return {
    id: "admin_create_team",
    name: "admin_create_team",
    description: "Create a new team within an organization. Requires authority_level >= 70 (org admin) and matching org.",
    parameters: {
      type: "object",
      properties: {
        name: { type: "string", description: "Team full name" },
        display_label: { type: "string", description: "Short label for logs/dashboard" },
        org_id: { type: "string", description: "Parent organization UUID" },
      },
      required: ["name", "org_id"],
    },
    async execute(toolCallId: string, params: { name: string; display_label?: string; org_id: string }, signal?: AbortSignal) {
      const result = await client.adminCreateTeam(params);
      return {
        content: [{ type: "text", text: JSON.stringify(result) }],
      };
    },
  };
}
