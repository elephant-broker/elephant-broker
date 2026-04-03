import type { ElephantBrokerClient } from "../client.js";

export function createAdminCreateOrgTool(client: ElephantBrokerClient) {
  return {
    id: "admin_create_org",
    name: "admin_create_org",
    description: "Create a new organization. Requires authority_level >= 90 (system admin).",
    parameters: {
      type: "object",
      properties: {
        name: { type: "string", description: "Organization full name" },
        display_label: { type: "string", description: "Short label for logs/dashboard" },
      },
      required: ["name"],
    },
    async execute(toolCallId: string, params: { name: string; display_label?: string }, signal?: AbortSignal) {
      const result = await client.adminCreateOrg(params);
      return {
        content: [{ type: "text", text: JSON.stringify(result) }],
      };
    },
  };
}
