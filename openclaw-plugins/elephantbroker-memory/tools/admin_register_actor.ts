import type { ElephantBrokerClient } from "../client.js";

export function createAdminRegisterActorTool(client: ElephantBrokerClient) {
  return {
    id: "admin_register_actor",
    name: "admin_register_actor",
    description: "Register a new actor (human or agent). Requires authority_level >= 70 (org admin).",
    parameters: {
      type: "object",
      properties: {
        display_name: { type: "string", description: "Actor display name" },
        type: {
          type: "string",
          enum: ["human_coordinator", "human_operator", "worker_agent", "manager_agent", "reviewer_agent", "supervisor_agent"],
          description: "Actor type",
        },
        authority_level: { type: "number", description: "Authority level (0-100)" },
        org_id: { type: "string", description: "Organization UUID" },
        team_ids: { type: "array", items: { type: "string" }, description: "Team UUIDs" },
        handles: { type: "array", items: { type: "string" }, description: "Platform-qualified handles (e.g. email:admin@acme.com)" },
      },
      required: ["display_name", "type"],
    },
    async execute(toolCallId: string, params: {
      display_name: string; type: string; authority_level?: number;
      org_id?: string; team_ids?: string[]; handles?: string[];
    }, signal?: AbortSignal) {
      const result = await client.adminRegisterActor(params);
      return {
        content: [{ type: "text", text: JSON.stringify(result) }],
      };
    },
  };
}
