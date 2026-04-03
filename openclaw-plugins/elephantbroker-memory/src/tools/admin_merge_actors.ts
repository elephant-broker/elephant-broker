import type { ElephantBrokerClient } from "../client.js";

export function createAdminMergeActorsTool(client: ElephantBrokerClient) {
  return {
    id: "admin_merge_actors",
    name: "admin_merge_actors",
    description:
      "Merge a duplicate actor into a canonical one. Transfers all handles, team memberships, " +
      "and graph edges (CREATED_BY, OWNS_GOAL, MEMBER_OF) from the duplicate to the canonical actor. " +
      "Requires authority_level >= 70 (org admin).",
    parameters: {
      type: "object",
      properties: {
        canonical_id: { type: "string", description: "UUID of the canonical (surviving) actor" },
        duplicate_id: { type: "string", description: "UUID of the duplicate actor to merge and delete" },
      },
      required: ["canonical_id", "duplicate_id"],
    },
    async execute(toolCallId: string, params: { canonical_id: string; duplicate_id: string }, signal?: AbortSignal) {
      const result = await client.adminMergeActors(params.canonical_id, params.duplicate_id);
      return {
        content: [{ type: "text", text: JSON.stringify(result) }],
      };
    },
  };
}
