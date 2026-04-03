import type { ElephantBrokerClient } from "../client.js";

export function createArtifactCreateTool(client: ElephantBrokerClient) {
  return {
    id: "create_artifact",
    name: "create_artifact",
    description:
      "Save content as a session-scoped (temporary, auto-expires) or persistent " +
      "(permanent, stored in knowledge graph) artifact. Session artifacts are " +
      "automatically cleaned up. Persistent artifacts survive across sessions.",
    parameters: {
      type: "object" as const,
      properties: {
        content: {
          type: "string",
          description: "The artifact content to save",
        },
        tool_name: {
          type: "string",
          description: "Source tool or 'manual' (default: 'manual')",
        },
        scope: {
          type: "string",
          enum: ["session", "persistent"],
          description: "Scope: 'session' (temporary) or 'persistent' (permanent in knowledge graph)",
        },
        tags: {
          type: "array",
          items: { type: "string" },
          description: "Optional tags for categorization and search",
        },
        goal_id: {
          type: "string",
          description: "Optional link to a session goal (persistent scope only)",
        },
        summary: {
          type: "string",
          description: "Optional human summary (auto-generated from content if not provided)",
        },
      },
      required: ["content"],
    },
    async execute(toolCallId: string, params: {
      content: string;
      tool_name?: string;
      scope?: "session" | "persistent";
      tags?: string[];
      goal_id?: string;
      summary?: string;
    }, signal?: AbortSignal) {
      const content = params.content?.trim() || "";
      if (!content) {
        return {
          content: [{ type: "text", text: JSON.stringify({ error: "content is required", artifact_id: null }) }],
        };
      }

      try {
        const result = await client.createArtifact({
          content,
          tool_name: params.tool_name || "manual",
          scope: params.scope || "session",
          tags: params.tags || [],
          goal_id: params.goal_id,
          summary: params.summary,
        });

        const artifactId = "artifact_id" in result ? result.artifact_id : undefined;
        const scopeUsed = params.scope || "session";

        return {
          content: [{ type: "text", text: JSON.stringify({
            artifact_id: artifactId || "unknown",
            scope: scopeUsed,
            status: "created",
            message: scopeUsed === "persistent"
              ? "Artifact saved permanently to knowledge graph"
              : "Artifact saved for this session (auto-expires)",
          }) }],
        };
      } catch (err) {
        console.error(`[EB] Create artifact failed: ${err}`);
        return {
          content: [{ type: "text", text: JSON.stringify({
            error: `Failed to create artifact: ${err}`,
            artifact_id: null,
          }) }],
        };
      }
    },
  };
}
