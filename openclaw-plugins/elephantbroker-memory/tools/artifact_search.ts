import type { ElephantBrokerClient } from "../client.js";

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export function createArtifactSearchTool(client: ElephantBrokerClient) {
  return {
    id: "artifact_search",
    name: "artifact_search",
    description:
      "Search or retrieve tool output artifacts by query or exact ID. " +
      "When you see '[Tool output: X — summary → Call artifact_search(\"id\") for full output]' " +
      "in your context, use this tool with the provided ID to get the full content.",
    parameters: {
      type: "object" as const,
      properties: {
        query: {
          type: "string",
          description: "Search query text or exact artifact UUID for direct retrieval",
        },
        tool_name: {
          type: "string",
          description: "Optional filter by source tool name (e.g., 'bash', 'python')",
        },
        scope: {
          type: "string",
          enum: ["session", "persistent", "all"],
          description: "Search scope: 'session' (current session only), 'persistent' (knowledge graph), 'all' (both, default)",
        },
        max_results: {
          type: "number",
          description: "Maximum results to return (default 5, max 50)",
        },
      },
      required: ["query"],
    },
    async execute(toolCallId: string, params: {
      query: string;
      tool_name?: string;
      scope?: "session" | "persistent" | "all";
      max_results?: number;
    }, signal?: AbortSignal) {
      const query = params.query?.trim() || "";
      const scope = params.scope || "all";
      const maxResults = Math.min(params.max_results || 5, 50);

      // UUID detection — direct lookup for artifact retrieval
      if (UUID_RE.test(query)) {
        if (scope === "session" || scope === "all") {
          try {
            const result = await client.getSessionArtifact(query);
            if (result) {
              return {
                content: [{ type: "text", text: JSON.stringify({
                  results: [{
                    artifact_id: result.artifact_id,
                    tool_name: result.tool_name,
                    summary: result.summary,
                    content: result.content,
                    created_at: result.created_at,
                    token_estimate: result.token_estimate,
                    scope: "session",
                  }],
                  total: 1,
                  lookup_type: "direct_session",
                }) }],
              };
            }
          } catch (err) {
            console.warn(`[EB] Session artifact lookup failed: ${err}`);
          }
        }
        // UUID not found in session — fall through to search
      }

      // Search mode — query across session and/or persistent buckets
      const allResults: Array<{
        artifact_id: string;
        tool_name: string;
        summary: string;
        created_at: string;
        token_estimate: number;
        scope: string;
      }> = [];

      if (scope === "session" || scope === "all") {
        try {
          const sessionResults = await client.searchSessionArtifacts({
            query,
            tool_name: params.tool_name,
            max_results: maxResults,
          });
          allResults.push(
            ...sessionResults.map((r) => ({
              artifact_id: r.artifact_id,
              tool_name: r.tool_name,
              summary: r.summary,
              created_at: r.created_at,
              token_estimate: 0,
              scope: "session",
            })),
          );
        } catch (err) {
          console.warn(`[EB] Session artifact search failed: ${err}`);
        }
      }

      if (scope === "persistent" || scope === "all") {
        try {
          const persistentResults = await client.searchArtifacts(query, maxResults);
          allResults.push(
            ...persistentResults.map((r) => ({
              artifact_id: r.artifact_id,
              tool_name: r.tool_name,
              summary: r.summary,
              created_at: r.created_at,
              token_estimate: r.token_estimate || 0,
              scope: "persistent",
            })),
          );
        } catch (err) {
          console.warn(`[EB] Persistent artifact search failed: ${err}`);
        }
      }

      // Dedup by artifact_id, take top N
      const seen = new Set<string>();
      const deduped = allResults.filter((r) => {
        if (seen.has(r.artifact_id)) return false;
        seen.add(r.artifact_id);
        return true;
      });

      return {
        content: [{ type: "text", text: JSON.stringify({
          results: deduped.slice(0, maxResults),
          total: deduped.length,
          lookup_type: "search",
        }) }],
      };
    },
  };
}
