import type { ElephantBrokerClient } from "../client.js";

export function createProcedureCreateTool(client: ElephantBrokerClient) {
  return {
    id: "procedure_create",
    name: "procedure_create",
    description:
      "Create a new procedure — a known sequence of steps for accomplishing a task. " +
      "Each step has an instruction and optional evidence requirements.",
    parameters: {
      type: "object",
      properties: {
        name: { type: "string", description: "Short name for the procedure" },
        description: { type: "string", description: "What this procedure accomplishes" },
        scope: { type: "string", description: "Visibility scope (session, actor, team, global)" },
        steps: {
          type: "array",
          items: {
            type: "object",
            properties: {
              order: { type: "number", description: "Step order (0-indexed)" },
              instruction: { type: "string", description: "What to do in this step" },
              is_optional: { type: "boolean", description: "Whether this step can be skipped" },
            },
            required: ["order", "instruction"],
          },
          description: "Ordered list of procedure steps",
        },
      },
      required: ["name", "steps"],
    },
    async execute(toolCallId: string, params: {
      name: string;
      description?: string;
      scope?: string;
      steps: Array<{ order: number; instruction: string; is_optional?: boolean }>;
    }, signal?: AbortSignal) {
      const result = await client.createProcedure({
        name: params.name,
        description: params.description,
        scope: params.scope,
        steps: params.steps,
      });
      return {
        content: [{ type: "text", text: JSON.stringify(result) }],
      };
    },
  };
}
