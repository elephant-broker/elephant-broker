import type { ElephantBrokerClient } from "../client.js";

export function createProcedureActivateTool(client: ElephantBrokerClient) {
  return {
    id: "procedure_activate",
    name: "procedure_activate",
    description:
      "Activate a procedure for execution. Returns an execution object with an execution_id " +
      "that tracks progress through the procedure steps.",
    parameters: {
      type: "object",
      properties: {
        procedure_id: { type: "string", description: "UUID of the procedure to activate" },
        actor_id: { type: "string", description: "UUID of the actor executing the procedure" },
      },
      required: ["procedure_id", "actor_id"],
    },
    async execute(toolCallId: string, params: { procedure_id: string; actor_id: string }, signal?: AbortSignal) {
      const result = await client.activateProcedure(params.procedure_id, {
        actor_id: params.actor_id,
      });
      return {
        content: [{ type: "text", text: JSON.stringify(result) }],
      };
    },
  };
}
