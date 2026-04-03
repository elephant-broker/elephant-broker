import type { ElephantBrokerClient } from "../client.js";

export function createProcedureStatusTool(client: ElephantBrokerClient) {
  return {
    id: "procedure_session_status",
    name: "procedure_session_status",
    description:
      "Get the status of all active procedure executions in the current session. " +
      "Returns each execution with its procedure_id, current step, and completed steps.",
    parameters: {
      type: "object",
      properties: {},
    },
    async execute(toolCallId: string, params: Record<string, never>, signal?: AbortSignal) {
      const result = await client.getSessionProcedureStatus();
      return {
        content: [{ type: "text", text: JSON.stringify(result) }],
      };
    },
  };
}
