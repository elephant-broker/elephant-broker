import type { ElephantBrokerClient } from "../client.js";

export function createProcedureCompleteTool(client: ElephantBrokerClient) {
  return {
    id: "procedure_complete_step",
    name: "procedure_complete_step",
    description:
      "Mark a procedure step as complete. Provide the execution_id and step_id " +
      "from the active procedure execution, plus optional evidence of completion.",
    parameters: {
      type: "object",
      properties: {
        execution_id: { type: "string", description: "UUID of the active procedure execution" },
        step_id: { type: "string", description: "UUID of the step to mark complete" },
        evidence: { type: "string", description: "Evidence that the step was completed" },
        proof_type: {
          type: "string",
          enum: ["diff_hash", "chunk_ref", "receipt", "version_record", "supervisor_sign_off"],
          description: "Type of proof provided",
        },
      },
      required: ["execution_id", "step_id"],
    },
    async execute(toolCallId: string, params: {
      execution_id: string;
      step_id: string;
      evidence?: string;
      proof_type?: string;
    }, signal?: AbortSignal) {
      const result = await client.completeProcedureStep(params.execution_id, params.step_id, {
        evidence: params.evidence,
        proof_type: params.proof_type,
      });
      return {
        content: [{ type: "text", text: JSON.stringify(result) }],
      };
    },
  };
}
