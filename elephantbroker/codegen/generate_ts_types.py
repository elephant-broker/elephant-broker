"""Generate TypeScript type definitions from Pydantic schemas."""
from __future__ import annotations

import json
from pathlib import Path

from elephantbroker.schemas.artifact import ArtifactSummary, ToolArtifact
from elephantbroker.schemas.base import Scope
from elephantbroker.schemas.fact import FactAssertion, MemoryClass
from elephantbroker.schemas.goal import GoalState
from elephantbroker.schemas.procedure import ProcedureDefinition
from elephantbroker.schemas.profile import Budgets, ProfilePolicy, RetrievalPolicy
from elephantbroker.schemas.working_set import ScoringWeights
from elephantbroker.schemas.actor import ActorRef

# Models to export as TypeScript types
EXPORTABLE_MODELS = {
    "FactAssertion": FactAssertion,
    "ActorRef": ActorRef,
    "GoalState": GoalState,
    "ToolArtifact": ToolArtifact,
    "ArtifactSummary": ArtifactSummary,
    "ProcedureDefinition": ProcedureDefinition,
    "ProfilePolicy": ProfilePolicy,
    "Budgets": Budgets,
    "RetrievalPolicy": RetrievalPolicy,
    "ScoringWeights": ScoringWeights,
}


def generate_json_schemas(output_dir: str | Path) -> dict[str, Path]:
    """Generate JSON Schema files from Pydantic models.

    Returns mapping of model name to output file path.
    """
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    result = {}
    for name, model in EXPORTABLE_MODELS.items():
        schema = model.model_json_schema()
        out_path = output_dir / f"{name}.schema.json"
        out_path.write_text(json.dumps(schema, indent=2))
        result[name] = out_path

    return result


def generate_enum_ts(output_path: str | Path) -> None:
    """Generate TypeScript enum definitions for Scope and MemoryClass."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    lines = [
        "// Auto-generated from Pydantic schemas — do not edit",
        "",
        "export type Scope = " + " | ".join(f'"{s.value}"' for s in Scope) + ";",
        "",
        "export type MemoryClass = " + " | ".join(f'"{m.value}"' for m in MemoryClass) + ";",
        "",
    ]
    output_path.write_text("\n".join(lines))
