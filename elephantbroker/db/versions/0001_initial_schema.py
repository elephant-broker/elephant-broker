"""Baseline migration — creates all 7 audit tables from SQLite schema.

Revision: 0001
Creates:
  - authority_rules
  - org_profile_overrides
  - procedure_events
  - goal_events
  - tuning_deltas
  - scoring_ledger
  - consolidation_reports
  - procedure_suggestions
"""
from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "0001"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    # --- authority_rules ---
    op.create_table(
        "authority_rules",
        sa.Column("action", sa.Text, primary_key=True),
        sa.Column("rule_json", sa.Text, nullable=False),
        sa.Column("updated_at", sa.Text, nullable=False),
    )

    # --- org_profile_overrides ---
    op.create_table(
        "org_profile_overrides",
        sa.Column("org_id", sa.Text, nullable=False),
        sa.Column("profile_id", sa.Text, nullable=False),
        sa.Column("overrides_json", sa.Text, nullable=False),
        sa.Column("updated_at", sa.Text, nullable=False),
        sa.Column("updated_by_actor_id", sa.Text, nullable=True),
        sa.PrimaryKeyConstraint("org_id", "profile_id"),
    )

    # --- procedure_events ---
    op.create_table(
        "procedure_events",
        sa.Column("event_id", sa.Text, primary_key=True),
        sa.Column("session_key", sa.Text, nullable=False),
        sa.Column("session_id", sa.Text, nullable=False),
        sa.Column("procedure_id", sa.Text, nullable=False),
        sa.Column("procedure_name", sa.Text, nullable=False),
        sa.Column("execution_id", sa.Text, nullable=True),
        sa.Column("event_type", sa.Text, nullable=False),
        sa.Column("step_id", sa.Text, nullable=True),
        sa.Column("step_instruction", sa.Text, nullable=True),
        sa.Column("proof_type", sa.Text, nullable=True),
        sa.Column("proof_value", sa.Text, nullable=True),
        sa.Column("timestamp", sa.Text, nullable=False),
        sa.Column("gateway_id", sa.Text, nullable=False, server_default="local"),
    )
    op.create_index("idx_proc_events_session", "procedure_events", ["session_key", "session_id"])
    op.create_index("idx_proc_events_gw_ts", "procedure_events", ["gateway_id", "timestamp"])

    # --- goal_events ---
    op.create_table(
        "goal_events",
        sa.Column("event_id", sa.Text, primary_key=True),
        sa.Column("session_key", sa.Text, nullable=False),
        sa.Column("session_id", sa.Text, nullable=False),
        sa.Column("goal_id", sa.Text, nullable=False),
        sa.Column("goal_title", sa.Text, nullable=False),
        sa.Column("parent_goal_id", sa.Text, nullable=True),
        sa.Column("event_type", sa.Text, nullable=False),
        sa.Column("evidence", sa.Text, nullable=True),
        sa.Column("timestamp", sa.Text, nullable=False),
        sa.Column("gateway_id", sa.Text, nullable=False, server_default="local"),
    )
    op.create_index("idx_goal_events_session", "goal_events", ["session_key", "session_id"])
    op.create_index("idx_goal_events_gw_ts", "goal_events", ["gateway_id", "timestamp"])

    # --- tuning_deltas ---
    op.create_table(
        "tuning_deltas",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("profile_id", sa.Text, nullable=False),
        sa.Column("org_id", sa.Text, nullable=False),
        sa.Column("gateway_id", sa.Text, nullable=False),
        sa.Column("dimension", sa.Text, nullable=False),
        sa.Column("accumulated_delta", sa.Float, nullable=False, server_default="0.0"),
        sa.Column("last_raw_delta", sa.Float, nullable=False, server_default="0.0"),
        sa.Column("cycle_count", sa.Integer, nullable=False, server_default="0"),
        sa.Column("updated_at", sa.Text, nullable=False),
        sa.UniqueConstraint("profile_id", "org_id", "gateway_id", "dimension",
                            name="uq_tuning_deltas_key"),
    )

    # --- scoring_ledger ---
    op.create_table(
        "scoring_ledger",
        sa.Column("id", sa.BigInteger, primary_key=True, autoincrement=True),
        sa.Column("fact_id", sa.Text, nullable=False),
        sa.Column("session_id", sa.Text, nullable=False),
        sa.Column("session_key", sa.Text, nullable=False),
        sa.Column("gateway_id", sa.Text, nullable=False),
        sa.Column("profile_id", sa.Text, nullable=False),
        sa.Column("dim_scores_json", sa.Text, nullable=False),
        sa.Column("was_selected", sa.Boolean, nullable=False),
        sa.Column("successful_use_count_at_scoring", sa.Integer, nullable=False, server_default="0"),
        sa.Column("created_at", sa.Text, nullable=False),
    )
    op.create_index("idx_scoring_ledger_gw", "scoring_ledger", ["gateway_id", "created_at"])

    # --- consolidation_reports ---
    op.create_table(
        "consolidation_reports",
        sa.Column("report_id", sa.Text, primary_key=True),
        sa.Column("org_id", sa.Text, nullable=False),
        sa.Column("gateway_id", sa.Text, nullable=False),
        sa.Column("profile_id", sa.Text, nullable=True),
        sa.Column("started_at", sa.Text, nullable=False),
        sa.Column("completed_at", sa.Text, nullable=True),
        sa.Column("status", sa.Text, nullable=False, server_default="running"),
        sa.Column("summary_json", sa.Text, nullable=True),
        sa.Column("stages_json", sa.Text, nullable=True),
        sa.Column("error", sa.Text, nullable=True),
    )
    op.create_index("idx_consol_reports_gw", "consolidation_reports", ["gateway_id", "started_at"])

    # --- procedure_suggestions ---
    op.create_table(
        "procedure_suggestions",
        sa.Column("id", sa.Text, primary_key=True),
        sa.Column("report_id", sa.Text, nullable=True),
        sa.Column("gateway_id", sa.Text, nullable=False),
        sa.Column("pattern_description", sa.Text, nullable=False),
        sa.Column("tool_sequence_json", sa.Text, nullable=False),
        sa.Column("sessions_observed", sa.Integer, nullable=False, server_default="0"),
        sa.Column("draft_procedure_json", sa.Text, nullable=True),
        sa.Column("confidence", sa.Float, nullable=False, server_default="0.5"),
        sa.Column("approval_status", sa.Text, nullable=False, server_default="pending"),
        sa.Column("created_at", sa.Text, nullable=False),
    )


def downgrade() -> None:
    op.drop_table("procedure_suggestions")
    op.drop_table("consolidation_reports")
    op.drop_table("scoring_ledger")
    op.drop_table("tuning_deltas")
    op.drop_table("goal_events")
    op.drop_table("procedure_events")
    op.drop_table("org_profile_overrides")
    op.drop_table("authority_rules")
