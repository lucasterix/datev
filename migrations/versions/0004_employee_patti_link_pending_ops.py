"""employee patti link + pending operations queue

Revision ID: 0004_employee_patti_link
Revises: 0003_payroll_statement
Create Date: 2026-04-26

"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0004_employee_patti_link"
down_revision: Union[str, None] = "0003_payroll_statement"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # --- extend datev_employee with Patti link + sync bookkeeping ---------
    op.add_column(
        "datev_employee",
        sa.Column("patti_user_id", sa.Integer(), nullable=True),
    )
    op.add_column(
        "datev_employee",
        sa.Column("patti_person_id", sa.Integer(), nullable=True),
    )
    op.add_column(
        "datev_employee",
        sa.Column(
            "patti_link_state",
            sa.String(length=16),
            nullable=False,
            server_default="unmatched",
        ),
    )
    op.add_column(
        "datev_employee",
        sa.Column("raw_patti", sa.JSON(), nullable=True),
    )
    op.add_column(
        "datev_employee",
        sa.Column("patti_updated_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "datev_employee",
        sa.Column("last_patti_synced_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "datev_employee",
        sa.Column("datev_data_hash", sa.String(length=64), nullable=True),
    )
    op.add_column(
        "datev_employee",
        sa.Column("last_datev_synced_at", sa.DateTime(timezone=True), nullable=True),
    )

    op.create_index(
        "ix_datev_employee_patti_user_id",
        "datev_employee",
        ["patti_user_id"],
    )
    op.create_index(
        "ix_datev_employee_patti_person_id",
        "datev_employee",
        ["patti_person_id"],
    )

    # --- pending_operation queue ----------------------------------------
    op.create_table(
        "datev_pending_operation",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "employee_id",
            sa.Integer(),
            sa.ForeignKey("datev_employee.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("op", sa.String(length=48), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column(
            "status",
            sa.String(length=16),
            nullable=False,
            server_default="pending",
        ),
        sa.Column("attempts", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("not_before", sa.DateTime(timezone=True), nullable=True),
        sa.Column("requested_by_email", sa.String(length=255), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.create_index(
        "ix_pending_operation_employee_id",
        "datev_pending_operation",
        ["employee_id"],
    )
    op.create_index(
        "ix_pending_operation_op",
        "datev_pending_operation",
        ["op"],
    )
    op.create_index(
        "ix_pending_operation_status",
        "datev_pending_operation",
        ["status"],
    )
    op.create_index(
        "ix_pending_operation_not_before",
        "datev_pending_operation",
        ["not_before"],
    )


def downgrade() -> None:
    op.drop_index("ix_pending_operation_not_before", table_name="datev_pending_operation")
    op.drop_index("ix_pending_operation_status", table_name="datev_pending_operation")
    op.drop_index("ix_pending_operation_op", table_name="datev_pending_operation")
    op.drop_index("ix_pending_operation_employee_id", table_name="datev_pending_operation")
    op.drop_table("datev_pending_operation")

    op.drop_index("ix_datev_employee_patti_person_id", table_name="datev_employee")
    op.drop_index("ix_datev_employee_patti_user_id", table_name="datev_employee")

    op.drop_column("datev_employee", "last_datev_synced_at")
    op.drop_column("datev_employee", "datev_data_hash")
    op.drop_column("datev_employee", "last_patti_synced_at")
    op.drop_column("datev_employee", "patti_updated_at")
    op.drop_column("datev_employee", "raw_patti")
    op.drop_column("datev_employee", "patti_link_state")
    op.drop_column("datev_employee", "patti_person_id")
    op.drop_column("datev_employee", "patti_user_id")
