"""employee table

Revision ID: 0002_employee
Revises: 0001_datev_oauth
Create Date: 2026-04-24

"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0002_employee"
down_revision: Union[str, None] = "0001_datev_oauth"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "datev_employee",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("client_id_path", sa.String(length=32), nullable=False),
        sa.Column("personnel_number", sa.Integer(), nullable=False),
        sa.Column("company_personnel_number", sa.String(length=40), nullable=True),
        sa.Column("first_name", sa.String(length=60), nullable=True),
        sa.Column("surname", sa.String(length=60), nullable=True),
        sa.Column("date_of_birth", sa.Date(), nullable=True),
        sa.Column("date_of_joining", sa.Date(), nullable=True),
        sa.Column("date_of_leaving", sa.Date(), nullable=True),
        sa.Column("job_title", sa.String(length=120), nullable=True),
        sa.Column("weekly_working_hours", sa.Float(), nullable=True),
        sa.Column("type_of_contract", sa.String(length=4), nullable=True),
        sa.Column("source_system", sa.String(length=8), nullable=True),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default=sa.true()),
        sa.Column("raw_masterdata", sa.JSON(), nullable=True),
        sa.Column("pending_changes", sa.JSON(), nullable=True),
        sa.Column("pending_since", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_synced_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("last_sync_status", sa.String(length=16), nullable=True),
        sa.Column("last_sync_error", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "ix_datev_employee_client_id_path", "datev_employee", ["client_id_path"]
    )
    op.create_index(
        "ix_datev_employee_personnel_number", "datev_employee", ["personnel_number"]
    )
    op.create_unique_constraint(
        "uq_employee_tenant_pnr",
        "datev_employee",
        ["client_id_path", "personnel_number"],
    )


def downgrade() -> None:
    op.drop_constraint("uq_employee_tenant_pnr", "datev_employee", type_="unique")
    op.drop_index("ix_datev_employee_personnel_number", table_name="datev_employee")
    op.drop_index("ix_datev_employee_client_id_path", table_name="datev_employee")
    op.drop_table("datev_employee")
