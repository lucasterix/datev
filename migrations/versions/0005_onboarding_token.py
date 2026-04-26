"""onboarding token table

Revision ID: 0005_onboarding_token
Revises: 0004_employee_patti_link
Create Date: 2026-04-27

"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0005_onboarding_token"
down_revision: Union[str, None] = "0004_employee_patti_link"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "datev_onboarding_token",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("token", sa.String(length=64), nullable=False),
        sa.Column("prefill", sa.JSON(), nullable=True),
        sa.Column("consumed_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("consumed_payload", sa.JSON(), nullable=True),
        sa.Column("revoked_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("created_by_email", sa.String(length=255), nullable=True),
        sa.Column("label", sa.String(length=120), nullable=True),
        sa.Column("note", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
    )
    op.create_index(
        "ix_onboarding_token_token",
        "datev_onboarding_token",
        ["token"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("ix_onboarding_token_token", table_name="datev_onboarding_token")
    op.drop_table("datev_onboarding_token")
