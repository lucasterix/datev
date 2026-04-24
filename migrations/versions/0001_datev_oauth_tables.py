"""datev oauth tables

Revision ID: 0001_datev_oauth
Revises:
Create Date: 2026-04-24

"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0001_datev_oauth"
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "datev_oauth_state",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("state", sa.String(length=128), nullable=False),
        sa.Column("code_verifier", sa.String(length=256), nullable=False),
        sa.Column("initiated_by_user_id", sa.Integer(), nullable=True),
        sa.Column("initiated_by_email", sa.String(length=255), nullable=True),
        sa.Column("return_to", sa.String(length=512), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
    )
    op.create_index(
        "ix_datev_oauth_state_state",
        "datev_oauth_state",
        ["state"],
        unique=True,
    )

    op.create_table(
        "datev_token",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("scope_key", sa.String(length=64), nullable=False),
        sa.Column("access_token", sa.Text(), nullable=False),
        sa.Column("refresh_token", sa.Text(), nullable=True),
        sa.Column(
            "token_type",
            sa.String(length=32),
            nullable=False,
            server_default="Bearer",
        ),
        sa.Column("scope", sa.Text(), nullable=False, server_default=""),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
        sa.Column("id_token_claims", sa.JSON(), nullable=True),
        sa.Column("connected_by_user_id", sa.Integer(), nullable=True),
        sa.Column("connected_by_email", sa.String(length=255), nullable=True),
        sa.Column(
            "connected_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.func.now(),
        ),
        sa.Column("last_refreshed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index(
        "ix_datev_token_scope_key",
        "datev_token",
        ["scope_key"],
        unique=True,
    )


def downgrade() -> None:
    op.drop_index("ix_datev_token_scope_key", table_name="datev_token")
    op.drop_table("datev_token")
    op.drop_index("ix_datev_oauth_state_state", table_name="datev_oauth_state")
    op.drop_table("datev_oauth_state")
