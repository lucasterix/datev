"""payroll statement + line item tables

Revision ID: 0003_payroll_statement
Revises: 0002_employee
Create Date: 2026-04-25

"""
from __future__ import annotations

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

revision: str = "0003_payroll_statement"
down_revision: Union[str, None] = "0002_employee"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "datev_payroll_statement",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("client_id_path", sa.String(length=32), nullable=False),
        sa.Column("consultant_number", sa.String(length=16), nullable=False),
        sa.Column("client_number", sa.String(length=16), nullable=False),
        sa.Column("personnel_number", sa.Integer(), nullable=False),
        sa.Column("reference_month", sa.Date(), nullable=False),
        # person snapshot
        sa.Column("surname", sa.String(length=80), nullable=True),
        sa.Column("first_name", sa.String(length=80), nullable=True),
        sa.Column("street", sa.String(length=120), nullable=True),
        sa.Column("postal_code", sa.String(length=10), nullable=True),
        sa.Column("city", sa.String(length=80), nullable=True),
        sa.Column("country_code", sa.String(length=4), nullable=True),
        sa.Column("date_of_birth", sa.Date(), nullable=True),
        sa.Column("sex", sa.String(length=16), nullable=True),
        sa.Column("social_security_number", sa.String(length=32), nullable=True),
        sa.Column("nationality", sa.String(length=40), nullable=True),
        sa.Column("iban", sa.String(length=40), nullable=True),
        sa.Column("bic", sa.String(length=20), nullable=True),
        # employment
        sa.Column("date_of_joining", sa.Date(), nullable=True),
        sa.Column("date_of_leaving", sa.Date(), nullable=True),
        sa.Column("job_title", sa.String(length=120), nullable=True),
        sa.Column("activity_type", sa.String(length=80), nullable=True),
        sa.Column("employee_group_code", sa.String(length=8), nullable=True),
        sa.Column("contribution_group_key", sa.String(length=8), nullable=True),
        # tax
        sa.Column("tax_class", sa.Integer(), nullable=True),
        sa.Column("tax_factor", sa.Numeric(6, 4), nullable=True),
        sa.Column("child_tax_allowances", sa.Numeric(6, 2), nullable=True),
        sa.Column("denomination", sa.String(length=8), nullable=True),
        sa.Column("spouse_denomination", sa.String(length=8), nullable=True),
        sa.Column("tax_identification_number", sa.String(length=16), nullable=True),
        sa.Column("finanzamt_number", sa.String(length=16), nullable=True),
        # insurer
        sa.Column("health_insurer_name", sa.String(length=80), nullable=True),
        sa.Column("health_insurer_number", sa.String(length=16), nullable=True),
        # time + leave
        sa.Column("weekly_working_hours", sa.Numeric(5, 2), nullable=True),
        sa.Column("annual_vacation_days", sa.Numeric(5, 2), nullable=True),
        sa.Column("vacation_entitlement_current_year", sa.Numeric(5, 2), nullable=True),
        sa.Column("vacation_taken_current_year", sa.Numeric(5, 2), nullable=True),
        sa.Column("vacation_remaining_current_year", sa.Numeric(5, 2), nullable=True),
        # monthly aggregates
        sa.Column("gross_total", sa.Numeric(12, 2), nullable=True),
        sa.Column("gross_tax", sa.Numeric(12, 2), nullable=True),
        sa.Column("tax_deductions", sa.Numeric(12, 2), nullable=True),
        sa.Column("gross_kv_pv", sa.Numeric(12, 2), nullable=True),
        sa.Column("gross_rv_av", sa.Numeric(12, 2), nullable=True),
        sa.Column("ss_deductions", sa.Numeric(12, 2), nullable=True),
        sa.Column("net_income", sa.Numeric(12, 2), nullable=True),
        sa.Column("payout_eur", sa.Numeric(12, 2), nullable=True),
        # flat-rate tax
        sa.Column("flat_tax_lst", sa.Numeric(12, 2), nullable=True),
        sa.Column("flat_tax_kist", sa.Numeric(12, 2), nullable=True),
        sa.Column("flat_tax_solz", sa.Numeric(12, 2), nullable=True),
        # YTD
        sa.Column("gross_total_ytd", sa.Numeric(14, 2), nullable=True),
        sa.Column("gross_tax_ytd", sa.Numeric(14, 2), nullable=True),
        sa.Column("gross_ss_ytd", sa.Numeric(14, 2), nullable=True),
        # employer SV
        sa.Column("ss_employer_share_monthly", sa.Numeric(12, 2), nullable=True),
        sa.Column("ss_employer_share_ytd", sa.Numeric(12, 2), nullable=True),
        sa.Column("allocation_contributions", sa.Numeric(12, 2), nullable=True),
        # hours
        sa.Column("days_present", sa.Numeric(6, 2), nullable=True),
        sa.Column("hours_present", sa.Numeric(8, 2), nullable=True),
        sa.Column("hours_paid", sa.Numeric(8, 2), nullable=True),
        sa.Column("days_sick", sa.Numeric(6, 2), nullable=True),
        sa.Column("hours_sick", sa.Numeric(8, 2), nullable=True),
        sa.Column("hours_overtime", sa.Numeric(8, 2), nullable=True),
        # provenance
        sa.Column("processing_code", sa.String(length=8), nullable=True),
        sa.Column("import_batch_id", sa.String(length=64), nullable=True),
        sa.Column("imported_by_email", sa.String(length=255), nullable=True),
        sa.Column(
            "imported_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=False,
        ),
        sa.Column("raw_sd", sa.JSON(), nullable=True),
        sa.Column("import_notes", sa.Text(), nullable=True),
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
        "ix_datev_payroll_statement_client_id_path",
        "datev_payroll_statement",
        ["client_id_path"],
    )
    op.create_index(
        "ix_datev_payroll_statement_personnel_number",
        "datev_payroll_statement",
        ["personnel_number"],
    )
    op.create_index(
        "ix_datev_payroll_statement_reference_month",
        "datev_payroll_statement",
        ["reference_month"],
    )
    op.create_index(
        "ix_datev_payroll_statement_import_batch_id",
        "datev_payroll_statement",
        ["import_batch_id"],
    )
    op.create_unique_constraint(
        "uq_payroll_statement_tenant_pnr_month",
        "datev_payroll_statement",
        ["client_id_path", "personnel_number", "reference_month"],
    )

    op.create_table(
        "datev_payroll_line_item",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "statement_id",
            sa.Integer(),
            sa.ForeignKey("datev_payroll_statement.id", ondelete="CASCADE"),
            nullable=False,
        ),
        sa.Column("personnel_number", sa.Integer(), nullable=False),
        sa.Column("allocation_date", sa.Date(), nullable=False),
        sa.Column("processing_code", sa.String(length=8), nullable=True),
        sa.Column("salary_type_code", sa.Integer(), nullable=False),
        sa.Column("salary_type_name", sa.String(length=120), nullable=True),
        sa.Column("unit", sa.String(length=32), nullable=True),
        sa.Column("quantity", sa.Numeric(12, 4), nullable=True),
        sa.Column("factor", sa.Numeric(12, 4), nullable=True),
        sa.Column("percentage", sa.Numeric(8, 4), nullable=True),
        sa.Column("tax_flag", sa.String(length=4), nullable=True),
        sa.Column("ss_flag", sa.String(length=4), nullable=True),
        sa.Column("gb_flag", sa.String(length=4), nullable=True),
        sa.Column("amount", sa.Numeric(12, 2), nullable=False),
        sa.Column("raw_la", sa.JSON(), nullable=True),
    )
    op.create_index(
        "ix_datev_payroll_line_item_statement_id",
        "datev_payroll_line_item",
        ["statement_id"],
    )
    op.create_index(
        "ix_datev_payroll_line_item_personnel_number",
        "datev_payroll_line_item",
        ["personnel_number"],
    )


def downgrade() -> None:
    op.drop_index(
        "ix_datev_payroll_line_item_personnel_number",
        table_name="datev_payroll_line_item",
    )
    op.drop_index(
        "ix_datev_payroll_line_item_statement_id",
        table_name="datev_payroll_line_item",
    )
    op.drop_table("datev_payroll_line_item")

    op.drop_constraint(
        "uq_payroll_statement_tenant_pnr_month",
        "datev_payroll_statement",
        type_="unique",
    )
    op.drop_index(
        "ix_datev_payroll_statement_import_batch_id",
        table_name="datev_payroll_statement",
    )
    op.drop_index(
        "ix_datev_payroll_statement_reference_month",
        table_name="datev_payroll_statement",
    )
    op.drop_index(
        "ix_datev_payroll_statement_personnel_number",
        table_name="datev_payroll_statement",
    )
    op.drop_index(
        "ix_datev_payroll_statement_client_id_path",
        table_name="datev_payroll_statement",
    )
    op.drop_table("datev_payroll_statement")
