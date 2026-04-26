from datetime import date, datetime

from sqlalchemy import (
    Date,
    DateTime,
    ForeignKey,
    Integer,
    JSON,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


class PayrollStatement(Base):
    """One monthly payroll statement per employee, parsed from a LuG-ASCII
    SD export file.

    The SD file delivers ~155 columns; we materialize the ones needed for
    filtering, list displays and the PDF header/footer as columns, and
    keep the full row in ``raw_sd`` for future fields we haven't promoted yet.

    ``reference_month`` is the Abrechnungsdatum truncated to the first of
    the month — the month the run was executed. Prior-month corrections
    belonging to this run live as line items with divergent ``Zuordnungsdatum``.
    """

    __tablename__ = "datev_payroll_statement"
    __table_args__ = (
        UniqueConstraint(
            "client_id_path",
            "personnel_number",
            "reference_month",
            name="uq_payroll_statement_tenant_pnr_month",
        ),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # === tenant + run identification ===
    client_id_path: Mapped[str] = mapped_column(String(32), nullable=False, index=True)
    consultant_number: Mapped[str] = mapped_column(String(16), nullable=False)
    client_number: Mapped[str] = mapped_column(String(16), nullable=False)
    personnel_number: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    reference_month: Mapped[date] = mapped_column(Date, nullable=False, index=True)

    # === person snapshot (so archived statements stay readable even if
    # the employee leaves and the live masterdata gets deleted) ===
    surname: Mapped[str | None] = mapped_column(String(80), nullable=True)
    first_name: Mapped[str | None] = mapped_column(String(80), nullable=True)
    street: Mapped[str | None] = mapped_column(String(120), nullable=True)
    postal_code: Mapped[str | None] = mapped_column(String(10), nullable=True)
    city: Mapped[str | None] = mapped_column(String(80), nullable=True)
    country_code: Mapped[str | None] = mapped_column(String(4), nullable=True)
    date_of_birth: Mapped[date | None] = mapped_column(Date, nullable=True)
    sex: Mapped[str | None] = mapped_column(String(16), nullable=True)
    social_security_number: Mapped[str | None] = mapped_column(String(32), nullable=True)
    nationality: Mapped[str | None] = mapped_column(String(40), nullable=True)
    iban: Mapped[str | None] = mapped_column(String(40), nullable=True)
    bic: Mapped[str | None] = mapped_column(String(20), nullable=True)

    # === employment context ===
    date_of_joining: Mapped[date | None] = mapped_column(Date, nullable=True)
    date_of_leaving: Mapped[date | None] = mapped_column(Date, nullable=True)
    job_title: Mapped[str | None] = mapped_column(String(120), nullable=True)
    activity_type: Mapped[str | None] = mapped_column(String(80), nullable=True)
    employee_group_code: Mapped[str | None] = mapped_column(String(8), nullable=True)  # PGS
    contribution_group_key: Mapped[str | None] = mapped_column(String(8), nullable=True)  # BGRS

    # === tax-card snapshot ===
    tax_class: Mapped[int | None] = mapped_column(Integer, nullable=True)
    tax_factor: Mapped[float | None] = mapped_column(Numeric(6, 4), nullable=True)
    child_tax_allowances: Mapped[float | None] = mapped_column(Numeric(6, 2), nullable=True)
    denomination: Mapped[str | None] = mapped_column(String(8), nullable=True)
    spouse_denomination: Mapped[str | None] = mapped_column(String(8), nullable=True)
    tax_identification_number: Mapped[str | None] = mapped_column(String(16), nullable=True)
    finanzamt_number: Mapped[str | None] = mapped_column(String(16), nullable=True)

    # === health insurer snapshot ===
    health_insurer_name: Mapped[str | None] = mapped_column(String(80), nullable=True)
    health_insurer_number: Mapped[str | None] = mapped_column(String(16), nullable=True)

    # === working-time + leave snapshot ===
    weekly_working_hours: Mapped[float | None] = mapped_column(Numeric(5, 2), nullable=True)
    annual_vacation_days: Mapped[float | None] = mapped_column(Numeric(5, 2), nullable=True)
    vacation_entitlement_current_year: Mapped[float | None] = mapped_column(Numeric(5, 2), nullable=True)
    vacation_taken_current_year: Mapped[float | None] = mapped_column(Numeric(5, 2), nullable=True)
    vacation_remaining_current_year: Mapped[float | None] = mapped_column(Numeric(5, 2), nullable=True)

    # === monthly aggregates (the core of the payslip) ===
    gross_total: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    gross_tax: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    tax_deductions: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    gross_kv_pv: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    gross_rv_av: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    ss_deductions: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    net_income: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    payout_eur: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)

    # === flat-rate taxes ===
    flat_tax_lst: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    flat_tax_kist: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    flat_tax_solz: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)

    # === year-to-date cumulatives ===
    gross_total_ytd: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    gross_tax_ytd: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)
    gross_ss_ytd: Mapped[float | None] = mapped_column(Numeric(14, 2), nullable=True)

    # === employer-side SV shares ===
    ss_employer_share_monthly: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    ss_employer_share_ytd: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)
    allocation_contributions: Mapped[float | None] = mapped_column(Numeric(12, 2), nullable=True)

    # === attendance/hours ===
    days_present: Mapped[float | None] = mapped_column(Numeric(6, 2), nullable=True)
    hours_present: Mapped[float | None] = mapped_column(Numeric(8, 2), nullable=True)
    hours_paid: Mapped[float | None] = mapped_column(Numeric(8, 2), nullable=True)
    days_sick: Mapped[float | None] = mapped_column(Numeric(6, 2), nullable=True)
    hours_sick: Mapped[float | None] = mapped_column(Numeric(8, 2), nullable=True)
    hours_overtime: Mapped[float | None] = mapped_column(Numeric(8, 2), nullable=True)

    # === import provenance ===
    processing_code: Mapped[str | None] = mapped_column(String(8), nullable=True)  # "90G" etc.
    import_batch_id: Mapped[str | None] = mapped_column(String(64), nullable=True, index=True)
    imported_by_email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    imported_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    # Full SD row as-parsed. All ~155 fields by german header name.
    raw_sd: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    # Notes on import warnings (e.g. missing IBAN, unexpected column count).
    import_notes: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    line_items: Mapped[list["PayrollLineItem"]] = relationship(
        back_populates="statement",
        cascade="all, delete-orphan",
        order_by="PayrollLineItem.allocation_date, PayrollLineItem.processing_code, PayrollLineItem.salary_type_code",
    )

    @property
    def full_name(self) -> str:
        parts = [self.first_name, self.surname]
        return " ".join(p for p in parts if p) or f"#{self.personnel_number}"


class PayrollLineItem(Base):
    """One line from a LuG-ASCII LA export: a single wage type applied to
    an employee for a specific allocation month.

    The same statement can own multiple line items, e.g. base salary,
    benefits-in-kind, and prior-month corrections booked into this run.
    We distinguish current-month entries from retroactive corrections by
    comparing ``allocation_date`` to ``PayrollStatement.reference_month``.
    """

    __tablename__ = "datev_payroll_line_item"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    statement_id: Mapped[int] = mapped_column(
        ForeignKey("datev_payroll_statement.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # === identity (redundant with statement but handy for raw queries) ===
    personnel_number: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    allocation_date: Mapped[date] = mapped_column(Date, nullable=False)
    processing_code: Mapped[str | None] = mapped_column(String(8), nullable=True)

    # === wage type ===
    salary_type_code: Mapped[int] = mapped_column(Integer, nullable=False)
    salary_type_name: Mapped[str | None] = mapped_column(String(120), nullable=True)
    unit: Mapped[str | None] = mapped_column(String(32), nullable=True)
    quantity: Mapped[float | None] = mapped_column(Numeric(12, 4), nullable=True)
    factor: Mapped[float | None] = mapped_column(Numeric(12, 4), nullable=True)
    percentage: Mapped[float | None] = mapped_column(Numeric(8, 4), nullable=True)

    # === tax/SV flags as shown in the export ===
    tax_flag: Mapped[str | None] = mapped_column(String(4), nullable=True)
    ss_flag: Mapped[str | None] = mapped_column(String(4), nullable=True)
    gb_flag: Mapped[str | None] = mapped_column(String(4), nullable=True)

    amount: Mapped[float] = mapped_column(Numeric(12, 2), nullable=False)

    raw_la: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    statement: Mapped[PayrollStatement] = relationship(back_populates="line_items")

    @property
    def is_retroactive(self) -> bool:
        """True if this line item refers to a month other than its statement's
        reference month — i.e. a correction posted into the current run."""
        return self.allocation_date.replace(day=1) != self.statement.reference_month
