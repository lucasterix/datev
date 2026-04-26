"""Payroll statement import + retrieval for the Buchhaltung UI."""

from __future__ import annotations

from datetime import date
from typing import Annotated, Any

from fastapi import APIRouter, Depends, File, HTTPException, Query, UploadFile, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session, selectinload

from app.core.auth import AuthenticatedUser, require_buchhaltung_user
from app.core.datev_client import default_client_path
from app.db.session import get_db
from app.models.payroll_statement import PayrollLineItem, PayrollStatement
from app.services.payroll_import import (
    PayrollImportError,
    import_lug_ascii,
)

router = APIRouter(prefix="/datev/payroll", tags=["payroll"])


# Cap uploads at 20 MB per file — a year of exports for our headcount
# sits comfortably under 2 MB, so 20 is generous but bounded.
MAX_UPLOAD_BYTES = 20 * 1024 * 1024


# --- response schemas -------------------------------------------------------


class StatementSummary(BaseModel):
    id: int
    personnel_number: int
    full_name: str
    first_name: str | None
    surname: str | None
    reference_month: str  # YYYY-MM
    gross_total: float | None
    net_income: float | None
    payout_eur: float | None
    processing_code: str | None
    date_of_leaving: str | None

    @classmethod
    def from_row(cls, s: PayrollStatement) -> "StatementSummary":
        return cls(
            id=s.id,
            personnel_number=s.personnel_number,
            full_name=s.full_name,
            first_name=s.first_name,
            surname=s.surname,
            reference_month=s.reference_month.strftime("%Y-%m"),
            gross_total=float(s.gross_total) if s.gross_total is not None else None,
            net_income=float(s.net_income) if s.net_income is not None else None,
            payout_eur=float(s.payout_eur) if s.payout_eur is not None else None,
            processing_code=s.processing_code,
            date_of_leaving=s.date_of_leaving.isoformat() if s.date_of_leaving else None,
        )


class LineItemOut(BaseModel):
    id: int
    allocation_date: str
    processing_code: str | None
    salary_type_code: int
    salary_type_name: str | None
    unit: str | None
    quantity: float | None
    factor: float | None
    percentage: float | None
    tax_flag: str | None
    ss_flag: str | None
    gb_flag: str | None
    amount: float
    is_retroactive: bool

    @classmethod
    def from_row(cls, li: PayrollLineItem, statement_month: date) -> "LineItemOut":
        return cls(
            id=li.id,
            allocation_date=li.allocation_date.isoformat(),
            processing_code=li.processing_code,
            salary_type_code=li.salary_type_code,
            salary_type_name=li.salary_type_name,
            unit=li.unit,
            quantity=float(li.quantity) if li.quantity is not None else None,
            factor=float(li.factor) if li.factor is not None else None,
            percentage=float(li.percentage) if li.percentage is not None else None,
            tax_flag=li.tax_flag,
            ss_flag=li.ss_flag,
            gb_flag=li.gb_flag,
            amount=float(li.amount),
            is_retroactive=li.allocation_date.replace(day=1) != statement_month,
        )


class StatementDetail(BaseModel):
    summary: StatementSummary
    # All the snapshot fields needed for the PDF, pre-decoded for the
    # frontend. Anything extra lives in ``raw_sd`` for completeness.
    snapshot: dict[str, Any]
    aggregates: dict[str, float | None]
    ytd: dict[str, float | None]
    hours: dict[str, float | None]
    line_items: list[LineItemOut]
    raw_sd: dict | None


# --- import endpoint --------------------------------------------------------


class ImportResponse(BaseModel):
    batch_id: str
    reference_month: str
    consultant_number: str
    client_number: str
    statements_created: int
    statements_updated: int
    line_items_written: int
    skipped_sd_rows: int
    skipped_la_rows: int
    warnings: list[str]


@router.post("/import-lug-ascii", response_model=ImportResponse)
async def import_lug_ascii_endpoint(
    user: Annotated[AuthenticatedUser, Depends(require_buchhaltung_user)],
    sd_file: UploadFile = File(
        ..., description="LuG ASCII Stammdaten-Export (SD-Datei)"
    ),
    la_file: UploadFile = File(
        ..., description="LuG ASCII Lohnarten-Export (LA-Datei)"
    ),
    db: Session = Depends(get_db),
) -> ImportResponse:
    """Import a monthly LuG-ASCII export pair.

    Both files must belong to the same Abrechnungslauf (same consultant,
    client, and Abrechnungsdatum). Re-uploading the same month replaces
    the affected statements' line items — idempotent from the caller's
    perspective.
    """
    sd_bytes = await _read_upload(sd_file)
    la_bytes = await _read_upload(la_file)

    try:
        result = import_lug_ascii(
            db,
            sd_bytes=sd_bytes,
            la_bytes=la_bytes,
            expected_client_id_path=default_client_path(),
            imported_by_email=user.email,
        )
    except PayrollImportError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except ValueError as exc:
        # file-level parser error
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc

    return ImportResponse(**result.to_dict())


async def _read_upload(upload: UploadFile) -> bytes:
    data = await upload.read()
    if len(data) > MAX_UPLOAD_BYTES:
        raise HTTPException(
            status_code=status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            detail=f"Datei {upload.filename!r} überschreitet {MAX_UPLOAD_BYTES // (1024*1024)} MB",
        )
    if not data:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Datei {upload.filename!r} ist leer",
        )
    return data


# --- list + detail endpoints ----------------------------------------------


@router.get("/statements", response_model=list[StatementSummary])
def list_statements(
    user: Annotated[AuthenticatedUser, Depends(require_buchhaltung_user)],
    month: str | None = Query(
        default=None,
        pattern=r"^\d{4}-\d{2}$",
        description="YYYY-MM filter; omit for all months",
    ),
    db: Session = Depends(get_db),
) -> list[StatementSummary]:
    query = select(PayrollStatement).where(
        PayrollStatement.client_id_path == default_client_path()
    )
    if month:
        year, mon = month.split("-")
        query = query.where(PayrollStatement.reference_month == date(int(year), int(mon), 1))

    query = query.order_by(
        PayrollStatement.reference_month.desc(),
        PayrollStatement.surname,
        PayrollStatement.first_name,
        PayrollStatement.personnel_number,
    )

    rows = db.execute(query).scalars().all()
    return [StatementSummary.from_row(s) for s in rows]


@router.get("/statements/{statement_id}", response_model=StatementDetail)
def get_statement(
    statement_id: int,
    user: Annotated[AuthenticatedUser, Depends(require_buchhaltung_user)],
    db: Session = Depends(get_db),
) -> StatementDetail:
    s = db.execute(
        select(PayrollStatement)
        .options(selectinload(PayrollStatement.line_items))
        .where(
            PayrollStatement.id == statement_id,
            PayrollStatement.client_id_path == default_client_path(),
        )
    ).scalar_one_or_none()
    if s is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Abrechnung nicht gefunden")

    return StatementDetail(
        summary=StatementSummary.from_row(s),
        snapshot={
            "street": s.street,
            "postal_code": s.postal_code,
            "city": s.city,
            "country_code": s.country_code,
            "date_of_birth": s.date_of_birth.isoformat() if s.date_of_birth else None,
            "sex": s.sex,
            "social_security_number": s.social_security_number,
            "nationality": s.nationality,
            "iban": s.iban,
            "bic": s.bic,
            "date_of_joining": s.date_of_joining.isoformat() if s.date_of_joining else None,
            "date_of_leaving": s.date_of_leaving.isoformat() if s.date_of_leaving else None,
            "job_title": s.job_title,
            "activity_type": s.activity_type,
            "employee_group_code": s.employee_group_code,
            "contribution_group_key": s.contribution_group_key,
            "tax_class": s.tax_class,
            "tax_factor": float(s.tax_factor) if s.tax_factor is not None else None,
            "child_tax_allowances": float(s.child_tax_allowances) if s.child_tax_allowances is not None else None,
            "denomination": s.denomination,
            "spouse_denomination": s.spouse_denomination,
            "tax_identification_number": s.tax_identification_number,
            "finanzamt_number": s.finanzamt_number,
            "health_insurer_name": s.health_insurer_name,
            "health_insurer_number": s.health_insurer_number,
            "weekly_working_hours": float(s.weekly_working_hours) if s.weekly_working_hours is not None else None,
            "annual_vacation_days": float(s.annual_vacation_days) if s.annual_vacation_days is not None else None,
            "vacation_entitlement_current_year": float(s.vacation_entitlement_current_year) if s.vacation_entitlement_current_year is not None else None,
            "vacation_taken_current_year": float(s.vacation_taken_current_year) if s.vacation_taken_current_year is not None else None,
            "vacation_remaining_current_year": float(s.vacation_remaining_current_year) if s.vacation_remaining_current_year is not None else None,
        },
        aggregates={
            "gross_total": _num(s.gross_total),
            "gross_tax": _num(s.gross_tax),
            "tax_deductions": _num(s.tax_deductions),
            "gross_kv_pv": _num(s.gross_kv_pv),
            "gross_rv_av": _num(s.gross_rv_av),
            "ss_deductions": _num(s.ss_deductions),
            "net_income": _num(s.net_income),
            "payout_eur": _num(s.payout_eur),
            "flat_tax_lst": _num(s.flat_tax_lst),
            "flat_tax_kist": _num(s.flat_tax_kist),
            "flat_tax_solz": _num(s.flat_tax_solz),
            "ss_employer_share_monthly": _num(s.ss_employer_share_monthly),
            "allocation_contributions": _num(s.allocation_contributions),
        },
        ytd={
            "gross_total_ytd": _num(s.gross_total_ytd),
            "gross_tax_ytd": _num(s.gross_tax_ytd),
            "gross_ss_ytd": _num(s.gross_ss_ytd),
            "ss_employer_share_ytd": _num(s.ss_employer_share_ytd),
        },
        hours={
            "days_present": _num(s.days_present),
            "hours_present": _num(s.hours_present),
            "hours_paid": _num(s.hours_paid),
            "days_sick": _num(s.days_sick),
            "hours_sick": _num(s.hours_sick),
            "hours_overtime": _num(s.hours_overtime),
        },
        line_items=[LineItemOut.from_row(li, s.reference_month) for li in s.line_items],
        raw_sd=s.raw_sd,
    )


def _num(value) -> float | None:
    return float(value) if value is not None else None
