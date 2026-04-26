"""Employee CRUD for the Buchhaltung UI."""

from datetime import datetime, timezone
from typing import Annotated, Any

from fastapi import APIRouter, Body, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.auth import AuthenticatedUser, require_buchhaltung_user
from app.core.datev_client import default_client_path
from app.db.session import get_db
from app.models.employee import Employee
from app.services.employee_sync import (
    sync_masterdata_from_datev,
    sync_masterdata_from_fixture,
)

router = APIRouter(prefix="/datev/employees", tags=["employees"])


class EmployeeOut(BaseModel):
    id: int
    personnel_number: int
    company_personnel_number: str | None
    first_name: str | None
    surname: str | None
    full_name: str
    date_of_birth: str | None
    date_of_joining: str | None
    date_of_leaving: str | None
    job_title: str | None
    weekly_working_hours: float | None
    type_of_contract: str | None
    source_system: str | None
    is_active: bool
    has_pending_changes: bool
    pending_since: str | None
    last_synced_at: str | None
    last_sync_status: str | None
    patti_link_state: str  # "unmatched" | "auto" | "manual"
    patti_person_id: int | None
    pending_op_count: int

    @classmethod
    def from_row(cls, e: Employee, pending_op_count: int = 0) -> "EmployeeOut":
        return cls(
            id=e.id,
            personnel_number=e.personnel_number,
            company_personnel_number=e.company_personnel_number,
            first_name=e.first_name,
            surname=e.surname,
            full_name=e.full_name,
            date_of_birth=e.date_of_birth.isoformat() if e.date_of_birth else None,
            date_of_joining=e.date_of_joining.isoformat() if e.date_of_joining else None,
            date_of_leaving=e.date_of_leaving.isoformat() if e.date_of_leaving else None,
            job_title=e.job_title,
            weekly_working_hours=e.weekly_working_hours,
            type_of_contract=e.type_of_contract,
            source_system=e.source_system,
            is_active=e.is_active,
            has_pending_changes=e.has_pending_changes,
            pending_since=e.pending_since.isoformat() if e.pending_since else None,
            last_synced_at=e.last_synced_at.isoformat() if e.last_synced_at else None,
            last_sync_status=e.last_sync_status,
            patti_link_state=e.patti_link_state,
            patti_person_id=e.patti_person_id,
            pending_op_count=pending_op_count,
        )


class EmployeeDetail(EmployeeOut):
    raw_masterdata: Any | None
    pending_changes: Any | None


@router.get("", response_model=list[EmployeeOut])
def list_employees(
    user: Annotated[AuthenticatedUser, Depends(require_buchhaltung_user)],
    db: Session = Depends(get_db),
    include_inactive: bool = False,
) -> list[EmployeeOut]:
    """List employees of the current tenant.

    Default: only currently-employed (``is_active=True``). DATEV's
    ``date_of_termination_of_employment`` drives that flag during sync.
    Pass ``?include_inactive=true`` to see everyone (e.g. for searching
    a former employee's record)."""
    from app.models.pending_operation import PendingOperation
    from sqlalchemy import func

    query = select(Employee).where(Employee.client_id_path == default_client_path())
    if not include_inactive:
        query = query.where(Employee.is_active.is_(True))
    query = query.order_by(Employee.surname, Employee.first_name, Employee.personnel_number)
    rows = db.execute(query).scalars().all()

    # Pending-op count per employee (only operations the user can act on)
    op_counts: dict[int, int] = dict(
        db.execute(
            select(PendingOperation.employee_id, func.count(PendingOperation.id))
            .where(PendingOperation.status.in_(("pending", "in_progress", "error")))
            .group_by(PendingOperation.employee_id)
        ).all()
    )

    return [EmployeeOut.from_row(e, op_counts.get(e.id, 0)) for e in rows]


@router.get("/{personnel_number}", response_model=EmployeeDetail)
def get_employee(
    personnel_number: int,
    user: Annotated[AuthenticatedUser, Depends(require_buchhaltung_user)],
    db: Session = Depends(get_db),
) -> EmployeeDetail:
    e = db.execute(
        select(Employee).where(
            Employee.client_id_path == default_client_path(),
            Employee.personnel_number == personnel_number,
        )
    ).scalar_one_or_none()
    if e is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Employee not found")
    out = EmployeeDetail.model_validate(EmployeeOut.from_row(e).model_dump())
    out.raw_masterdata = e.raw_masterdata
    out.pending_changes = e.pending_changes
    return out


class PendingPatch(BaseModel):
    weekly_working_hours: float | None = Field(default=None, ge=0, le=99)
    monthly_gross_salary_eur: float | None = Field(default=None, ge=0)
    note: str | None = Field(default=None, max_length=500)


@router.patch("/{personnel_number}/pending", response_model=EmployeeDetail)
def set_pending_changes(
    personnel_number: int,
    payload: PendingPatch,
    user: Annotated[AuthenticatedUser, Depends(require_buchhaltung_user)],
    db: Session = Depends(get_db),
) -> EmployeeDetail:
    """Store a local edit that will later be pushed to DATEV (via hr:files
    or hr:exchange). Does NOT yet leave our server."""
    e = db.execute(
        select(Employee).where(
            Employee.client_id_path == default_client_path(),
            Employee.personnel_number == personnel_number,
        )
    ).scalar_one_or_none()
    if e is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Employee not found")

    incoming = payload.model_dump(exclude_none=True)
    if not incoming:
        # Empty body clears the pending slate (undo).
        e.pending_changes = None
        e.pending_since = None
    else:
        merged = dict(e.pending_changes or {})
        merged.update(incoming)
        merged["_edited_by_email"] = user.email
        merged["_edited_at"] = datetime.now(timezone.utc).isoformat()
        e.pending_changes = merged
        e.pending_since = datetime.now(timezone.utc)

    db.commit()
    db.refresh(e)

    out = EmployeeDetail.model_validate(EmployeeOut.from_row(e).model_dump())
    out.raw_masterdata = e.raw_masterdata
    out.pending_changes = e.pending_changes
    return out


class SyncRequest(BaseModel):
    payroll_accounting_month: str | None = Field(default=None, pattern=r"^\d{4}-\d{2}$")


@router.post("/sync")
def sync_employees(
    user: Annotated[AuthenticatedUser, Depends(require_buchhaltung_user)],
    payload: SyncRequest = Body(default_factory=SyncRequest),
    db: Session = Depends(get_db),
) -> dict:
    """Pull the employee list from DATEV hr:exports and upsert it locally."""
    return sync_masterdata_from_datev(db, payroll_accounting_month=payload.payroll_accounting_month)


@router.post("/sync-fixture")
def sync_employees_fixture(
    user: Annotated[AuthenticatedUser, Depends(require_buchhaltung_user)],
    db: Session = Depends(get_db),
) -> dict:
    """Load the built-in fixture. Lets us validate the UI while the
    DATEV sandbox hr:exports endpoint is flaky. Temporary."""
    return sync_masterdata_from_fixture(db)
