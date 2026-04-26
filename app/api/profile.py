"""Employee profile + edit endpoints for the Buchhaltung UI.

Reads come from the local cache (``Employee.raw_masterdata`` +
``Employee.raw_patti``), populated by the sync service. Writes are
queued as ``pending_operation`` rows and applied asynchronously by
the worker — Daniel never has to wait for the bridge round-trip,
and edits survive bridge outages.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Annotated, Any

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.auth import AuthenticatedUser, require_buchhaltung_user
from app.core.datev_local_client import default_client_path
from app.core.logging import get_logger
from app.db.session import get_db
from app.models.employee import Employee
from app.models.pending_operation import PendingOperation
from app.services import operation_apply

logger = get_logger("datev.profile")

router = APIRouter(prefix="/datev/employees", tags=["employee-profile"])


# --- response schemas ----------------------------------------------------


class PendingOpOut(BaseModel):
    id: int
    op: str
    payload: Any
    status: str
    attempts: int
    last_error: str | None
    created_at: str
    last_attempt_at: str | None


class CalendarRecordOut(BaseModel):
    id: str | None = None
    date_of_emergence: str | None = None
    reason_for_absence_id: str | None = None
    salary_type_id: int | None = None
    hours: float | None = None
    days: float | None = None
    accounting_month: str | None = None


class EmployeeProfile(BaseModel):
    """Merged profile: DATEV stamp + Patti link + pending queue.

    The frontend just renders what's there; sync state is per-section
    so the user knows whether each field is in flight or settled.
    """

    # Identity
    personnel_number: int
    full_name: str
    is_active: bool
    date_of_birth: str | None
    date_of_joining: str | None
    date_of_leaving: str | None

    # Contact (DATEV-side)
    contact: dict
    # Bank (DATEV-only)
    bank: dict
    # Bezüge (DATEV-only)
    bezuege: dict

    # Patti link
    patti: dict

    # Movement data — calendar records of the current + previous month,
    # filtered to entries that look like absences (have a reason_for_absence_id).
    absences: list[CalendarRecordOut]

    # Sync state
    last_datev_synced_at: str | None
    last_patti_synced_at: str | None

    # Outstanding writes for this employee
    pending_operations: list[PendingOpOut]


def _section_contact_from_datev(raw: dict | None) -> dict:
    """Pull contact fields out of the DATEV masterdata blob."""
    if not raw:
        return {}
    personal = raw.get("personal_data") or {}
    addr = raw.get("address") or {}
    return {
        "first_name": raw.get("first_name") or personal.get("first_name"),
        "surname": raw.get("surname") or personal.get("surname"),
        "street": addr.get("street"),
        "house_number": addr.get("house_number"),
        "postal_code": addr.get("postal_code"),
        "city": addr.get("city"),
        "country": addr.get("country") or "D",
        "address_affix": addr.get("address_affix"),
    }


def _section_bank_from_datev(raw: dict | None) -> dict:
    if not raw:
        return {}
    acc = raw.get("account") or {}
    return {
        "iban": acc.get("iban"),
        "bic": acc.get("bic"),
        "differing_account_holder": acc.get("differing_account_holder"),
    }


def _section_bezuege_from_datev(raw: dict | None) -> dict:
    """Bezüge: monthly Festbezüge (gross-payments) + Stundenlöhne."""
    if not raw:
        return {}
    return {
        "gross_payments": raw.get("gross_payments") or [],
        "hourly_wages": raw.get("hourly_wages") or [],
    }


def _section_patti(raw: dict | None) -> dict:
    if not raw:
        return {"linked": False}
    addr = (raw.get("address") or {})
    comm = (raw.get("communication") or {})
    return {
        "linked": True,
        "person_id": raw.get("id"),
        "first_name": raw.get("first_name"),
        "last_name": raw.get("last_name"),
        "born_at": raw.get("born_at"),
        "address": {
            "id": addr.get("id"),
            "address_line": addr.get("address_line"),
            "city": addr.get("city"),
            "zip_code": (
                addr.get("zip_code", {}).get("zip_code")
                if isinstance(addr.get("zip_code"), dict)
                else addr.get("zip_code")
            ),
        },
        "communication": {
            "id": comm.get("id"),
            "mobile_number": comm.get("mobile_number"),
            "phone_number": comm.get("phone_number"),
            "email": comm.get("email"),
        },
        "iban": raw.get("iban"),
        "bic": raw.get("bic"),
        "updated_at": raw.get("updated_at"),
    }


def _employee_or_404(db: Session, personnel_number: int) -> Employee:
    e = db.execute(
        select(Employee).where(
            Employee.client_id_path == default_client_path(),
            Employee.personnel_number == personnel_number,
        )
    ).scalar_one_or_none()
    if e is None:
        raise HTTPException(status_code=404, detail="Employee not found")
    return e


def _pending_for(db: Session, employee_id: int) -> list[PendingOpOut]:
    rows = db.execute(
        select(PendingOperation)
        .where(
            PendingOperation.employee_id == employee_id,
            PendingOperation.status.in_(("pending", "in_progress", "error")),
        )
        .order_by(PendingOperation.created_at.desc())
    ).scalars().all()
    return [
        PendingOpOut(
            id=r.id,
            op=r.op,
            payload=r.payload,
            status=r.status,
            attempts=r.attempts,
            last_error=r.last_error,
            created_at=r.created_at.isoformat() if r.created_at else "",
            last_attempt_at=r.last_attempt_at.isoformat() if r.last_attempt_at else None,
        )
        for r in rows
    ]


# --- profile read ---------------------------------------------------------


def _ensure_datev_details_loaded(db: Session, e: Employee) -> None:
    """Lazy-load DATEV detail on first profile view.

    The bulk sync only fetches the thin employee list to stay fast
    (~5s instead of 3min for 86 employees). Detail data — address,
    account, gross-payments etc. — gets fetched when the user opens
    a profile and is then cached in raw_masterdata."""
    raw = e.raw_masterdata or {}
    has_detail = any(k in raw for k in ("address", "account", "personal_data"))
    if has_detail:
        return  # already cached
    try:
        from app.core import datev_local_client
        from datetime import datetime, timezone
        detail = datev_local_client.get_employee(e.personnel_number)
        # Try to also fetch address + account in parallel calls — these
        # come from separate DATEV endpoints. Tolerate failures.
        try:
            detail["address"] = datev_local_client.get_address(e.personnel_number)
        except Exception:
            pass
        try:
            detail["account"] = datev_local_client.get_account(e.personnel_number)
        except Exception:
            pass
        try:
            detail["gross_payments"] = datev_local_client.list_gross_payments(e.personnel_number)
        except Exception:
            pass
        try:
            detail["hourly_wages"] = datev_local_client.list_hourly_wages(e.personnel_number)
        except Exception:
            pass
        e.raw_masterdata = detail
        e.last_datev_synced_at = datetime.now(timezone.utc)
        db.commit()
    except Exception as exc:  # noqa: BLE001 — never block the UI
        logger.warning("lazy_detail_fetch_failed", pnr=e.personnel_number, error=str(exc))


def _fetch_recent_absences(personnel_number: int, months_back: int = 2) -> list[CalendarRecordOut]:
    """Pull the last N months of calendar-records for this employee
    and keep only entries that look like absences (have a reason).

    DATEV's calendar-records endpoint returns entries whose Zuordnungsmonat
    matches the reference-date month, so we have to call it per month.
    Two months covers most active sick-notes; older periods are
    historical and live in the LuG ASCII archive."""
    from app.core import datev_local_client
    from datetime import date as _date
    from dateutil.relativedelta import relativedelta  # type: ignore[import-not-found]

    today = _date.today()
    out: list[CalendarRecordOut] = []
    for offset in range(months_back + 1):
        target = today - relativedelta(months=offset)
        first_of_month = target.replace(day=1).isoformat()
        try:
            records = datev_local_client.list_calendar_records(
                personnel_number, reference_date=first_of_month
            )
        except Exception:  # noqa: BLE001 — bridge offline / DATEV 5xx
            continue
        for r in records:
            if not (r.get("reason_for_absence_id") or "").strip():
                continue
            out.append(
                CalendarRecordOut(
                    id=str(r.get("id")) if r.get("id") is not None else None,
                    date_of_emergence=r.get("date_of_emergence"),
                    reason_for_absence_id=r.get("reason_for_absence_id"),
                    salary_type_id=r.get("salary_type_id"),
                    hours=r.get("hours"),
                    days=r.get("days"),
                    accounting_month=r.get("accounting_month"),
                )
            )
    out.sort(key=lambda x: x.date_of_emergence or "", reverse=True)
    return out


@router.get("/{personnel_number}/profile", response_model=EmployeeProfile)
def get_profile(
    personnel_number: int,
    user: Annotated[AuthenticatedUser, Depends(require_buchhaltung_user)],
    db: Session = Depends(get_db),
) -> EmployeeProfile:
    e = _employee_or_404(db, personnel_number)
    _ensure_datev_details_loaded(db, e)
    absences = _fetch_recent_absences(e.personnel_number)
    return EmployeeProfile(
        personnel_number=e.personnel_number,
        full_name=e.full_name,
        is_active=e.is_active,
        date_of_birth=e.date_of_birth.isoformat() if e.date_of_birth else None,
        date_of_joining=e.date_of_joining.isoformat() if e.date_of_joining else None,
        date_of_leaving=e.date_of_leaving.isoformat() if e.date_of_leaving else None,
        contact=_section_contact_from_datev(e.raw_masterdata),
        bank=_section_bank_from_datev(e.raw_masterdata),
        bezuege=_section_bezuege_from_datev(e.raw_masterdata),
        patti=_section_patti(e.raw_patti),
        absences=absences,
        last_datev_synced_at=e.last_datev_synced_at.isoformat() if e.last_datev_synced_at else None,
        last_patti_synced_at=e.last_patti_synced_at.isoformat() if e.last_patti_synced_at else None,
        pending_operations=_pending_for(db, e.id),
    )


# --- contact edit ---------------------------------------------------------


class ContactPatch(BaseModel):
    """Only fields the user actually changed are sent. Defaults None."""
    street: str | None = Field(default=None, max_length=120)
    house_number: str | None = Field(default=None, max_length=20)
    postal_code: str | None = Field(default=None, max_length=10)
    city: str | None = Field(default=None, max_length=80)
    country: str | None = Field(default=None, max_length=4)
    address_affix: str | None = Field(default=None, max_length=80)
    mobile_number: str | None = Field(default=None, max_length=40)
    phone_number: str | None = Field(default=None, max_length=40)
    email: str | None = Field(default=None, max_length=255)


@router.patch("/{personnel_number}/contact")
def edit_contact(
    personnel_number: int,
    patch: ContactPatch,
    user: Annotated[AuthenticatedUser, Depends(require_buchhaltung_user)],
    db: Session = Depends(get_db),
) -> dict:
    """Queue contact edits for both DATEV and Patti.

    Address fields → DATEV (always) + Patti (if linked + has address).
    Phone/email   → Patti only (DATEV's local API doesn't carry these).
    """
    e = _employee_or_404(db, personnel_number)
    delta = patch.model_dump(exclude_none=True)
    if not delta:
        raise HTTPException(status_code=400, detail="No fields to update")

    address_fields = {"street", "house_number", "postal_code", "city",
                      "country", "address_affix"}
    addr_delta = {k: v for k, v in delta.items() if k in address_fields}
    comm_delta = {k: v for k, v in delta.items()
                  if k in ("mobile_number", "phone_number", "email")}

    queued = []

    if addr_delta:
        # DATEV expects the full Address object including ``id`` (the
        # personnel number). Merge with current raw_masterdata.address
        # so we don't blank out fields the user didn't touch.
        current = (e.raw_masterdata or {}).get("address") or {}
        merged = {**current, **addr_delta, "id": str(personnel_number).zfill(5)}
        merged.setdefault("country", "D")
        op = operation_apply.enqueue(
            db,
            employee_id=e.id,
            op="datev.update_address",
            payload={"personnel_number": personnel_number, "address": merged},
            requested_by_email=user.email,
        )
        queued.append(op.id)

        # Patti side: only if linked + we have an address_id
        patti_addr_id = ((e.raw_patti or {}).get("address") or {}).get("id")
        if e.patti_person_id and patti_addr_id:
            patti_fields: dict[str, Any] = {}
            line = " ".join(
                v for v in (addr_delta.get("street"), addr_delta.get("house_number"))
                if v
            )
            if line:
                # Only overwrite address_line if street or house_number changed
                cur = (e.raw_patti or {}).get("address") or {}
                cur_street = (cur.get("address_line") or "").rsplit(" ", 1)
                full_street = addr_delta.get("street") or (cur_street[0] if len(cur_street) >= 1 else "")
                full_hnr = addr_delta.get("house_number") or (cur_street[1] if len(cur_street) >= 2 else "")
                patti_fields["address_line"] = (full_street + " " + full_hnr).strip()
            if "city" in addr_delta:
                patti_fields["city"] = addr_delta["city"]
            # postal code change: would need find_or_create_zip_code which
            # the worker has to do at apply time, so just pass the desired
            # zip and let the handler do the lookup.
            if "postal_code" in addr_delta:
                patti_fields["zip_code"] = addr_delta["postal_code"]

            if patti_fields:
                op = operation_apply.enqueue(
                    db,
                    employee_id=e.id,
                    op="patti.update_address",
                    payload={
                        "address_id": patti_addr_id,
                        "fields": patti_fields,
                    },
                    requested_by_email=user.email,
                )
                queued.append(op.id)

    if comm_delta:
        # Patti is the only place phone/email live for now
        patti_comm_id = ((e.raw_patti or {}).get("communication") or {}).get("id")
        if e.patti_person_id and patti_comm_id:
            op = operation_apply.enqueue(
                db,
                employee_id=e.id,
                op="patti.update_communication",
                payload={
                    "communication_id": patti_comm_id,
                    "fields": comm_delta,
                },
                requested_by_email=user.email,
            )
            queued.append(op.id)
        else:
            raise HTTPException(
                status_code=409,
                detail="Phone/E-Mail-Änderung erfordert Patti-Verknüpfung",
            )

    db.commit()
    return {"queued_operation_ids": queued}


# --- bank edit ------------------------------------------------------------


class BankPatch(BaseModel):
    iban: str | None = Field(default=None, max_length=40)
    bic: str | None = Field(default=None, max_length=20)
    differing_account_holder: str | None = Field(default=None, max_length=120)


@router.patch("/{personnel_number}/bank")
def edit_bank(
    personnel_number: int,
    patch: BankPatch,
    user: Annotated[AuthenticatedUser, Depends(require_buchhaltung_user)],
    db: Session = Depends(get_db),
) -> dict:
    """Queue an account update for DATEV. (Patti also has iban/bic on
    person, but those are usually null at Fröhlich Dienste; we don't
    push there for now.)"""
    e = _employee_or_404(db, personnel_number)
    delta = patch.model_dump(exclude_none=True)
    if not delta:
        raise HTTPException(status_code=400, detail="No fields to update")

    current = (e.raw_masterdata or {}).get("account") or {}
    merged = {**current, **delta, "id": str(personnel_number).zfill(5)}

    op = operation_apply.enqueue(
        db,
        employee_id=e.id,
        op="datev.update_account",
        payload={"personnel_number": personnel_number, "account": merged},
        requested_by_email=user.email,
    )
    db.commit()
    return {"queued_operation_ids": [op.id]}


# --- bezuege (gross-payment edit / hourly-wage edit) ----------------------


class GrossPaymentPatch(BaseModel):
    gross_payment_id: int | None = None  # null → create new
    salary_type_id: int
    amount: float
    reference_date: str = Field(pattern=r"^\d{4}-\d{2}$")
    reduction: str | None = None
    payment_interval: str | None = None
    cost_center_allocation_id: str | None = None
    cost_unit_allocation_id: str | None = None


@router.put("/{personnel_number}/bezuege/gross-payment")
def edit_gross_payment(
    personnel_number: int,
    patch: GrossPaymentPatch,
    user: Annotated[AuthenticatedUser, Depends(require_buchhaltung_user)],
    db: Session = Depends(get_db),
) -> dict:
    """Update or create a recurring monthly gross payment."""
    e = _employee_or_404(db, personnel_number)
    body = patch.model_dump(exclude_none=True)
    body["personnel_number"] = str(personnel_number).zfill(5)

    if patch.gross_payment_id is None:
        op_name = "datev.create_gross_payment"
        payload = {"personnel_number": personnel_number, "gross_payment": body}
    else:
        op_name = "datev.update_gross_payment"
        body["id"] = str(patch.gross_payment_id)
        payload = {
            "personnel_number": personnel_number,
            "gross_payment_id": patch.gross_payment_id,
            "gross_payment": body,
        }

    op = operation_apply.enqueue(
        db, employee_id=e.id, op=op_name, payload=payload,
        requested_by_email=user.email,
    )
    db.commit()
    return {"queued_operation_ids": [op.id]}


class HourlyWagePatch(BaseModel):
    hourly_wage_id: int = Field(..., ge=1, le=5)  # 1..5 only per LuG
    amount: float = Field(..., ge=0)


@router.put("/{personnel_number}/bezuege/hourly-wage")
def edit_hourly_wage(
    personnel_number: int,
    patch: HourlyWagePatch,
    user: Annotated[AuthenticatedUser, Depends(require_buchhaltung_user)],
    db: Session = Depends(get_db),
) -> dict:
    e = _employee_or_404(db, personnel_number)
    body = {
        "id": str(patch.hourly_wage_id),
        "personnel_number": str(personnel_number).zfill(5),
        "amount": patch.amount,
    }
    op = operation_apply.enqueue(
        db,
        employee_id=e.id,
        op="datev.update_hourly_wage",
        payload={
            "personnel_number": personnel_number,
            "hourly_wage_id": patch.hourly_wage_id,
            "hourly_wage": body,
        },
        requested_by_email=user.email,
    )
    db.commit()
    return {"queued_operation_ids": [op.id]}


# --- AU / Krankmeldung ---------------------------------------------------


class SicknessNoticeBody(BaseModel):
    """Payload to record a sick-leave period.

    Each calendar day in [start_date, end_date] is enqueued as its own
    DATEV calendar-record (one row per day is the LuG convention).
    ``salary_type_id`` defaults to 1650 (Lohnfortzahlung Std) — Daniel
    can override per-employee if the Mandant uses a different code.
    ``reason_for_absence_id`` defaults to "K" (Krank).
    """

    start_date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")
    end_date: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")
    hours_per_day: float = Field(default=8.0, ge=0, le=24)
    days_per_day: float = Field(default=1.0, ge=0, le=1)
    salary_type_id: int = Field(default=1650)
    reason_for_absence_id: str = Field(default="K", max_length=8)
    note: str | None = Field(default=None, max_length=500)


@router.post("/{personnel_number}/absences")
def create_absence(
    personnel_number: int,
    body: SicknessNoticeBody,
    user: Annotated[AuthenticatedUser, Depends(require_buchhaltung_user)],
    db: Session = Depends(get_db),
) -> dict:
    """Queue calendar-record creates for each day of an absence period."""
    from datetime import date, timedelta
    e = _employee_or_404(db, personnel_number)

    try:
        start = date.fromisoformat(body.start_date)
        end = date.fromisoformat(body.end_date)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Ungültiges Datum: {exc}") from exc

    if end < start:
        raise HTTPException(status_code=400, detail="end_date liegt vor start_date")
    span_days = (end - start).days + 1
    if span_days > 92:
        raise HTTPException(status_code=400, detail="Zeitraum > 92 Tage; bitte einzeln erfassen")

    queued: list[int] = []
    for offset in range(span_days):
        day = start + timedelta(days=offset)
        record = {
            "personnel_number": str(personnel_number).zfill(5),
            "date_of_emergence": day.isoformat(),
            "reason_for_absence_id": body.reason_for_absence_id,
            "salary_type_id": body.salary_type_id,
            "hours": body.hours_per_day,
            "days": body.days_per_day,
        }
        op = operation_apply.enqueue(
            db,
            employee_id=e.id,
            op="datev.create_calendar_record",
            payload={"personnel_number": personnel_number, "record": record},
            requested_by_email=user.email,
        )
        queued.append(op.id)

    db.commit()
    return {
        "queued_operation_ids": queued,
        "days": span_days,
        "start_date": body.start_date,
        "end_date": body.end_date,
    }


# --- manual Patti link --------------------------------------------------


class PattiLinkBody(BaseModel):
    patti_person_id: int


@router.post("/{personnel_number}/link-patti")
def link_patti(
    personnel_number: int,
    body: PattiLinkBody,
    user: Annotated[AuthenticatedUser, Depends(require_buchhaltung_user)],
    db: Session = Depends(get_db),
) -> dict:
    """Manually associate this Employee with a Patti person."""
    e = _employee_or_404(db, personnel_number)
    e.patti_person_id = body.patti_person_id
    e.patti_link_state = "manual"
    e.last_patti_synced_at = None  # force refresh on next pull
    db.commit()
    return {"ok": True, "patti_person_id": body.patti_person_id}


@router.delete("/{personnel_number}/link-patti")
def unlink_patti(
    personnel_number: int,
    user: Annotated[AuthenticatedUser, Depends(require_buchhaltung_user)],
    db: Session = Depends(get_db),
) -> dict:
    e = _employee_or_404(db, personnel_number)
    e.patti_person_id = None
    e.patti_user_id = None
    e.patti_link_state = "unmatched"
    e.raw_patti = None
    db.commit()
    return {"ok": True}
