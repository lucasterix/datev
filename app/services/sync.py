"""Pull-side sync: fetch fresh state from DATEV (via bridge) and Patti
into the local ``Employee`` rows.

This is the read-side of the bidirectional flow. Writes go the other
way through ``operation_apply.py`` + the ``pending_operation`` queue.

Three top-level entrypoints:

- :func:`pull_employees_from_datev` — refresh DATEV side. List, then
  per employee detail to capture date_of_birth (the list endpoint omits
  it). Upserts into ``Employee``, sets ``raw_masterdata``,
  ``last_datev_synced_at``, recomputes ``datev_data_hash``.
- :func:`auto_link_employees` — for employees with ``patti_link_state ==
  "unmatched"``, run the name+DOB heuristic against Patti's people list
  and set ``patti_person_id`` + ``patti_link_state = "auto"`` on
  confident matches (≥0.9).
- :func:`pull_patti_for_linked_employees` — for employees with a
  ``patti_person_id`` set, GET the latest Patti person + nested address +
  communication, save into ``raw_patti``, update ``patti_updated_at``.

All three are safe to call repeatedly. They commit per-batch so a long
sync that aborts halfway still persists what was already done.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.clients.patti_client import PattiClient, PattiError
from app.core import datev_local_client
from app.core.datev_local_client import LocalDatevError
from app.core.datev_oauth import get_current_token  # noqa: F401  (legacy)
from app.core.logging import get_logger
from app.models.employee import Employee
from app.services.employee_match import (
    MatchKey,
    match_datev_to_patti,
)

logger = get_logger("datev.sync")


# --- common helpers -------------------------------------------------------


def _parse_iso(raw: Any) -> date | None:
    if not raw:
        return None
    if isinstance(raw, date):
        return raw
    s = str(raw).strip()
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(s.split("T")[0], fmt).date()
        except ValueError:
            continue
    return None


def _parse_iso_dt(raw: Any) -> datetime | None:
    if not raw:
        return None
    if isinstance(raw, datetime):
        return raw
    s = str(raw).strip()
    # Patti uses RFC3339-ish; tolerant parse
    for fmt in (
        "%Y-%m-%dT%H:%M:%S.%fZ",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
    ):
        try:
            dt = datetime.strptime(s, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt
        except ValueError:
            continue
    return None


def _compute_data_hash(snapshot: dict) -> str:
    """Hash a normalized JSON snapshot. Used to detect DATEV-side changes
    (since DATEV's API does not expose updated_at)."""
    canonical = json.dumps(snapshot, sort_keys=True, default=str, ensure_ascii=False)
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


# --- DATEV pull -----------------------------------------------------------


def pull_employees_from_datev(
    db: Session,
    *,
    fetch_details: bool = True,
    reference_date: str | None = None,
) -> dict:
    """Refresh the local ``Employee`` rows from the DATEV bridge.

    The list endpoint returns a thin record (id, first_name, surname,
    company_personnel_number, dates). To get DOB / address / personal
    data we'd have to call ``GET /clients/{c}/employees/{id}`` per
    employee — opt-in via ``fetch_details`` because that's expensive
    on first sync (86 calls × ~200ms = 17s).

    Returns counts. Sets ``last_sync_status`` per row.
    """
    client_id_path = datev_local_client.default_client_path()

    try:
        listed = datev_local_client.list_employees(reference_date=reference_date)
    except LocalDatevError as exc:
        logger.error("datev_pull_failed", status=exc.status_code, body=exc.body)
        return {"ok": False, "error": str(exc), "status": exc.status_code}

    created = 0
    updated = 0
    detail_failures = 0

    for thin in listed:
        pnr = _parse_personnel_number(thin)
        if pnr is None:
            continue

        full = thin
        if fetch_details:
            try:
                full = datev_local_client.get_employee(pnr, reference_date=reference_date)
            except LocalDatevError as exc:
                detail_failures += 1
                logger.warning("datev_employee_detail_failed",
                               pnr=pnr, status=exc.status_code)
                # Fall back to thin record
                full = thin

        row, was_new = _upsert_employee_from_datev(db, client_id_path, pnr, full)
        if was_new:
            created += 1
        else:
            updated += 1

    db.commit()

    return {
        "ok": True,
        "client_id_path": client_id_path,
        "listed": len(listed),
        "created": created,
        "updated": updated,
        "detail_failures": detail_failures,
    }


def _parse_personnel_number(rec: dict) -> int | None:
    raw = rec.get("id") or rec.get("personnel_number")
    if raw is None:
        return None
    try:
        return int(str(raw))
    except (ValueError, TypeError):
        return None


def _upsert_employee_from_datev(
    db: Session, client_id_path: str, pnr: int, datev: dict
) -> tuple[Employee, bool]:
    row = db.execute(
        select(Employee).where(
            Employee.client_id_path == client_id_path,
            Employee.personnel_number == pnr,
        )
    ).scalar_one_or_none()

    was_new = row is None
    if row is None:
        row = Employee(client_id_path=client_id_path, personnel_number=pnr)
        db.add(row)

    # Hot fields
    personal = datev.get("personal_data") or {}
    employment = datev.get("employment_period") or datev.get("employment") or {}
    activity = datev.get("activity") or {}

    row.first_name = datev.get("first_name") or personal.get("first_name") or row.first_name
    row.surname = datev.get("surname") or personal.get("surname") or row.surname
    row.company_personnel_number = (
        datev.get("company_personnel_number")
        or personal.get("company_personnel_number")
        or row.company_personnel_number
    )

    dob = _parse_iso(personal.get("date_of_birth") or datev.get("date_of_birth"))
    if dob:
        row.date_of_birth = dob

    doj = _parse_iso(
        datev.get("date_of_commencement_of_employment")
        or employment.get("date_of_commencement_of_employment")
    )
    if doj:
        row.date_of_joining = doj

    dol = _parse_iso(
        datev.get("date_of_termination_of_employment")
        or employment.get("date_of_termination_of_employment")
    )
    row.date_of_leaving = dol  # may be None — explicit overwrite

    row.job_title = activity.get("job_title") or row.job_title
    row.weekly_working_hours = activity.get("weekly_working_hours") or row.weekly_working_hours
    row.source_system = "lug"
    row.is_active = (row.date_of_leaving is None) or (row.date_of_leaving > datetime.now(timezone.utc).date())

    row.raw_masterdata = datev
    row.last_synced_at = datetime.now(timezone.utc)
    row.last_datev_synced_at = row.last_synced_at
    row.last_sync_status = "ok"
    row.last_sync_error = None
    row.datev_data_hash = _compute_data_hash(datev)

    return row, was_new


# --- Patti link auto-matcher ---------------------------------------------


def auto_link_employees(db: Session) -> dict:
    """For all unmatched Employees, look up Patti person via name+DOB.

    Returns counts: ``{"linked": N, "still_unmatched": M}``."""
    rows = db.execute(
        select(Employee).where(Employee.patti_link_state == "unmatched")
    ).scalars().all()
    if not rows:
        return {"linked": 0, "still_unmatched": 0}

    client = PattiClient()
    try:
        client.login()
        people = client.list_all_people()
    except PattiError as exc:
        logger.error("patti_pull_failed", error=str(exc))
        return {"ok": False, "error": str(exc)}

    # Adapt our Employee rows to the dict shape match_datev_to_patti expects
    datev_records = [
        {
            "id": r.personnel_number,
            "first_name": r.first_name,
            "surname": r.surname,
            "date_of_birth": r.date_of_birth.isoformat() if r.date_of_birth else None,
        }
        for r in rows
    ]
    matches = match_datev_to_patti(datev_records, people, threshold=0.9)

    linked = 0
    for r in rows:
        match = matches.get(r.personnel_number)
        if match:
            r.patti_person_id = match["id"]
            r.patti_link_state = "auto"
            r.raw_patti = match
            r.patti_updated_at = _parse_iso_dt(match.get("updated_at"))
            r.last_patti_synced_at = datetime.now(timezone.utc)
            linked += 1

    db.commit()
    return {
        "linked": linked,
        "still_unmatched": len(rows) - linked,
    }


# --- Patti detail pull for already-linked employees ---------------------


def pull_patti_for_linked_employees(db: Session) -> dict:
    """For every Employee with ``patti_person_id``, refresh ``raw_patti``
    + ``patti_updated_at`` from Patti's authoritative person endpoint.

    Returns counts of refreshed and failed lookups."""
    rows = db.execute(
        select(Employee).where(Employee.patti_person_id.is_not(None))
    ).scalars().all()
    if not rows:
        return {"refreshed": 0, "failed": 0}

    client = PattiClient()
    try:
        client.login()
    except PattiError as exc:
        logger.error("patti_login_failed", error=str(exc))
        return {"ok": False, "error": str(exc)}

    refreshed = 0
    failed = 0
    for r in rows:
        try:
            person = client.get_person(int(r.patti_person_id))
        except (PattiError, Exception) as exc:  # noqa: BLE001 — log and continue
            failed += 1
            logger.warning("patti_get_person_failed",
                           pnr=r.personnel_number, person_id=r.patti_person_id,
                           error=str(exc))
            continue

        r.raw_patti = person
        r.patti_updated_at = _parse_iso_dt(person.get("updated_at"))
        r.last_patti_synced_at = datetime.now(timezone.utc)
        refreshed += 1

    db.commit()
    return {"refreshed": refreshed, "failed": failed}


# --- one-shot full sync ----------------------------------------------------


def full_sync(db: Session, *, fetch_datev_details: bool = True) -> dict:
    """Convenience entrypoint: DATEV pull → auto-link → Patti pull.

    Used by the manual /sync trigger and the periodic background loop.
    """
    out: dict[str, Any] = {}
    out["datev"] = pull_employees_from_datev(db, fetch_details=fetch_datev_details)
    if out["datev"].get("ok"):
        out["auto_link"] = auto_link_employees(db)
        out["patti"] = pull_patti_for_linked_employees(db)
    return out
