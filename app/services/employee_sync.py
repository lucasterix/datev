"""Sync employee masterdata from DATEV hr:exports into local storage.

Pulls `GET /clients/{client-id}/employees/masterdata` (synchronous API,
returns a full snapshot for the requested payroll month) and upserts
each record into `datev_employee`. Keeps the full DATEV payload in
`raw_masterdata` so future features can use any field without needing
a migration.

Also exposes a fixture-load path so the UI is usable while DATEV's
sandbox is returning 500 on hr:exports.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from typing import Any

from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core import datev_client
from app.core.datev_client import DatevApiError, default_client_path
from app.core.logging import get_logger
from app.models.employee import Employee

logger = get_logger("datev.employee_sync")


def _parse_date(raw: Any) -> date | None:
    if not raw:
        return None
    if isinstance(raw, date):
        return raw
    if not isinstance(raw, str):
        return None
    s = raw.strip()
    # DATEV returns both ISO (2001-01-07) and German (15.02.1980) — accept both.
    for fmt in ("%Y-%m-%d", "%d.%m.%Y"):
        try:
            return datetime.strptime(s, fmt).date()
        except ValueError:
            continue
    return None


def _extract_fields(md: dict) -> dict:
    """Pull the small set of hot columns out of a DATEV masterdata record."""
    personal = md.get("personal_data") or {}
    employment = md.get("employment") or {}
    return {
        "personnel_number": md.get("personnel_number") or personal.get("personnel_number"),
        "company_personnel_number": (
            md.get("company_personnel_number") or personal.get("company_personnel_number")
        ),
        "first_name": personal.get("first_name"),
        "surname": personal.get("surname"),
        "date_of_birth": _parse_date(personal.get("date_of_birth")),
        "date_of_joining": _parse_date(employment.get("date_of_joining")),
        "date_of_leaving": _parse_date(employment.get("date_of_leaving")),
        "job_title": employment.get("job_title"),
        "weekly_working_hours": employment.get("weekly_working_hours"),
        "type_of_contract": str(employment.get("type_of_contract"))[:4] if employment.get("type_of_contract") is not None else None,
    }


def _upsert(
    db: Session,
    client_id_path: str,
    md: dict,
    source_system: str | None,
) -> Employee:
    fields = _extract_fields(md)
    pnr = fields["personnel_number"]
    if pnr is None:
        raise ValueError(f"masterdata record missing personnel_number: {md!r}")

    row = db.execute(
        select(Employee).where(
            Employee.client_id_path == client_id_path,
            Employee.personnel_number == pnr,
        )
    ).scalar_one_or_none()

    now = datetime.now(timezone.utc)

    if row is None:
        row = Employee(client_id_path=client_id_path, **fields)
        db.add(row)
    else:
        for key, value in fields.items():
            setattr(row, key, value)

    row.raw_masterdata = md
    row.source_system = source_system
    row.last_synced_at = now
    row.last_sync_status = "ok"
    row.last_sync_error = None
    row.is_active = row.date_of_leaving is None or row.date_of_leaving > now.date()
    return row


def sync_masterdata_from_datev(
    db: Session,
    payroll_accounting_month: str | None = None,
) -> dict:
    """Pull the full employee list from DATEV and upsert into the local DB."""
    client_id_path = default_client_path()
    params = {}
    if payroll_accounting_month:
        params["payroll_accounting_month"] = payroll_accounting_month

    try:
        body = datev_client.get(
            db,
            "hr-exports",
            f"/clients/{client_id_path}/employees/masterdata",
            params=params,
        )
    except DatevApiError as exc:
        logger.error("datev_masterdata_pull_failed", status=exc.status_code, body=exc.body)
        return {
            "ok": False,
            "source": "datev",
            "client_id_path": client_id_path,
            "status": exc.status_code,
            "body": exc.body,
        }

    # Response can be either a single object (one month, one employee)
    # or a list. The DATEV spec says "object for one month" but the
    # collection endpoint typically returns a list.
    records = body if isinstance(body, list) else [body] if isinstance(body, dict) else []
    # hr:exports returns the X-Payroll-System response header, but our
    # httpx wrapper already discards it. If we need to know source_system
    # later, it can live inside the record (currently unavailable, so we
    # store None and let the UI infer from content).
    source_system = None

    updated = 0
    for md in records:
        _upsert(db, client_id_path, md, source_system)
        updated += 1
    db.commit()

    return {
        "ok": True,
        "source": "datev",
        "client_id_path": client_id_path,
        "count": updated,
    }


# Fixture derived from the DATEV OpenAPI example payloads. Lets us
# build the UI before the sandbox tenant is cleared for hr:exports.
_FIXTURE: list[dict] = [
    {
        "personnel_number": 101,
        "company_personnel_number": "M-101",
        "personal_data": {
            "first_name": "Anna",
            "surname": "Beispiel",
            "sex": "W",
            "date_of_birth": "1985-03-14",
            "address": {
                "street": "Musterweg",
                "house_number": "12a",
                "city": "Nürnberg",
                "zip_code": "90402",
                "country": "DE",
            },
        },
        "employment": {
            "date_of_joining": "2019-02-01",
            "job_title": "Pflegekraft",
            "type_of_contract": 1,
            "weekly_working_hours": 35,
        },
        "social_security": {
            "contribution_class_health_insurance": 1,
            "contribution_class_pension_insurance": 1,
            "contribution_class_unemployment_insurance": 1,
            "contribution_class_long_term_care_insurance": 1,
        },
        "taxes": {"tax_class": 1, "denomination": "rk"},
    },
    {
        "personnel_number": 102,
        "company_personnel_number": "M-102",
        "personal_data": {
            "first_name": "Boris",
            "surname": "Muster",
            "sex": "M",
            "date_of_birth": "1978-11-02",
            "address": {
                "street": "Hauptstr.",
                "house_number": "7",
                "city": "Fürth",
                "zip_code": "90762",
                "country": "DE",
            },
        },
        "employment": {
            "date_of_joining": "2015-06-15",
            "job_title": "Leitung Pflege",
            "type_of_contract": 1,
            "weekly_working_hours": 40,
        },
        "social_security": {
            "contribution_class_health_insurance": 1,
            "contribution_class_pension_insurance": 1,
            "contribution_class_unemployment_insurance": 1,
            "contribution_class_long_term_care_insurance": 1,
        },
        "taxes": {"tax_class": 3, "denomination": "ev"},
    },
    {
        "personnel_number": 103,
        "company_personnel_number": "M-103",
        "personal_data": {
            "first_name": "Clara",
            "surname": "Testerin",
            "sex": "W",
            "date_of_birth": "1992-06-21",
            "address": {
                "street": "Gartenweg",
                "house_number": "5",
                "city": "Erlangen",
                "zip_code": "91052",
                "country": "DE",
            },
        },
        "employment": {
            "date_of_joining": "2022-09-01",
            "job_title": "Pflegekraft",
            "type_of_contract": 4,
            "weekly_working_hours": 20,
        },
        "social_security": {
            "contribution_class_health_insurance": 1,
            "contribution_class_pension_insurance": 1,
            "contribution_class_unemployment_insurance": 1,
            "contribution_class_long_term_care_insurance": 1,
        },
        "taxes": {"tax_class": 1, "denomination": "rk"},
    },
]


def sync_masterdata_from_fixture(db: Session) -> dict:
    """Load the built-in fixture into the DB so the UI is usable while the
    DATEV sandbox is returning 500 on hr:exports. Safe to call repeatedly
    — it upserts. Remove once real sync works."""
    client_id_path = default_client_path()
    for md in _FIXTURE:
        _upsert(db, client_id_path, md, source_system="fixture")
    db.commit()
    return {
        "ok": True,
        "source": "fixture",
        "client_id_path": client_id_path,
        "count": len(_FIXTURE),
    }
