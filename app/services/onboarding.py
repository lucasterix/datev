"""Employee onboarding: take a freshly-collected master-data form
(either Daniel manually, or the new hire via a one-shot link) and
create the matching DATEV + Patti records.

We do this synchronously so the caller gets a clean success/failure
right away. Bridge-offline / Patti-down cases surface as 502/503 with
a German message — the caller can retry once the prerequisite is back.

Local Employee row is only created after the DATEV employee has been
confirmed: that way ``Employee.personnel_number`` always reflects the
real DATEV id, never a placeholder.
"""

from __future__ import annotations

import uuid
from datetime import date, datetime, timezone
from typing import Any

from pydantic import BaseModel, Field
from sqlalchemy.orm import Session

from app.clients.patti_client import PattiClient, PattiError
from app.core import datev_local_client
from app.core.datev_local_client import (
    BridgeUnavailable,
    LocalDatevError,
    default_client_path,
)
from app.core.logging import get_logger
from app.models.employee import Employee

logger = get_logger("datev.onboarding")


# --- payload schema ------------------------------------------------------


class OnboardingAddress(BaseModel):
    street: str = Field(..., min_length=1, max_length=120)
    house_number: str = Field(..., min_length=1, max_length=20)
    postal_code: str = Field(..., min_length=4, max_length=10)
    city: str = Field(..., min_length=1, max_length=80)
    country: str = Field(default="D", max_length=4)
    address_affix: str | None = Field(default=None, max_length=80)


class OnboardingBank(BaseModel):
    iban: str = Field(..., min_length=15, max_length=40)
    bic: str | None = Field(default=None, max_length=20)


class OnboardingPersonal(BaseModel):
    first_name: str = Field(..., min_length=1, max_length=80)
    surname: str = Field(..., min_length=1, max_length=80)
    date_of_birth: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")
    place_of_birth: str | None = Field(default=None, max_length=80)
    sex: str = Field(default="indeterminate")  # female|male|non_binary|indeterminate
    nationality: str = Field(default="000", max_length=8)
    marital_status: str | None = Field(default=None, max_length=32)
    social_security_number: str | None = Field(default=None, max_length=32)


class OnboardingTax(BaseModel):
    tax_class: int = Field(..., ge=1, le=6)
    child_tax_allowances: float | None = Field(default=None, ge=0)
    denomination: str | None = Field(default=None, max_length=8)
    tax_identification_number: str | None = Field(default=None, max_length=16)


class OnboardingSocialInsurance(BaseModel):
    company_number_of_health_insurer: int | None = Field(default=None)
    health_insurer_name: str | None = Field(default=None, max_length=80)


class OnboardingEmployment(BaseModel):
    date_of_commencement: str = Field(..., pattern=r"^\d{4}-\d{2}-\d{2}$")
    job_title: str | None = Field(default=None, max_length=120)
    weekly_working_hours: float = Field(..., ge=0, le=80)
    contractual_structure: str = Field(default="unbefristet_in_vollzeit")
    employee_type: str = Field(default="101")
    activity_type: str = Field(default="angestellter")


class OnboardingContact(BaseModel):
    mobile_number: str | None = Field(default=None, max_length=40)
    phone_number: str | None = Field(default=None, max_length=40)
    email: str | None = Field(default=None, max_length=255)


class EmployeeOnboardingPayload(BaseModel):
    """The full onboarding form. Required sections cover what DATEV
    needs to register a new employee; contact info goes to Patti."""
    personal: OnboardingPersonal
    address: OnboardingAddress
    bank: OnboardingBank
    tax: OnboardingTax
    social_insurance: OnboardingSocialInsurance
    employment: OnboardingEmployment
    contact: OnboardingContact = OnboardingContact()


# --- service -------------------------------------------------------------


class OnboardingError(Exception):
    """Onboarding step failed in a way the caller should surface."""


def _datev_create_employee(payload: EmployeeOnboardingPayload) -> int:
    """POST a fresh employee to DATEV. Returns the personnel_number
    DATEV assigns. Caller already verified the bridge is reachable."""
    client_id_path = default_client_path()
    body = {
        "first_name": payload.personal.first_name,
        "surname": payload.personal.surname,
        "date_of_commencement_of_employment": payload.employment.date_of_commencement,
    }
    response = datev_local_client.post(
        f"/clients/{client_id_path}/employees",
        json_body=body,
    )
    # POST /employees returns Created-201 with body { id: "<personnel_number>" }
    if not isinstance(response, dict) or not response.get("id"):
        raise OnboardingError(f"DATEV erstellte keinen Mitarbeiter: {response!r}")
    try:
        return int(str(response["id"]))
    except (ValueError, TypeError) as exc:
        raise OnboardingError(f"Personalnummer ungültig: {response['id']!r}") from exc


def _datev_apply_details(pnr: int, payload: EmployeeOnboardingPayload) -> list[str]:
    """Push address / account / personal-data / tax-card / social-insurance
    PUTs after employee creation. Returns warnings (non-fatal failures)."""
    warnings: list[str] = []
    pnr_str = str(pnr).zfill(5)
    client_id_path = default_client_path()

    def _try(label: str, fn) -> None:
        try:
            fn()
        except (LocalDatevError, BridgeUnavailable) as exc:
            warnings.append(f"{label}: {exc.body if hasattr(exc, 'body') else exc}")
            logger.warning("onboard_detail_failed", step=label, error=str(exc))

    address_body = {
        "id": pnr_str,
        "street": payload.address.street,
        "house_number": payload.address.house_number,
        "postal_code": payload.address.postal_code,
        "city": payload.address.city,
        "country": payload.address.country,
    }
    if payload.address.address_affix:
        address_body["address_affix"] = payload.address.address_affix
    _try("Adresse",
         lambda: datev_local_client.put(
             f"/clients/{client_id_path}/employees/{pnr}/address",
             json_body=address_body,
         ))

    if payload.bank.iban:
        _try("Bankverbindung",
             lambda: datev_local_client.put(
                 f"/clients/{client_id_path}/employees/{pnr}/account",
                 json_body={
                     "id": pnr_str,
                     "iban": payload.bank.iban,
                     "bic": payload.bank.bic or "",
                 },
             ))

    personal_body: dict[str, Any] = {
        "id": pnr_str,
        "first_name": payload.personal.first_name,
        "surname": payload.personal.surname,
        "date_of_birth": payload.personal.date_of_birth,
        "sex": payload.personal.sex,
        "nationality": payload.personal.nationality,
    }
    if payload.personal.place_of_birth:
        personal_body["place_of_birth"] = payload.personal.place_of_birth
    if payload.personal.marital_status:
        personal_body["marital_status"] = payload.personal.marital_status
    if payload.personal.social_security_number:
        personal_body["social_security_number"] = payload.personal.social_security_number
    _try("Persönliche Daten",
         lambda: datev_local_client.put(
             f"/clients/{client_id_path}/employees/{pnr}/personal-data",
             json_body=personal_body,
         ))

    tax_body: dict[str, Any] = {
        "id": pnr_str,
        "tax_class": payload.tax.tax_class,
    }
    if payload.tax.child_tax_allowances is not None:
        tax_body["child_tax_allowances"] = payload.tax.child_tax_allowances
    if payload.tax.denomination:
        tax_body["denomination"] = payload.tax.denomination
    _try("Steuerkarte",
         lambda: datev_local_client.put(
             f"/clients/{client_id_path}/employees/{pnr}/tax-card",
             json_body=tax_body,
         ))

    if payload.tax.tax_identification_number:
        _try("Steueridentifikationsnummer",
             lambda: datev_local_client.put(
                 f"/clients/{client_id_path}/employees/{pnr}/taxation",
                 json_body={
                     "id": pnr_str,
                     "tax_identification_number": payload.tax.tax_identification_number,
                 },
             ))

    if payload.social_insurance.company_number_of_health_insurer:
        si_body = {
            "id": pnr_str,
            "company_number_of_health_insurer": payload.social_insurance.company_number_of_health_insurer,
            "contribution_class_health_insurance": "allgemeiner_beitrag",
            "contribution_class_pension_insurance": "voller_beitrag",
            "contribution_class_unemployment_insurance": "voller_beitrag",
            "contribution_class_nursing_insurance": "voller_beitrag",
        }
        _try("Sozialversicherung",
             lambda: datev_local_client.put(
                 f"/clients/{client_id_path}/employees/{pnr}/social-insurance",
                 json_body=si_body,
             ))

    activity_body = {
        "id": pnr_str,
        "weekly_working_hours": payload.employment.weekly_working_hours,
        "contractual_structure": payload.employment.contractual_structure,
        "employee_type": payload.employment.employee_type,
        "activity_type": payload.employment.activity_type,
    }
    if payload.employment.job_title:
        activity_body["job_title"] = payload.employment.job_title
    _try("Tätigkeit",
         lambda: datev_local_client.put(
             f"/clients/{client_id_path}/employees/{pnr}/activity",
             json_body=activity_body,
         ))

    return warnings


def _patti_onboard_person(payload: EmployeeOnboardingPayload) -> int | None:
    """Create address + communication + person in Patti. Returns the
    new Patti person id, or None if Patti onboarding fails (we still
    keep the DATEV employee — Patti link can be made manually later)."""
    try:
        client = PattiClient()
        client.login()

        zip_code_id = client.find_or_create_zip_code(payload.address.postal_code)

        addr_resp = client._request(
            "POST", "/api/v1/addresses",
            json={
                "address_line": f"{payload.address.street} {payload.address.house_number}".strip(),
                "city": payload.address.city,
                "zip_code_id": zip_code_id,
            },
        ).json()
        address_id = int(addr_resp["id"])

        comm_resp = client._request(
            "POST", "/api/v1/communications",
            json={
                "mobile_number": payload.contact.mobile_number,
                "phone_number": payload.contact.phone_number,
                "email": payload.contact.email,
            },
        ).json()
        communication_id = int(comm_resp["id"])

        person_resp = client._request(
            "POST", "/api/v1/people",
            json={
                "first_name": payload.personal.first_name,
                "last_name": payload.personal.surname,
                "born_at": payload.personal.date_of_birth,
                "address_id": address_id,
                "communication_id": communication_id,
                "iban": payload.bank.iban,
                "bic": payload.bank.bic,
            },
        ).json()
        return int(person_resp["id"])
    except (PattiError, Exception) as exc:  # noqa: BLE001
        logger.warning("patti_onboard_failed", error=str(exc))
        return None


def submit(
    db: Session,
    payload: EmployeeOnboardingPayload,
    *,
    source: str,
    submitted_by_email: str | None = None,
) -> dict:
    """Run the full onboarding: DATEV first (must succeed for us to
    have a personnel_number), then Patti, then mirror into the local
    Employee table.

    ``source`` = "manual" or "link" (audit only)."""

    # Sanity: bridge online?
    if not datev_local_client.ping():
        raise OnboardingError(
            "DATEV-Bridge nicht erreichbar — bitte später erneut versuchen."
        )

    # Step 1: create the DATEV employee. Without this we have no pnr.
    try:
        pnr = _datev_create_employee(payload)
    except (LocalDatevError, BridgeUnavailable) as exc:
        body = exc.body if hasattr(exc, "body") else str(exc)
        raise OnboardingError(f"DATEV-Anlage fehlgeschlagen: {body!r}") from exc

    # Step 2: push the detail PUTs. Any failures become warnings —
    # employee is already created.
    warnings = _datev_apply_details(pnr, payload)

    # Step 3: Patti (best-effort)
    patti_person_id = _patti_onboard_person(payload)

    # Step 4: write the local mirror so the new hire shows up in the
    # list immediately (no need to wait for the next pull).
    client_id_path = default_client_path()
    row = Employee(
        client_id_path=client_id_path,
        personnel_number=pnr,
        first_name=payload.personal.first_name,
        surname=payload.personal.surname,
        date_of_birth=date.fromisoformat(payload.personal.date_of_birth),
        date_of_joining=date.fromisoformat(payload.employment.date_of_commencement),
        job_title=payload.employment.job_title,
        weekly_working_hours=payload.employment.weekly_working_hours,
        is_active=True,
        source_system="lug",
        last_synced_at=datetime.now(timezone.utc),
        last_datev_synced_at=datetime.now(timezone.utc),
        last_sync_status="ok",
        patti_person_id=patti_person_id,
        patti_link_state="auto" if patti_person_id else "unmatched",
    )
    db.add(row)
    db.commit()

    logger.info(
        "onboarding_done",
        source=source,
        pnr=pnr,
        patti_person_id=patti_person_id,
        warnings_count=len(warnings),
        submitted_by=submitted_by_email,
    )

    return {
        "ok": True,
        "personnel_number": pnr,
        "patti_person_id": patti_person_id,
        "warnings": warnings,
    }


def new_token(prefill: dict | None = None) -> str:
    """Generate a fresh single-use onboarding token (32 hex chars)."""
    return uuid.uuid4().hex
