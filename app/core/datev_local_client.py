"""HTTP client for the on-premise DATEVconnect API (lohn-api-v3 /
Payroll-3.1.4) reached via the Tailscale-Bridge on the LuG-PC.

Architecture: caller -> this module -> Tailscale -> bridge -> http://localhost:58454

The bridge (see ``bridge/`` in this repo) injects HTTP Basic Auth and
re-issues the request from 127.0.0.1 so Microsoft http.sys accepts it.
That means the caller (this module) does NOT need to deal with auth —
the bridge URL alone is sufficient.

Endpoints implemented here are the subset we need for the employee-edit
workflow: read employee details, update contact (address), update bank
(account), update Bezüge (gross-payments / hourly-wages). Bewegungsdaten
(calendar/month records) are exposed too for the Monatsabschluss flow.

All DATEVconnect endpoints require a ``reference-date=YYYY-MM-DD`` query
param even on writes; ``_today_iso()`` provides a sensible default.
"""

from __future__ import annotations

from datetime import date
from typing import Any
from urllib.parse import urljoin

import httpx

from app.core.logging import get_logger
from app.core.settings import settings

logger = get_logger("datev.local")


class LocalDatevError(Exception):
    """Non-2xx from the local DATEVconnect API. Holds parsed body so
    callers can surface DATEV's German error message."""

    def __init__(self, status_code: int, body: Any, url: str):
        self.status_code = status_code
        self.body = body
        self.url = url
        super().__init__(f"local-datev {status_code} @ {url}: {body!r}")


class BridgeUnavailable(LocalDatevError):
    """Bridge returned 503 — DATEV LuG closed, stick out, or session
    expired. The pending_operations queue should reschedule the call."""


def _today_iso() -> str:
    return date.today().isoformat()


def default_client_path() -> str:
    """``{consultant}-{client}`` fragment used in every DATEV path."""
    if settings.datev_default_client_id_path:
        return settings.datev_default_client_id_path
    if settings.datev_consultant_number and settings.datev_client_number:
        return f"{settings.datev_consultant_number}-{settings.datev_client_number}"
    raise RuntimeError("DATEV consultant/client numbers not configured")


def _bridge_base() -> str:
    if not settings.datev_local_bridge_url:
        raise RuntimeError(
            "DATEV_LOCAL_BRIDGE_URL not set — local DATEV access disabled"
        )
    return settings.datev_local_bridge_url.rstrip("/")


def _build_url(path: str) -> str:
    """All paths in Payroll-3.1.4 sit under ``/datev/api/hr/v3``."""
    return urljoin(_bridge_base() + "/", "datev/api/hr/v3" + path)


def _parse_body(response: httpx.Response) -> Any:
    ctype = response.headers.get("content-type", "")
    if "application/json" in ctype or "application/problem+json" in ctype:
        try:
            return response.json()
        except Exception:
            return response.text
    return response.text[:2000] if response.text else None


def _request(
    method: str,
    path: str,
    *,
    params: dict | None = None,
    json_body: Any = None,
    headers: dict | None = None,
    timeout: float = 30.0,
) -> httpx.Response:
    """Bare HTTP request. Adds reference-date if missing."""
    url = _build_url(path)
    final_params = dict(params or {})
    final_params.setdefault("reference-date", _today_iso())

    response = httpx.request(
        method, url,
        params=final_params,
        json=json_body,
        headers=headers,
        timeout=timeout,
    )

    if response.status_code >= 400:
        body = _parse_body(response)
        logger.warning(
            "local_datev_error",
            method=method, path=path,
            status=response.status_code, body=body,
        )
    return response


def _ok_or_raise(response: httpx.Response) -> Any:
    if response.status_code == 503:
        raise BridgeUnavailable(503, _parse_body(response), str(response.url))
    if response.status_code >= 400:
        raise LocalDatevError(response.status_code, _parse_body(response), str(response.url))
    return _parse_body(response)


# --- generic verbs ---------------------------------------------------------


def get(path: str, params: dict | None = None) -> Any:
    return _ok_or_raise(_request("GET", path, params=params))


def put(path: str, json_body: Any, params: dict | None = None) -> Any:
    headers = {"Content-Type": "application/json;charset=utf-8"}
    response = _request("PUT", path, params=params, json_body=json_body, headers=headers)
    return _ok_or_raise(response)


def post(path: str, json_body: Any, params: dict | None = None) -> Any:
    headers = {"Content-Type": "application/json;charset=utf-8"}
    response = _request("POST", path, params=params, json_body=json_body, headers=headers)
    return _ok_or_raise(response)


def delete(path: str, params: dict | None = None) -> Any:
    return _ok_or_raise(_request("DELETE", path, params=params))


# --- client + employee discovery ------------------------------------------


def list_clients() -> list[dict]:
    return get("/clients") or []


def list_employees(*, reference_date: str | None = None) -> list[dict]:
    """All employees at the configured tenant on a given date.

    Defaults to today; pass an ISO date to query a different point in
    time (DATEV returns historical state for the given date)."""
    return get(
        f"/clients/{default_client_path()}/employees",
        params={"reference-date": reference_date} if reference_date else None,
    ) or []


def get_employee(personnel_number: int | str, *, reference_date: str | None = None) -> dict:
    return get(
        f"/clients/{default_client_path()}/employees/{personnel_number}",
        params={"reference-date": reference_date} if reference_date else None,
    )


def list_employment_periods(*, reference_date: str | None = None) -> list[dict]:
    """All employment periods for the tenant. Cheap one-call source of
    truth for who is ausgeschieden — entries with
    ``date_of_termination_of_employment`` <= today have left."""
    return get(
        f"/clients/{default_client_path()}/employment-periods",
        params={"reference-date": reference_date} if reference_date else None,
    ) or []


# --- contact (Adresse) ----------------------------------------------------


def get_address(personnel_number: int | str) -> dict:
    return get(f"/clients/{default_client_path()}/employees/{personnel_number}/address")


def update_address(personnel_number: int | str, address: dict) -> None:
    """Address fields per Payroll-3.1.4 schema:

    ``{ id, street, house_number, city, postal_code, country, address_affix }``

    PUT returns 204; we return None on success.
    """
    put(
        f"/clients/{default_client_path()}/employees/{personnel_number}/address",
        json_body=address,
    )


def get_personal_data(personnel_number: int | str) -> dict:
    return get(f"/clients/{default_client_path()}/employees/{personnel_number}/personal-data")


def update_personal_data(personnel_number: int | str, data: dict) -> None:
    put(
        f"/clients/{default_client_path()}/employees/{personnel_number}/personal-data",
        json_body=data,
    )


# --- bank (Bankverbindung) ------------------------------------------------


def get_account(personnel_number: int | str) -> dict:
    return get(f"/clients/{default_client_path()}/employees/{personnel_number}/account")


def update_account(personnel_number: int | str, account: dict) -> None:
    """Account fields: ``{ id, iban, bic, differing_account_holder }``."""
    put(
        f"/clients/{default_client_path()}/employees/{personnel_number}/account",
        json_body=account,
    )


# --- Bezüge variant (a) / (c) — feste Brutto-Bezüge -----------------------


def list_gross_payments(personnel_number: int | str) -> list[dict]:
    return get(
        f"/clients/{default_client_path()}/employees/{personnel_number}/gross-payments"
    ) or []


def get_gross_payment(personnel_number: int | str, gross_payment_id: int | str) -> dict:
    return get(
        f"/clients/{default_client_path()}/employees/{personnel_number}"
        f"/gross-payments/{gross_payment_id}"
    )


def create_gross_payment(personnel_number: int | str, payment: dict) -> dict:
    return post(
        f"/clients/{default_client_path()}/employees/{personnel_number}/gross-payments",
        json_body=payment,
    )


def update_gross_payment(
    personnel_number: int | str, gross_payment_id: int | str, payment: dict
) -> None:
    put(
        f"/clients/{default_client_path()}/employees/{personnel_number}"
        f"/gross-payments/{gross_payment_id}",
        json_body=payment,
    )


# --- Bezüge variant (b) — Stundenlöhne ------------------------------------


def list_hourly_wages(personnel_number: int | str) -> list[dict]:
    return get(
        f"/clients/{default_client_path()}/employees/{personnel_number}/hourly-wages"
    ) or []


def update_hourly_wage(
    personnel_number: int | str, hourly_wage_id: int | str, wage: dict
) -> None:
    """Hourly wage 1-5: ``{ id, personnel_number, amount }``."""
    put(
        f"/clients/{default_client_path()}/employees/{personnel_number}"
        f"/hourly-wages/{hourly_wage_id}",
        json_body=wage,
    )


# --- Bewegungsdaten (Kalender + Monat) -----------------------------------


def list_calendar_records(
    personnel_number: int | str, *, reference_date: str
) -> list[dict]:
    """Calendar records for the month containing ``reference_date``.

    DATEV semantics: only records whose Zuordnungsmonat matches the
    month of the parameter are returned."""
    return get(
        f"/clients/{default_client_path()}/employees/{personnel_number}/calendar-records",
        params={"reference-date": reference_date},
    ) or []


def create_calendar_record(personnel_number: int | str, record: dict) -> dict:
    return post(
        f"/clients/{default_client_path()}/employees/{personnel_number}/calendar-records",
        json_body=record,
    )


def update_calendar_record(calendar_record_id: int | str, record: dict) -> None:
    put(
        f"/clients/{default_client_path()}/calendar-records/{calendar_record_id}",
        json_body=record,
    )


def delete_calendar_record(calendar_record_id: int | str) -> None:
    delete(f"/clients/{default_client_path()}/calendar-records/{calendar_record_id}")


def list_month_records(
    personnel_number: int | str, *, reference_date: str
) -> list[dict]:
    return get(
        f"/clients/{default_client_path()}/employees/{personnel_number}/month-records",
        params={"reference-date": reference_date},
    ) or []


def create_month_record(personnel_number: int | str, record: dict) -> dict:
    return post(
        f"/clients/{default_client_path()}/employees/{personnel_number}/month-records",
        json_body=record,
    )


def update_month_record(month_record_id: int | str, record: dict) -> None:
    put(
        f"/clients/{default_client_path()}/month-records/{month_record_id}",
        json_body=record,
    )


# --- health probe ---------------------------------------------------------


def ping(timeout: float = 3.0) -> bool:
    """Returns True if the bridge answers and DATEVconnect is reachable.

    Short timeout (3s default) so a /health endpoint call from the UI
    doesn't block on a 30s default httpx timeout when the LuG-PC is off.
    Used by the sync worker to decide whether to drain the
    pending_operations queue right now or wait.
    """
    if not settings.datev_local_bridge_url:
        return False
    try:
        # Use a lightweight bare-HTTP call so we don't trigger the
        # default 30s timeout in the regular request wrapper.
        url = _build_url("/clients")
        response = httpx.get(
            url,
            params={"reference-date": _today_iso()},
            timeout=timeout,
        )
        return response.status_code == 200
    except (httpx.HTTPError, Exception):  # noqa: BLE001
        return False
