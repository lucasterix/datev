"""DATEV REST API HTTP client.

Wraps httpx with the two DATEV requirements that cost us a few hours
to discover:

1. **Every request needs `X-Datev-Client-Id` header** (documented as an
   apiKey securityScheme in every DATEVconnect-online OpenAPI spec —
   the API gateway uses it to route/identify the registered app).
2. **Bearer token + client-id auth are BOTH required.** Sending only
   the Bearer token yields 401 "Invalid client id or secret" from the
   IBM DataPower gateway.

The client also handles auto-refresh of the access token before each
call (access tokens live 15 minutes, so background work would fail
constantly without this).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

import httpx
from sqlalchemy.orm import Session

from app.core.datev_oauth import (
    ACCESS_TOKEN_SAFETY_MARGIN_SECONDS,
    get_current_token,
    refresh_if_needed,
)
from app.core.logging import get_logger
from app.core.settings import settings
from app.models.datev_token import DatevToken

logger = get_logger("datev.client")


# Each DATEVconnect product lives on its own hostname. Map friendly
# names → host so callers don't hard-code URLs.
API_HOSTS = {
    "hr-exchange": "hr-exchange.api.datev.de",
    "hr-exports": "hr-exports.api.datev.de",
    "hr-files": "hr-files.api.datev.de",
    "hr-payrollreports": "hr-payrollreports.api.datev.de",
    "hr-documents": "hr-documents.api.datev.de",
    "eau-api": "eau.api.datev.de",
}


class DatevApiError(Exception):
    """Raised when a DATEV API call returns a non-2xx response.
    Carries the parsed error body so callers can surface the DCO/DPA code."""

    def __init__(self, status_code: int, body: Any, url: str):
        self.status_code = status_code
        self.body = body
        self.url = url
        super().__init__(f"DATEV API {status_code} @ {url}: {body!r}")


def _fresh_token(db: Session) -> DatevToken:
    token = get_current_token(db)
    if token is None:
        raise RuntimeError("Not connected to DATEV — complete OAuth flow first")
    refreshed = refresh_if_needed(db, token)
    # refresh_if_needed either returns the same row (still fresh) or a
    # refreshed one; either way we commit so the new expires_at sticks.
    db.commit()
    return refreshed


def _headers(token: DatevToken, extra: dict | None = None) -> dict:
    base = {
        "Authorization": f"Bearer {token.access_token}",
        "X-Datev-Client-Id": settings.datev_client_id,
        "Accept": "application/json",
    }
    if extra:
        base.update(extra)
    return base


def _build_url(product: str, path: str, version: str = "v1") -> str:
    host = API_HOSTS[product]
    # path starts with "/"
    return f"https://{host}{settings.datev_platform_path}/{version}{path}"


def _parse_body(response: httpx.Response) -> Any:
    ctype = response.headers.get("content-type", "")
    if "application/json" in ctype or "application/problem+json" in ctype:
        try:
            return response.json()
        except Exception:
            return response.text
    return response.text[:2000] if response.text else None


def _request(
    db: Session,
    product: str,
    method: str,
    path: str,
    *,
    version: str = "v1",
    params: dict | None = None,
    json_body: Any = None,
    files: Any = None,
    data: Any = None,
    headers: dict | None = None,
    timeout: float = 30.0,
) -> httpx.Response:
    token = _fresh_token(db)
    url = _build_url(product, path, version=version)

    response = httpx.request(
        method,
        url,
        params=params,
        json=json_body,
        files=files,
        data=data,
        headers=_headers(token, headers),
        timeout=timeout,
    )

    # Log every non-2xx call with the DATEV error code for later triage.
    if response.status_code >= 400:
        body = _parse_body(response)
        logger.warning(
            "datev_api_error",
            product=product,
            method=method,
            path=path,
            status=response.status_code,
            body=body,
        )
    return response


def get(db: Session, product: str, path: str, *, version: str = "v1", params: dict | None = None) -> Any:
    """GET a DATEV REST endpoint. Raises DatevApiError on 4xx/5xx."""
    response = _request(db, product, "GET", path, version=version, params=params)
    if response.status_code >= 400:
        raise DatevApiError(response.status_code, _parse_body(response), str(response.url))
    return _parse_body(response)


def post(
    db: Session,
    product: str,
    path: str,
    *,
    version: str = "v1",
    json_body: Any = None,
    files: Any = None,
    data: Any = None,
    headers: dict | None = None,
) -> Any:
    response = _request(
        db, product, "POST", path,
        version=version, json_body=json_body, files=files, data=data, headers=headers,
    )
    if response.status_code >= 400:
        raise DatevApiError(response.status_code, _parse_body(response), str(response.url))
    return _parse_body(response)


def head_status(db: Session, product: str, path: str, *, version: str = "v1") -> int:
    """Return the HTTP status code only (for permission/availability probes)."""
    response = _request(db, product, "GET", path, version=version)
    return response.status_code


def default_client_path() -> str:
    """The `{consultant}-{client}` fragment used in almost every DATEV API path."""
    if settings.datev_default_client_id_path:
        return settings.datev_default_client_id_path
    if settings.datev_consultant_number and settings.datev_client_number:
        return f"{settings.datev_consultant_number}-{settings.datev_client_number}"
    raise RuntimeError("DATEV consultant/client numbers not configured")
