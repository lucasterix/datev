"""HTTP client for the internal Patti app (https://patti.app).

Patti has no API-key auth — we log in via HTML form + CSRF, hold the
``laravel_session`` cookie + ``XSRF-TOKEN``, and re-issue requests with
the appropriate ``X-XSRF-TOKEN`` header for writes. Same approach as
the FrohZeitRakete backend's PattiClient (which we adapted for the
employee-sync use case in this project).

Patti's data model used here:
- ``users`` are login accounts (employees by another name)
- ``people`` carry the actual master data (name, born_at, address_id,
  communication_id, plus iban/bic — null for caretaker users today,
  but the schema supports it)
- ``addresses`` and ``communications`` are linked from ``people`` and
  separately PUT-able
"""

from __future__ import annotations

import threading
from typing import Any
from urllib.parse import unquote

import requests
from bs4 import BeautifulSoup
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from app.core.logging import get_logger
from app.core.settings import settings

logger = get_logger("patti")


_TRANSIENT_EXCEPTIONS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.ReadTimeout,
    requests.exceptions.ChunkedEncodingError,
)


class PattiError(RuntimeError):
    """Patti-side problem (auth, network, 5xx). Routes/services map this
    to HTTP 502/503 so the caller doesn't see a generic 500."""


class PattiClient:
    """Stateful client — call ``login()`` once, then use the GET/PUT
    helpers. Safe to re-use across requests but not thread-safe; create
    one per worker / wrap in a lock if needed."""

    def __init__(self) -> None:
        self.base_url = settings.patti_base_url.rstrip("/")
        self.timeout = settings.patti_timeout_seconds
        self.session = requests.Session()
        self.session.headers.update({
            "Accept": "application/json, text/plain, */*",
            "X-Requested-With": "XMLHttpRequest",
            "Origin": self.base_url,
            "Referer": f"{self.base_url}/",
            "User-Agent": "Mozilla/5.0",
        })
        self._login_lock = threading.Lock()

    # --- auth ---------------------------------------------------------------

    def _extract_csrf_token(self, html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        token_input = soup.find("input", {"name": "_token"})
        if token_input and token_input.get("value"):
            return str(token_input["value"])
        xsrf_cookie = self.session.cookies.get("XSRF-TOKEN")
        if xsrf_cookie:
            return xsrf_cookie
        raise PattiError("CSRF token not found on Patti login page.")

    @retry(
        retry=retry_if_exception_type(_TRANSIENT_EXCEPTIONS),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=4),
        reraise=True,
    )
    def login(self) -> None:
        with self._login_lock:
            login_url = f"{self.base_url}/login"
            page = self.session.get(login_url, timeout=self.timeout)
            page.raise_for_status()
            token = self._extract_csrf_token(page.text)

            response = self.session.post(
                login_url,
                data={
                    "_token": token,
                    "email": settings.patti_login_email,
                    "password": settings.patti_login_password,
                },
                allow_redirects=False,
                timeout=self.timeout,
            )
            if response.status_code not in (302, 303):
                raise PattiError(
                    f"Patti login failed ({response.status_code}): "
                    f"{response.text[:300]}"
                )
            if "laravel_session" not in self.session.cookies:
                raise PattiError("Patti login: laravel_session cookie missing.")
            # Trigger fresh XSRF cookie for subsequent writes
            self.session.get(f"{self.base_url}/", timeout=self.timeout)
            logger.info("patti_login_success")

    def _xsrf_headers(self) -> dict[str, str]:
        xsrf = self.session.cookies.get("XSRF-TOKEN")
        return {"X-XSRF-TOKEN": unquote(xsrf)} if xsrf else {}

    # --- HTTP primitives ---------------------------------------------------

    def _request(self, method: str, path: str, **kwargs: Any) -> requests.Response:
        kwargs.setdefault("timeout", self.timeout)
        if method.upper() != "GET":
            headers = kwargs.setdefault("headers", {})
            headers.update(self._xsrf_headers())

        url = f"{self.base_url}{path}"
        response = self.session.request(method, url, **kwargs)

        # Auto-relogin once on auth-expired responses, then retry the request.
        if response.status_code in (401, 419):
            logger.info("patti_session_expired", path=path, status=response.status_code)
            self.login()
            if method.upper() != "GET":
                kwargs["headers"].update(self._xsrf_headers())
            response = self.session.request(method, url, **kwargs)

        response.raise_for_status()
        return response

    @retry(
        retry=retry_if_exception_type(_TRANSIENT_EXCEPTIONS),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=4),
        reraise=True,
    )
    def _get(self, path: str, **kwargs: Any) -> requests.Response:
        return self._request("GET", path, **kwargs)

    @retry(
        retry=retry_if_exception_type(_TRANSIENT_EXCEPTIONS),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=0.5, min=0.5, max=4),
        reraise=True,
    )
    def _put(self, path: str, **kwargs: Any) -> requests.Response:
        return self._request("PUT", path, **kwargs)

    # --- users (= Mitarbeiter) ---------------------------------------------

    def list_users(self, page: int = 1, per_page: int = 200) -> dict[str, Any]:
        """GET /api/v1/users — Laravel-paginated list. Caller has to walk
        pages via ``last_page`` or ``next_page_url`` for the full set."""
        return self._get(
            "/api/v1/users",
            params={"page": str(page), "per_page": str(per_page)},
        ).json()

    def list_all_users(self) -> list[dict[str, Any]]:
        """Walk all user pages, returning a flat list."""
        page = 1
        out: list[dict[str, Any]] = []
        while True:
            data = self.list_users(page=page, per_page=200)
            rows = data.get("data") or []
            out.extend(rows)
            last = data.get("last_page") or 1
            if page >= last:
                break
            page += 1
        return out

    def get_user(self, user_id: int) -> dict[str, Any]:
        return self._get(f"/api/v1/users/{user_id}").json()

    # --- people (= Stammdaten) ---------------------------------------------

    def get_person(self, person_id: int) -> dict[str, Any]:
        """GET /api/v1/people/{id} — includes address + communication."""
        return self._get(f"/api/v1/people/{person_id}").json()

    def list_people(self, page: int = 1, per_page: int = 200) -> dict[str, Any]:
        return self._get(
            "/api/v1/people",
            params={"page": str(page), "per_page": str(per_page)},
        ).json()

    def list_all_people(self) -> list[dict[str, Any]]:
        page = 1
        out: list[dict[str, Any]] = []
        while True:
            data = self.list_people(page=page, per_page=200)
            rows = data.get("data") or []
            out.extend(rows)
            last = data.get("last_page") or 1
            if page >= last:
                break
            page += 1
        return out

    def update_person(
        self,
        person_id: int,
        *,
        first_name: str | None = None,
        last_name: str | None = None,
        born_at: str | None = None,
        iban: str | None = None,
        bic: str | None = None,
    ) -> dict[str, Any]:
        """PUT /api/v1/people/{id}.

        Only fields explicitly passed (non-None) are sent. ``born_at``
        in ISO format ``YYYY-MM-DD`` or ``YYYY-MM-DDTHH:MM:SS``.
        """
        payload: dict[str, Any] = {}
        if first_name is not None:
            payload["first_name"] = first_name
        if last_name is not None:
            payload["last_name"] = last_name
        if born_at is not None:
            payload["born_at"] = born_at
        if iban is not None:
            payload["iban"] = iban
        if bic is not None:
            payload["bic"] = bic
        return self._put(f"/api/v1/people/{person_id}", json=payload).json()

    # --- addresses ---------------------------------------------------------

    def get_address(self, address_id: int) -> dict[str, Any]:
        return self._get(f"/api/v1/addresses/{address_id}").json()

    def update_address(
        self,
        address_id: int,
        *,
        address_line: str | None = None,
        zip_code_id: int | None = None,
        city: str | None = None,
        additional_information: str | None = None,
    ) -> dict[str, Any]:
        """PUT /api/v1/addresses/{id}.

        Patti's address.zip_code is a foreign-key reference (``zip_code_id``);
        the actual ``zip_code`` literal lives on the ZipCode resource. So
        for a city change we usually need to look up / create the ZipCode
        first via :meth:`find_or_create_zip_code`."""
        payload: dict[str, Any] = {}
        if address_line is not None:
            payload["address_line"] = address_line
        if zip_code_id is not None:
            payload["zip_code_id"] = zip_code_id
        if city is not None:
            payload["city"] = city
        if additional_information is not None:
            payload["additional_information"] = additional_information
        return self._put(f"/api/v1/addresses/{address_id}", json=payload).json()

    # --- communications ----------------------------------------------------

    def update_communication(
        self,
        communication_id: int,
        *,
        mobile_number: str | None = None,
        phone_number: str | None = None,
        email: str | None = None,
    ) -> dict[str, Any]:
        """PUT /api/v1/communications/{id}.

        Sends only non-None fields; missing fields stay as-is in Patti."""
        payload: dict[str, Any] = {}
        if mobile_number is not None:
            payload["mobile_number"] = mobile_number
        if phone_number is not None:
            payload["phone_number"] = phone_number
        if email is not None:
            payload["email"] = email
        return self._put(
            f"/api/v1/communications/{communication_id}", json=payload
        ).json()

    # --- zip codes (lookup / create) ---------------------------------------

    def find_or_create_zip_code(self, zip_code: str) -> int:
        """Resolve a German postal code to a ZipCode ID, creating it if
        Patti doesn't have it yet. Returns the ZipCode resource id."""
        data = self._get(
            "/api/v1/zip-codes", params={"q": zip_code, "per_page": "5"}
        ).json()
        for row in data.get("data") or []:
            if str(row.get("zip_code")).strip() == zip_code.strip():
                return int(row["id"])
        # Not found -> create. Patti requires county_id; we let Patti
        # auto-resolve (most installs do this automatically).
        created = self._request(
            "POST", "/api/v1/zip-codes", json={"zip_code": zip_code}
        ).json()
        return int(created["id"])
