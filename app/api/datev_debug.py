"""Temporary debug endpoints to probe DATEV API permissions.

These let us sanity-check whether the registered app has actual
access to each DATEVconnect-online product for the configured
consultant/client. Remove once the real UI is wired up.
"""

from typing import Annotated

from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from app.core import datev_client
from app.core.auth import AuthenticatedUser, require_buchhaltung_user
from app.core.datev_client import DatevApiError, default_client_path
from app.db.session import get_db

router = APIRouter(prefix="/datev/debug", tags=["datev-debug"])


def _probe(db: Session, product: str, path: str) -> dict:
    try:
        body = datev_client.get(db, product, path)
        return {"ok": True, "product": product, "path": path, "body": body}
    except DatevApiError as exc:
        return {
            "ok": False,
            "product": product,
            "path": path,
            "status": exc.status_code,
            "body": exc.body,
        }


@router.get("/probe-all")
def probe_all(
    user: Annotated[AuthenticatedUser, Depends(require_buchhaltung_user)],
    db: Session = Depends(get_db),
) -> dict:
    """Hit the 'does-this-tenant-have-access' endpoint on every subscribed
    DATEV product. 200 = yes, 403 = not activated in RVO, 401 = auth issue.
    """
    c = default_client_path()
    checks = [
        ("hr-exports", "/clients"),
        ("hr-exports", f"/clients/{c}"),
        ("hr-exchange", f"/clients/{c}"),
        ("hr-files", "/clients"),
        ("hr-files", f"/clients/{c}"),
        ("hr-payrollreports", "/clients"),
        ("hr-payrollreports", f"/clients/{c}"),
        ("hr-documents", "/clients"),
        ("hr-documents", f"/clients/{c}"),
    ]
    return {
        "client_id_path": c,
        "results": [_probe(db, product, path) for product, path in checks],
    }


@router.get("/masterdata")
def debug_masterdata(
    user: Annotated[AuthenticatedUser, Depends(require_buchhaltung_user)],
    payroll_accounting_month: str | None = None,
    db: Session = Depends(get_db),
) -> dict:
    """Pull the full employee masterdata list for the default client via
    hr:exports (synchronous). Requires payroll_accounting_month=YYYY-MM
    unless we've already seen one-month default behaviour on DATEV side."""
    c = default_client_path()
    params = {}
    if payroll_accounting_month:
        params["payroll_accounting_month"] = payroll_accounting_month
    try:
        body = datev_client.get(
            db, "hr-exports", f"/clients/{c}/employees/masterdata", params=params
        )
        return {"ok": True, "count": len(body) if isinstance(body, list) else None, "body": body}
    except DatevApiError as exc:
        return {"ok": False, "status": exc.status_code, "body": exc.body}
