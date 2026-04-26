"""Admin-side onboarding API.

Two flows:

- ``POST /datev/employees`` — Daniel fills the form himself and submits
  directly. Synchronous: returns the new personnel_number on success.
- ``POST /datev/onboarding/links`` — generate a single-use token whose
  URL Daniel can email to a new hire. The hire fills in the form on a
  public page and submits; the token is consumed and unusable afterward.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel, Field
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.core.auth import AuthenticatedUser, require_buchhaltung_user
from app.core.settings import settings
from app.db.session import get_db
from app.models.onboarding_token import OnboardingToken
from app.services import onboarding
from app.services.onboarding import (
    EmployeeOnboardingPayload,
    OnboardingError,
)

router = APIRouter(tags=["onboarding"])


# --- direct manual add ---------------------------------------------------


@router.post("/datev/employees", status_code=status.HTTP_201_CREATED)
def manual_add(
    payload: EmployeeOnboardingPayload,
    user: Annotated[AuthenticatedUser, Depends(require_buchhaltung_user)],
    db: Session = Depends(get_db),
) -> dict:
    """Create a new employee in DATEV + Patti directly from an admin form."""
    try:
        result = onboarding.submit(
            db, payload, source="manual", submitted_by_email=user.email
        )
    except OnboardingError as exc:
        raise HTTPException(status_code=502, detail=str(exc)) from exc
    return result


# --- onboarding links (token CRUD) --------------------------------------


class CreateLinkBody(BaseModel):
    label: str | None = Field(default=None, max_length=120)
    note: str | None = Field(default=None, max_length=500)
    expires_in_days: int = Field(default=14, ge=1, le=90)
    prefill: dict | None = None  # e.g. {"first_name": "...", "email": "..."}


class TokenOut(BaseModel):
    id: int
    token: str
    label: str | None
    note: str | None
    state: str  # open | consumed | revoked | expired
    created_at: str
    expires_at: str
    consumed_at: str | None
    consumed_personnel_number: int | None
    public_url: str


def _public_url(token: str) -> str:
    """The URL we hand out to the new hire."""
    base = settings.datev_post_auth_redirect.rstrip("/")
    # Strip any path so we land on the root admin host.
    if base.endswith("/admin/buchhaltung/mitarbeiter"):
        base = base[: -len("/admin/buchhaltung/mitarbeiter")]
    elif "/admin" in base:
        base = base.split("/admin")[0]
    return f"{base}/onboarding/{token}"


def _to_out(t: OnboardingToken) -> TokenOut:
    consumed_pnr = None
    if t.consumed_payload and isinstance(t.consumed_payload, dict):
        consumed_pnr = t.consumed_payload.get("personnel_number")
    return TokenOut(
        id=t.id,
        token=t.token,
        label=t.label,
        note=t.note,
        state=t.state,
        created_at=t.created_at.isoformat() if t.created_at else "",
        expires_at=t.expires_at.isoformat() if t.expires_at else "",
        consumed_at=t.consumed_at.isoformat() if t.consumed_at else None,
        consumed_personnel_number=consumed_pnr,
        public_url=_public_url(t.token),
    )


@router.post("/datev/onboarding/links", response_model=TokenOut, status_code=201)
def create_link(
    body: CreateLinkBody,
    user: Annotated[AuthenticatedUser, Depends(require_buchhaltung_user)],
    db: Session = Depends(get_db),
) -> TokenOut:
    token_str = onboarding.new_token()
    row = OnboardingToken(
        token=token_str,
        prefill=body.prefill,
        expires_at=datetime.now(timezone.utc) + timedelta(days=body.expires_in_days),
        created_by_email=user.email,
        label=body.label,
        note=body.note,
    )
    db.add(row)
    db.commit()
    db.refresh(row)
    return _to_out(row)


@router.get("/datev/onboarding/links", response_model=list[TokenOut])
def list_links(
    user: Annotated[AuthenticatedUser, Depends(require_buchhaltung_user)],
    db: Session = Depends(get_db),
) -> list[TokenOut]:
    rows = db.execute(
        select(OnboardingToken).order_by(OnboardingToken.created_at.desc())
    ).scalars().all()
    return [_to_out(r) for r in rows]


@router.delete("/datev/onboarding/links/{link_id}")
def revoke_link(
    link_id: int,
    user: Annotated[AuthenticatedUser, Depends(require_buchhaltung_user)],
    db: Session = Depends(get_db),
) -> dict:
    row = db.get(OnboardingToken, link_id)
    if row is None:
        raise HTTPException(status_code=404, detail="Link nicht gefunden")
    if row.consumed_at is not None:
        raise HTTPException(status_code=409, detail="Link wurde bereits eingelöst")
    if row.revoked_at is None:
        row.revoked_at = datetime.now(timezone.utc)
        db.commit()
    return {"ok": True, "state": row.state}
