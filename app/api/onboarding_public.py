"""Public onboarding endpoints (no auth).

The new hire doesn't have a Fröhlich Dienste login — they just have
the link Daniel emailed them. The token in the path is the
authorisation: present + unconsumed = OK, anything else = 410 Gone.
"""

from __future__ import annotations

from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.db.session import get_db
from app.models.onboarding_token import OnboardingToken
from app.services import onboarding
from app.services.onboarding import (
    EmployeeOnboardingPayload,
    OnboardingError,
)

router = APIRouter(prefix="/onboarding", tags=["onboarding-public"])


class PublicTokenInfo(BaseModel):
    """Tiny payload returned by GET — enough to pre-fill the form
    without leaking which admin created the token."""
    state: str  # open | consumed | revoked | expired
    label: str | None
    prefill: dict | None


def _load_token(db: Session, token: str) -> OnboardingToken:
    row = db.execute(
        select(OnboardingToken).where(OnboardingToken.token == token)
    ).scalar_one_or_none()
    if row is None:
        raise HTTPException(status_code=404, detail="Link unbekannt")
    return row


@router.get("/{token}", response_model=PublicTokenInfo)
def info(token: str, db: Session = Depends(get_db)) -> PublicTokenInfo:
    row = _load_token(db, token)
    if row.state != "open":
        # 410 Gone for consumed/revoked/expired so the frontend can
        # show a definitive "Link nicht mehr gültig" page.
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail=f"Link {row.state}",
        )
    return PublicTokenInfo(state=row.state, label=row.label, prefill=row.prefill)


@router.post("/{token}")
def submit(
    token: str,
    payload: EmployeeOnboardingPayload,
    db: Session = Depends(get_db),
) -> dict:
    """Submit the filled onboarding form. Single-use: token is marked
    consumed before we even attempt DATEV/Patti so a user can't
    accidentally double-submit by hammering refresh."""
    row = _load_token(db, token)
    if row.state != "open":
        raise HTTPException(
            status_code=status.HTTP_410_GONE,
            detail=f"Link {row.state}",
        )

    # Mark consumed first (idempotency / replay protection).
    row.consumed_at = datetime.now(timezone.utc)
    db.commit()

    try:
        result = onboarding.submit(
            db, payload, source="link", submitted_by_email=None
        )
    except OnboardingError as exc:
        # Roll back the consumption so the user can retry once the
        # underlying issue (bridge offline / DATEV down) is gone.
        row.consumed_at = None
        db.commit()
        raise HTTPException(status_code=502, detail=str(exc)) from exc

    # Persist what came back so the admin sees it next to the link.
    row.consumed_payload = result
    db.commit()

    return {
        "ok": True,
        "personnel_number": result["personnel_number"],
        "message": "Vielen Dank — Deine Daten sind angekommen.",
    }
