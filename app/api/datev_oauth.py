"""Routes for the DATEV Authorization Code + PKCE flow.

- GET  /datev/oauth/authorize  → returns the DATEV login URL
- GET  /datev/oauth/callback   → DATEV redirects here with code+state
- GET  /datev/status           → is the service connected, and until when
"""

from datetime import datetime, timezone
from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy.orm import Session

from app.core.auth import AuthenticatedUser, require_buchhaltung_user
from app.core.datev_oauth import (
    build_authorize_url,
    exchange_code,
    get_current_token,
    refresh_if_needed,
)
from app.core.logging import get_logger
from app.core.settings import settings
from app.db.session import get_db

router = APIRouter(prefix="/datev", tags=["datev-oauth"])
logger = get_logger("datev.oauth.api")


@router.get("/oauth/authorize")
def oauth_authorize(
    user: Annotated[AuthenticatedUser, Depends(require_buchhaltung_user)],
    return_to: str | None = Query(default=None),
    db: Session = Depends(get_db),
) -> dict:
    """Called by the admin-web when a buchhaltung user clicks
    'Mit DATEV verbinden'. Returns the URL the browser should navigate
    to. (We return JSON instead of a 302 so the SPA can log/observe it.)"""
    url = build_authorize_url(
        db,
        initiated_by_user_id=user.id,
        initiated_by_email=user.email,
        return_to=return_to or settings.datev_post_auth_redirect,
    )
    return {"authorize_url": url}


@router.get("/oauth/callback")
def oauth_callback(
    request: Request,
    code: str | None = Query(default=None),
    state: str | None = Query(default=None),
    error: str | None = Query(default=None),
    error_description: str | None = Query(default=None),
    db: Session = Depends(get_db),
):
    """DATEV redirects here after the user logs in on the DATEV side.
    Completes the token exchange and bounces the browser back to the
    admin-web Buchhaltung UI."""
    if error:
        logger.warning("datev_oauth_error_callback", error=error, description=error_description)
        return _simple_error_page(error, error_description)

    if not code or not state:
        return _simple_error_page("invalid_request", "Missing code or state")

    try:
        token = exchange_code(db, code=code, state=state)
    except ValueError as exc:
        return _simple_error_page("invalid_state", str(exc))
    except Exception as exc:  # noqa: BLE001 — surface *something* to the user
        logger.exception("datev_code_exchange_failed")
        return _simple_error_page("exchange_failed", str(exc))

    # Figure out where to send the browser next. Stored return_to
    # was consumed along with the state; fall back to config default.
    target = settings.datev_post_auth_redirect
    status_url = (
        target + ("&" if "?" in target else "?") + f"datev_connected_at={int(token.connected_at.timestamp())}"
    )
    return RedirectResponse(url=status_url, status_code=302)


@router.get("/status")
def status_endpoint(
    user: Annotated[AuthenticatedUser, Depends(require_buchhaltung_user)],
    db: Session = Depends(get_db),
) -> dict:
    token = get_current_token(db)
    if token is None:
        return {
            "connected": False,
            "environment": settings.datev_environment,
            "scopes_requested": settings.datev_scopes_list,
        }

    now = datetime.now(timezone.utc)
    seconds_until_expiry = int((token.expires_at - now).total_seconds())

    # Opportunistic refresh on status probes — keeps idle tokens warm.
    refreshed = False
    if seconds_until_expiry < 60 and token.refresh_token:
        try:
            token = refresh_if_needed(db, token)
            db.commit()
            refreshed = True
            seconds_until_expiry = int((token.expires_at - now).total_seconds())
        except Exception:
            logger.exception("datev_refresh_on_status_failed")

    return {
        "connected": True,
        "environment": settings.datev_environment,
        "scope": token.scope,
        "connected_at": token.connected_at.isoformat(),
        "last_refreshed_at": token.last_refreshed_at.isoformat() if token.last_refreshed_at else None,
        "access_token_expires_in_seconds": seconds_until_expiry,
        "has_refresh_token": bool(token.refresh_token),
        "connected_by_email": token.connected_by_email,
        "id_token_claims": token.id_token_claims,
        "refreshed_just_now": refreshed,
    }


@router.post("/disconnect")
def disconnect(
    user: Annotated[AuthenticatedUser, Depends(require_buchhaltung_user)],
    db: Session = Depends(get_db),
) -> dict:
    token = get_current_token(db)
    if token is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Not connected")
    db.delete(token)
    db.commit()
    logger.info("datev_disconnected", by_email=user.email)
    return {"ok": True}


def _simple_error_page(error: str, description: str | None) -> HTMLResponse:
    body = f"""<!doctype html>
<html lang="de">
<head><meta charset="utf-8"><title>DATEV Auth-Fehler</title></head>
<body style="font-family:system-ui;max-width:640px;margin:40px auto;padding:0 16px;color:#0f172a;">
<h1 style="color:#b91c1c;">DATEV-Anmeldung fehlgeschlagen</h1>
<p><strong>Grund:</strong> {error}</p>
<p>{description or ""}</p>
<p><a href="{settings.datev_post_auth_redirect}">Zurück zur Buchhaltung</a></p>
</body></html>
"""
    return HTMLResponse(content=body, status_code=400)
