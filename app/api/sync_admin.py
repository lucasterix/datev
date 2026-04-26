"""Sync + queue admin endpoints — for manual triggers and health checks.

The autonomous worker can also call ``operation_apply.drain`` and
``sync.full_sync`` itself (see app startup), but exposing them as
endpoints lets Daniel poke things from the UI when needed.
"""

from __future__ import annotations

from typing import Annotated

from fastapi import APIRouter, Depends, status
from sqlalchemy import func, select
from sqlalchemy.orm import Session

from app.core import datev_local_client
from app.core.auth import AuthenticatedUser, require_buchhaltung_user
from app.db.session import get_db
from app.models.pending_operation import PendingOperation
from app.services import operation_apply, sync as sync_service

router = APIRouter(prefix="/datev/sync", tags=["sync"])


@router.get("/health")
def health(
    user: Annotated[AuthenticatedUser, Depends(require_buchhaltung_user)],
    db: Session = Depends(get_db),
) -> dict:
    """Bridge availability + queue depth."""
    bridge_ok = datev_local_client.ping()

    counts = dict(db.execute(
        select(PendingOperation.status, func.count(PendingOperation.id))
        .group_by(PendingOperation.status)
    ).all())

    return {
        "bridge_reachable": bridge_ok,
        "queue": {
            "pending": counts.get("pending", 0),
            "in_progress": counts.get("in_progress", 0),
            "done": counts.get("done", 0),
            "error": counts.get("error", 0),
        },
    }


@router.post("/full")
def full_sync(
    user: Annotated[AuthenticatedUser, Depends(require_buchhaltung_user)],
    db: Session = Depends(get_db),
    fetch_datev_details: bool = False,
) -> dict:
    """Pull DATEV → auto-link → pull Patti.

    By default ``fetch_datev_details=False`` — we only pull the thin
    list (Name, Personalnummer, Eintrittsdatum). Per-employee details
    are loaded lazily when the user opens the profile (avoids ~3min
    sequential 86-employee detail loop that was timing out)."""
    return sync_service.full_sync(db, fetch_datev_details=fetch_datev_details)


@router.post("/drain-queue")
def drain_queue(
    user: Annotated[AuthenticatedUser, Depends(require_buchhaltung_user)],
    db: Session = Depends(get_db),
    max_ops: int = 100,
) -> dict:
    """Apply up to ``max_ops`` pending operations now."""
    return operation_apply.drain(db, max_ops=max_ops)


@router.post("/auto-link", status_code=status.HTTP_200_OK)
def auto_link(
    user: Annotated[AuthenticatedUser, Depends(require_buchhaltung_user)],
    db: Session = Depends(get_db),
) -> dict:
    return sync_service.auto_link_employees(db)
