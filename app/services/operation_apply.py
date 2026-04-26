"""Worker that drains the ``datev_pending_operation`` queue.

A pending operation is a queued write to either DATEV (via the local
bridge) or Patti. The worker:

1. Picks ``pending`` ops whose ``not_before`` is in the past (or null).
2. Marks them ``in_progress``, calls the right API.
3. On success: ``done``.
4. On transient error (BridgeUnavailable, 5xx, network): bumps
   ``attempts``, sets exponential ``not_before``, returns to ``pending``.
5. On permanent error (4xx, validation): ``error`` until human resolves.

This module is import-safe (no DB session at import time). Caller
provides a Session per drain run.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Callable

import httpx
import requests
from sqlalchemy import select
from sqlalchemy.orm import Session

from app.clients.patti_client import PattiClient, PattiError
from app.core import datev_local_client
from app.core.datev_local_client import BridgeUnavailable, LocalDatevError
from app.core.logging import get_logger
from app.models.pending_operation import PendingOperation

logger = get_logger("datev.op_apply")


class OperationError(Exception):
    """Permanent failure — caller marks the op as ``error``."""


# --- handler signatures ----------------------------------------------------
# Each handler takes (op_payload: dict) and either returns normally
# (success) or raises:
# - BridgeUnavailable / LocalDatevError(503/5xx) / requests transient
#   → transient (retry)
# - LocalDatevError(4xx) / PattiError / OperationError → permanent
# - Anything else → log + treat as permanent so we don't loop


_TRANSIENT_HTTP = (
    BridgeUnavailable,
    httpx.ConnectError,
    httpx.ReadTimeout,
    httpx.RemoteProtocolError,
    requests.exceptions.ConnectionError,
    requests.exceptions.ReadTimeout,
    requests.exceptions.ChunkedEncodingError,
)


# --- DATEV handlers ------------------------------------------------------


def _h_datev_update_address(payload: dict) -> None:
    pnr = payload["personnel_number"]
    body = payload["address"]
    datev_local_client.update_address(pnr, body)


def _h_datev_update_account(payload: dict) -> None:
    datev_local_client.update_account(payload["personnel_number"], payload["account"])


def _h_datev_update_personal_data(payload: dict) -> None:
    datev_local_client.update_personal_data(
        payload["personnel_number"], payload["personal_data"]
    )


def _h_datev_create_gross_payment(payload: dict) -> None:
    datev_local_client.create_gross_payment(
        payload["personnel_number"], payload["gross_payment"]
    )


def _h_datev_update_gross_payment(payload: dict) -> None:
    datev_local_client.update_gross_payment(
        payload["personnel_number"],
        payload["gross_payment_id"],
        payload["gross_payment"],
    )


def _h_datev_update_hourly_wage(payload: dict) -> None:
    datev_local_client.update_hourly_wage(
        payload["personnel_number"],
        payload["hourly_wage_id"],
        payload["hourly_wage"],
    )


def _h_datev_create_calendar_record(payload: dict) -> None:
    datev_local_client.create_calendar_record(
        payload["personnel_number"], payload["record"]
    )


def _h_datev_update_calendar_record(payload: dict) -> None:
    datev_local_client.update_calendar_record(
        payload["calendar_record_id"], payload["record"]
    )


def _h_datev_delete_calendar_record(payload: dict) -> None:
    datev_local_client.delete_calendar_record(payload["calendar_record_id"])


def _h_datev_create_month_record(payload: dict) -> None:
    datev_local_client.create_month_record(payload["personnel_number"], payload["record"])


def _h_datev_update_month_record(payload: dict) -> None:
    datev_local_client.update_month_record(payload["month_record_id"], payload["record"])


# --- Patti handlers ------------------------------------------------------


def _patti_session() -> PattiClient:
    """Patti login is per-call: short-lived session, cheap to relogin
    (single HTTP request after the form GET). Keeps the worker stateless."""
    c = PattiClient()
    c.login()
    return c


def _h_patti_update_person(payload: dict) -> None:
    c = _patti_session()
    c.update_person(payload["person_id"], **payload["fields"])


def _h_patti_update_address(payload: dict) -> None:
    c = _patti_session()
    c.update_address(payload["address_id"], **payload["fields"])


def _h_patti_update_communication(payload: dict) -> None:
    c = _patti_session()
    c.update_communication(payload["communication_id"], **payload["fields"])


# --- dispatch table ------------------------------------------------------


HANDLERS: dict[str, Callable[[dict], None]] = {
    "datev.update_address": _h_datev_update_address,
    "datev.update_account": _h_datev_update_account,
    "datev.update_personal_data": _h_datev_update_personal_data,
    "datev.create_gross_payment": _h_datev_create_gross_payment,
    "datev.update_gross_payment": _h_datev_update_gross_payment,
    "datev.update_hourly_wage": _h_datev_update_hourly_wage,
    "datev.create_calendar_record": _h_datev_create_calendar_record,
    "datev.update_calendar_record": _h_datev_update_calendar_record,
    "datev.delete_calendar_record": _h_datev_delete_calendar_record,
    "datev.create_month_record": _h_datev_create_month_record,
    "datev.update_month_record": _h_datev_update_month_record,
    "patti.update_person": _h_patti_update_person,
    "patti.update_address": _h_patti_update_address,
    "patti.update_communication": _h_patti_update_communication,
}


# --- backoff ------------------------------------------------------------


def _backoff_seconds(attempts: int) -> int:
    """Exponential, capped: 30s, 1min, 2min, 5min, 15min, 30min, 1h."""
    schedule = [30, 60, 120, 300, 900, 1800, 3600]
    return schedule[min(attempts - 1, len(schedule) - 1)]


# --- public API ---------------------------------------------------------


def apply_one(db: Session, op: PendingOperation) -> str:
    """Apply a single op. Returns ``"done"`` / ``"retry"`` / ``"error"``.

    Caller commits. ``op.status`` is mutated in place.
    """
    handler = HANDLERS.get(op.op)
    if handler is None:
        op.status = "error"
        op.last_error = f"unknown op: {op.op}"
        op.last_attempt_at = datetime.now(timezone.utc)
        return "error"

    op.status = "in_progress"
    op.attempts += 1
    op.last_attempt_at = datetime.now(timezone.utc)

    try:
        handler(op.payload)
        op.status = "done"
        op.last_error = None
        op.not_before = None
        logger.info("op_applied", op_id=op.id, op=op.op, attempts=op.attempts)
        return "done"

    except _TRANSIENT_HTTP as exc:
        # Transient — schedule retry
        delay = _backoff_seconds(op.attempts)
        op.status = "pending"
        op.not_before = datetime.now(timezone.utc) + timedelta(seconds=delay)
        op.last_error = f"transient: {type(exc).__name__}: {exc}"
        logger.info(
            "op_transient_retry",
            op_id=op.id, op=op.op, attempts=op.attempts,
            delay_seconds=delay,
        )
        return "retry"

    except LocalDatevError as exc:
        # 4xx-class — permanent unless 5xx (5xx already handled by BridgeUnavailable for 503,
        # but some other 5xx fall here). Treat 5xx as transient too.
        if 500 <= exc.status_code < 600:
            delay = _backoff_seconds(op.attempts)
            op.status = "pending"
            op.not_before = datetime.now(timezone.utc) + timedelta(seconds=delay)
            op.last_error = f"datev 5xx: {exc.body!r}"
            return "retry"
        op.status = "error"
        op.last_error = f"datev {exc.status_code}: {exc.body!r}"
        logger.warning("op_permanent_datev_error", op_id=op.id, op=op.op,
                       status=exc.status_code, body=exc.body)
        return "error"

    except PattiError as exc:
        op.status = "error"
        op.last_error = f"patti: {exc}"
        logger.warning("op_permanent_patti_error", op_id=op.id, op=op.op, error=str(exc))
        return "error"

    except requests.exceptions.HTTPError as exc:
        # Patti 4xx after auto-relogin
        op.status = "error"
        op.last_error = f"patti http: {exc}"
        return "error"

    except Exception as exc:  # noqa: BLE001 — never block the queue
        op.status = "error"
        op.last_error = f"unexpected: {type(exc).__name__}: {exc}"
        logger.exception("op_unexpected_error", op_id=op.id, op=op.op)
        return "error"


def drain(db: Session, *, max_ops: int = 50) -> dict[str, int]:
    """Apply up to ``max_ops`` pending operations whose ``not_before``
    has elapsed. Returns counts per outcome."""
    now = datetime.now(timezone.utc)
    rows = db.execute(
        select(PendingOperation)
        .where(
            PendingOperation.status == "pending",
            (PendingOperation.not_before.is_(None)) | (PendingOperation.not_before <= now),
        )
        .order_by(PendingOperation.created_at.asc())
        .limit(max_ops)
    ).scalars().all()

    counts = {"done": 0, "retry": 0, "error": 0, "total": len(rows)}
    for op in rows:
        outcome = apply_one(db, op)
        counts[outcome] = counts.get(outcome, 0) + 1
        db.commit()

    return counts


def enqueue(
    db: Session,
    *,
    employee_id: int,
    op: str,
    payload: dict,
    requested_by_email: str | None = None,
) -> PendingOperation:
    """Append a new pending operation. Caller commits."""
    from app.models.pending_operation import KNOWN_OPS
    if op not in KNOWN_OPS:
        raise ValueError(f"unknown operation: {op}")
    row = PendingOperation(
        employee_id=employee_id,
        op=op,
        payload=payload,
        status="pending",
        requested_by_email=requested_by_email,
    )
    db.add(row)
    db.flush()
    return row
