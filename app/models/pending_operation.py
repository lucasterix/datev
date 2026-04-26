"""Queue of pending writes to DATEV or Patti.

The bridge to DATEV can be unavailable (PC off, stick out, LuG
closed) and Patti's API can be down too — instead of failing user
edits we persist them as queued operations and let a background
worker drain them when the target is reachable again.

State machine:
- ``pending`` → ``in_progress`` → ``done``
                              ↘ ``error`` (with attempts++/last_error)

Errors that look transient (5xx, BridgeUnavailable, network) trigger
backoff and the row goes back to ``pending``. Errors that look
permanent (400/422/conflict with stale data) stay in ``error`` and
need a human to resolve.
"""

from datetime import datetime

from sqlalchemy import (
    DateTime,
    ForeignKey,
    Integer,
    JSON,
    String,
    Text,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db.base import Base


# Ops we know how to handle. Anything new goes into ``handlers`` in
# the sync worker.
KNOWN_OPS = {
    # → DATEV (via local bridge)
    "datev.update_address",
    "datev.update_account",
    "datev.update_personal_data",
    "datev.update_gross_payment",
    "datev.create_gross_payment",
    "datev.update_hourly_wage",
    "datev.create_calendar_record",
    "datev.update_calendar_record",
    "datev.delete_calendar_record",
    "datev.create_month_record",
    "datev.update_month_record",
    # → Patti
    "patti.update_person",
    "patti.update_address",
    "patti.update_communication",
}


class PendingOperation(Base):
    """One queued write toward DATEV or Patti."""

    __tablename__ = "datev_pending_operation"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    employee_id: Mapped[int] = mapped_column(
        ForeignKey("datev_employee.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # ``"datev.update_address"``, ``"patti.update_communication"`` etc.
    # See ``KNOWN_OPS``.
    op: Mapped[str] = mapped_column(String(48), nullable=False, index=True)

    # Args to the operation handler. Schema depends on ``op``; the
    # handler validates.
    payload: Mapped[dict] = mapped_column(JSON, nullable=False)

    # ``pending`` (default) | ``in_progress`` | ``done`` | ``error``
    status: Mapped[str] = mapped_column(
        String(16), nullable=False, default="pending", index=True
    )

    # Bookkeeping for retries
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    last_attempt_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    # When status==pending, the worker may delay re-trying until this
    # timestamp (set by exponential-backoff after a transient error).
    not_before: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True, index=True
    )

    # Audit
    requested_by_email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), nullable=False
    )

    # Relationship back to Employee for handy access (lazy load is fine).
    employee = relationship("Employee")
