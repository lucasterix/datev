"""One-shot onboarding link tokens.

Daniel hands out per-employee links so a new hire can fill in their
own master data instead of dictating it. Each token is single-use:
the moment ``consumed_at`` is set, all subsequent GET/POST against
that token return 410 Gone.

We also let Daniel revoke a token early (e.g. if he sent it to the
wrong address) by setting ``revoked_at`` manually.
"""

from datetime import datetime

from sqlalchemy import (
    DateTime,
    Integer,
    JSON,
    String,
    Text,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class OnboardingToken(Base):
    """Single-use onboarding token issued by an admin."""

    __tablename__ = "datev_onboarding_token"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # The shareable token — UUIDv4 hex without dashes, ~32 chars.
    token: Mapped[str] = mapped_column(String(64), nullable=False, unique=True, index=True)

    # Optional pre-fill: the admin can already include name + email so
    # the employee just verifies + fills the rest.
    prefill: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    # Once-only: set when the employee submits the form.
    consumed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    consumed_payload: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    # Manual revoke ("oops, wrong address")
    revoked_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # Auto-expiry — default 30 days from creation. Even if the link is
    # forgotten in someone's inbox, it goes stale.
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    # Audit
    created_by_email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    label: Mapped[str | None] = mapped_column(String(120), nullable=True)
    note: Mapped[str | None] = mapped_column(Text, nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )

    @property
    def state(self) -> str:
        if self.consumed_at:
            return "consumed"
        if self.revoked_at:
            return "revoked"
        if self.expires_at and self.expires_at < datetime.utcnow().astimezone(self.expires_at.tzinfo):
            return "expired"
        return "open"
