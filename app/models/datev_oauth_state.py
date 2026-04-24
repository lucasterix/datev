from datetime import datetime

from sqlalchemy import DateTime, Integer, String, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class DatevOAuthState(Base):
    """Transient record created when an OAuth authorization flow starts.
    Holds the PKCE code_verifier and the CSRF `state` so the callback
    can complete the token exchange. Cleaned up on callback success or
    after short TTL (~10 minutes)."""

    __tablename__ = "datev_oauth_state"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    state: Mapped[str] = mapped_column(String(128), unique=True, index=True, nullable=False)
    code_verifier: Mapped[str] = mapped_column(String(256), nullable=False)
    initiated_by_user_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    initiated_by_email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    return_to: Mapped[str | None] = mapped_column(String(512), nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
