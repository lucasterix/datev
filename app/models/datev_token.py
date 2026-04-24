from datetime import datetime

from sqlalchemy import JSON, DateTime, Integer, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class DatevToken(Base):
    """Stores the DATEV OAuth tokens. Singleton-by-convention: one row
    per `scope_key` (currently always 'primary'), upserted on each
    successful token exchange / refresh.

    Because the company has a single DATEV tenant, a per-user token
    is not needed — any authorized buchhaltung user triggers a refresh
    and the latest token set becomes the active one for background
    server-side calls."""

    __tablename__ = "datev_token"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    scope_key: Mapped[str] = mapped_column(String(64), unique=True, index=True, nullable=False)

    access_token: Mapped[str] = mapped_column(Text, nullable=False)
    refresh_token: Mapped[str | None] = mapped_column(Text, nullable=True)
    token_type: Mapped[str] = mapped_column(String(32), nullable=False, default="Bearer")
    scope: Mapped[str] = mapped_column(Text, nullable=False, default="")
    expires_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), nullable=False)

    id_token_claims: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    connected_by_user_id: Mapped[int | None] = mapped_column(Integer, nullable=True)
    connected_by_email: Mapped[str | None] = mapped_column(String(255), nullable=True)
    connected_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    last_refreshed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
