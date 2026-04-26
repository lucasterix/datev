from datetime import date, datetime

from sqlalchemy import Date, DateTime, Float, Integer, JSON, String, Text, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column

from app.db.base import Base


class Employee(Base):
    """Local cache of DATEV Lohn und Gehalt / LODAS employee master data.

    Primary source of truth for reads is DATEV (pulled via hr:exports).
    A handful of hot fields are extracted into dedicated columns for
    filtering and UI display; the full DATEV response is kept in the
    `raw_masterdata` JSONB so downstream features don't require schema
    changes as we discover new fields.

    Edits from the web UI land in `pending_changes` (merged into the
    source-of-truth before a push to DATEV via hr:files or hr:exchange).
    """

    __tablename__ = "datev_employee"
    __table_args__ = (
        UniqueConstraint("client_id_path", "personnel_number", name="uq_employee_tenant_pnr"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    # Tenant (consultant-client) this employee belongs to. Keeps us
    # multi-tenant-ready even though we only handle one mandant today.
    client_id_path: Mapped[str] = mapped_column(String(32), nullable=False, index=True)

    # === DATEV identifiers ===
    personnel_number: Mapped[int] = mapped_column(Integer, nullable=False, index=True)
    company_personnel_number: Mapped[str | None] = mapped_column(String(40), nullable=True)

    # === extracted hot fields (for lists + filters) ===
    first_name: Mapped[str | None] = mapped_column(String(60), nullable=True)
    surname: Mapped[str | None] = mapped_column(String(60), nullable=True)
    date_of_birth: Mapped[date | None] = mapped_column(Date, nullable=True)
    date_of_joining: Mapped[date | None] = mapped_column(Date, nullable=True)
    date_of_leaving: Mapped[date | None] = mapped_column(Date, nullable=True)
    job_title: Mapped[str | None] = mapped_column(String(120), nullable=True)
    weekly_working_hours: Mapped[float | None] = mapped_column(Float, nullable=True)
    type_of_contract: Mapped[str | None] = mapped_column(String(4), nullable=True)
    source_system: Mapped[str | None] = mapped_column(String(8), nullable=True)  # "lug" | "lodas"

    is_active: Mapped[bool] = mapped_column(default=True, nullable=False)

    # === Patti link (resolved by name+born_at match or manual mapping) ===
    patti_user_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    patti_person_id: Mapped[int | None] = mapped_column(Integer, nullable=True, index=True)
    # ``manual`` if a human confirmed the link in the UI, ``auto`` if our
    # matching heuristic resolved it, ``unmatched`` if no link yet.
    patti_link_state: Mapped[str] = mapped_column(
        String(16), nullable=False, default="unmatched"
    )

    # === full DATEV payload + Patti snapshot ===
    raw_masterdata: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    raw_patti: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    # === Edit-queue (legacy small queue from Phase 0) ===
    # Free-form editing fields; the new pending_operations table is the
    # canonical place for queued writes to DATEV/Patti.
    pending_changes: Mapped[dict | None] = mapped_column(JSON, nullable=True)
    pending_since: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    # === sync bookkeeping ===
    last_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_sync_status: Mapped[str | None] = mapped_column(String(16), nullable=True)  # ok | error | never
    last_sync_error: Mapped[str | None] = mapped_column(Text, nullable=True)

    # When did Patti say it last changed this person? Drives last-write-wins.
    patti_updated_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    last_patti_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    # Hash of the DATEV-side snapshot — DATEV's API doesn't expose
    # updated_at, so we fingerprint the payload to detect changes.
    datev_data_hash: Mapped[str | None] = mapped_column(String(64), nullable=True)
    last_datev_synced_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    @property
    def full_name(self) -> str:
        parts = [self.first_name, self.surname]
        return " ".join(p for p in parts if p) or f"#{self.personnel_number}"

    @property
    def has_pending_changes(self) -> bool:
        return bool(self.pending_changes)
