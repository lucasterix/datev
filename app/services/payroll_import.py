"""Import service: LuG-ASCII export → PayrollStatement + PayrollLineItem rows.

Takes parsed SD and LA rows, validates they belong to the same run,
upserts statements (one per employee × reference_month) and re-creates
line items on each import so a re-upload of the same month stays
idempotent.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import date
from typing import Iterable

from sqlalchemy import delete, select
from sqlalchemy.orm import Session

from app.core.logging import get_logger
from app.models.payroll_statement import PayrollLineItem, PayrollStatement
from app.services.lug_ascii_parser import (
    LARow,
    SDRow,
    parse_ascii_date,
    parse_ascii_float,
    parse_ascii_int,
    parse_la_file,
    parse_sd_file,
)

logger = get_logger("datev.payroll_import")


# --- German column name → statement column name -----------------------------

# Hot fields promoted out of raw_sd into typed columns. Keep this list
# together with the model — if you add a column to PayrollStatement,
# add its header mapping here.
_SD_STRING_FIELDS = {
    "Familienname": "surname",
    "Vorname": "first_name",
    "Straße/Postfach": "street",
    "PLZ": "postal_code",
    "Ort": "city",
    "Nationalitätskennzeichen": "country_code",
    "Geschlecht": "sex",
    "Versicherungsnummer": "social_security_number",
    "Staatsangehörigkeit": "nationality",
    "IBAN": "iban",
    "BIC": "bic",
    "Berufsbezeichnung": "job_title",
    "Arbeitnehmertyp": "activity_type",
    "Personengruppenschlüssel": "employee_group_code",
    "Beitragsgruppenschlüssel": "contribution_group_key",
    "Konfession AN": "denomination",
    "Konfession Ehegatte": "spouse_denomination",
    "Identifikationsnummer": "tax_identification_number",
    "Finanzamtsnummer": "finanzamt_number",
    # "Krankenkasse" holds the Kassen-Betriebsnummer (BN), e.g. "29720865"
    # for AOK Niedersachsen. The human-readable name lives in
    # "KK-Zusatzinformation". LuG's header labels are counter-intuitive.
    "KK-Zusatzinformation": "health_insurer_name",
    "Krankenkasse": "health_insurer_number",
}

_SD_DATE_FIELDS = {
    "Geburtsdatum": "date_of_birth",
    "Eintrittsdatum": "date_of_joining",
    "Austrittsdatum": "date_of_leaving",
}

_SD_INT_FIELDS = {
    "Steuerklasse": "tax_class",
}

_SD_DECIMAL_FIELDS = {
    "Steuerfaktor": "tax_factor",
    "Kinderfreibetrag": "child_tax_allowances",
    "Wöchentliche Arbeitszeit": "weekly_working_hours",
    "Urlaubsanspruch pro Jahr": "annual_vacation_days",
    "Urlaubsanspruch lfd Jahr": "vacation_entitlement_current_year",
    "Genommener Urlaub lfd Jahr": "vacation_taken_current_year",
    "Resturlaub lfd Jahr": "vacation_remaining_current_year",
    "Gesamtbrutto": "gross_total",
    "Steuerbrutto": "gross_tax",
    "Steuerrechtl Abzüge": "tax_deductions",
    "KV/PV-Brutto": "gross_kv_pv",
    "RV/AV-Brutto": "gross_rv_av",
    "SV-rechtl Abzüge": "ss_deductions",
    "Nettoverdienst": "net_income",
    "Auszahlungsbetrag Euro": "payout_eur",
    "Pauschale LST": "flat_tax_lst",
    "Pauschale KiST": "flat_tax_kist",
    "Pauschaler Solz": "flat_tax_solz",
    "Gesamtbrutto-gesamt": "gross_total_ytd",
    "Steuerbrutto-gesamt": "gross_tax_ytd",
    "SV-Brutto-gesamt": "gross_ss_ytd",
    "SV-AG-Anteil mtl": "ss_employer_share_monthly",
    "SV-AG-Anteil kumuliert": "ss_employer_share_ytd",
    "Umlagebeiträge": "allocation_contributions",
    "Anwesenheitstage": "days_present",
    "Anwesenheitsstunden": "hours_present",
    "Bezahlte Stunden": "hours_paid",
    "Krankheitstage": "days_sick",
    "Krankheitsstunden": "hours_sick",
    "Überstunden": "hours_overtime",
}


def _sd_to_columns(sd: SDRow) -> dict:
    """Map the parsed SD row into statement-column kwargs."""
    columns: dict = {}
    for header, attr in _SD_STRING_FIELDS.items():
        raw = (sd.raw_fields.get(header) or "").strip()
        columns[attr] = raw or None
    for header, attr in _SD_DATE_FIELDS.items():
        try:
            columns[attr] = parse_ascii_date(sd.raw_fields.get(header))
        except ValueError:
            columns[attr] = None
    for header, attr in _SD_INT_FIELDS.items():
        try:
            columns[attr] = parse_ascii_int(sd.raw_fields.get(header))
        except ValueError:
            columns[attr] = None
    for header, attr in _SD_DECIMAL_FIELDS.items():
        columns[attr] = parse_ascii_float(sd.raw_fields.get(header))
    return columns


# --- result type -----------------------------------------------------------


@dataclass
class ImportResult:
    batch_id: str
    reference_month: date
    consultant_number: str
    client_number: str
    statements_created: int
    statements_updated: int
    line_items_written: int
    skipped_sd_rows: int
    skipped_la_rows: int
    warnings: list[str]

    def to_dict(self) -> dict:
        return {
            "batch_id": self.batch_id,
            "reference_month": self.reference_month.isoformat(),
            "consultant_number": self.consultant_number,
            "client_number": self.client_number,
            "statements_created": self.statements_created,
            "statements_updated": self.statements_updated,
            "line_items_written": self.line_items_written,
            "skipped_sd_rows": self.skipped_sd_rows,
            "skipped_la_rows": self.skipped_la_rows,
            "warnings": self.warnings,
        }


# --- main entry point ------------------------------------------------------


class PayrollImportError(Exception):
    """Raised on file-level problems (wrong tenant, inconsistent months,
    corrupt files). Per-row problems are collected as warnings instead."""


def import_lug_ascii(
    db: Session,
    *,
    sd_bytes: bytes,
    la_bytes: bytes,
    expected_client_id_path: str,
    imported_by_email: str | None,
) -> ImportResult:
    """Parse and persist one monthly LuG export.

    Re-running for the same reference_month and tenant replaces all line
    items for affected statements and re-upserts the SD aggregates.
    """

    sd_rows = parse_sd_file(sd_bytes)
    la_rows = parse_la_file(la_bytes)

    if not sd_rows:
        raise PayrollImportError("SD-Datei enthält keine verwertbaren Zeilen")
    if not la_rows:
        raise PayrollImportError("LA-Datei enthält keine verwertbaren Zeilen")

    warnings: list[str] = []

    # --- consistency: both files must describe the same run ---
    consultant, client, reference = _assert_single_run(sd_rows, la_rows)

    client_id_path = f"{consultant}-{client}"
    if client_id_path != expected_client_id_path:
        raise PayrollImportError(
            f"Mandant in der Datei ({client_id_path}) stimmt nicht mit der "
            f"konfigurierten Umgebung ({expected_client_id_path}) überein"
        )

    # --- upsert statements ---
    batch_id = str(uuid.uuid4())
    created = 0
    updated = 0
    statements_by_pnr: dict[int, PayrollStatement] = {}

    for sd in sd_rows:
        existing = db.execute(
            select(PayrollStatement).where(
                PayrollStatement.client_id_path == client_id_path,
                PayrollStatement.personnel_number == sd.personnel_number,
                PayrollStatement.reference_month == reference,
            )
        ).scalar_one_or_none()

        kwargs = _sd_to_columns(sd)

        if existing is None:
            stmt = PayrollStatement(
                client_id_path=client_id_path,
                consultant_number=consultant,
                client_number=client,
                personnel_number=sd.personnel_number,
                reference_month=reference,
                processing_code=sd.processing_code,
                import_batch_id=batch_id,
                imported_by_email=imported_by_email,
                raw_sd=sd.raw_fields,
                **kwargs,
            )
            db.add(stmt)
            db.flush()  # get stmt.id
            statements_by_pnr[sd.personnel_number] = stmt
            created += 1
        else:
            for attr, value in kwargs.items():
                setattr(existing, attr, value)
            existing.processing_code = sd.processing_code
            existing.raw_sd = sd.raw_fields
            existing.import_batch_id = batch_id
            existing.imported_by_email = imported_by_email

            # Replace line items wholesale — simpler than diffing and
            # LA rows contain corrections that depend on current state.
            db.execute(
                delete(PayrollLineItem).where(PayrollLineItem.statement_id == existing.id)
            )
            db.flush()
            statements_by_pnr[sd.personnel_number] = existing
            updated += 1

    # --- write line items ---
    line_items_written = 0
    skipped_la = 0
    for la in la_rows:
        stmt = statements_by_pnr.get(la.personnel_number)
        if stmt is None:
            # LA rows reference a personnel_number we don't have a SD row
            # for — skip and warn (could happen if SD export is truncated).
            skipped_la += 1
            continue

        db.add(
            PayrollLineItem(
                statement_id=stmt.id,
                personnel_number=la.personnel_number,
                allocation_date=la.allocation_date,
                processing_code=la.processing_code,
                salary_type_code=la.salary_type_code,
                salary_type_name=la.salary_type_name,
                unit=la.unit,
                quantity=la.quantity,
                factor=la.factor,
                percentage=la.percentage,
                tax_flag=la.tax_flag,
                ss_flag=la.ss_flag,
                gb_flag=la.gb_flag,
                amount=la.amount,
                raw_la=la.raw_fields,
            )
        )
        line_items_written += 1

    if skipped_la:
        warnings.append(
            f"{skipped_la} LA-Zeilen übersprungen — Mitarbeiter fehlte in SD-Datei"
        )

    db.commit()

    logger.info(
        "payroll_import_done",
        batch_id=batch_id,
        reference_month=reference.isoformat(),
        statements_created=created,
        statements_updated=updated,
        line_items=line_items_written,
        skipped_la=skipped_la,
    )

    return ImportResult(
        batch_id=batch_id,
        reference_month=reference,
        consultant_number=consultant,
        client_number=client,
        statements_created=created,
        statements_updated=updated,
        line_items_written=line_items_written,
        skipped_sd_rows=0,
        skipped_la_rows=skipped_la,
        warnings=warnings,
    )


def _assert_single_run(
    sd_rows: Iterable[SDRow],
    la_rows: Iterable[LARow],
) -> tuple[str, str, date]:
    """Make sure all rows in both files refer to the same (consultant,
    client, reference_month). Mixed-run files are almost certainly a
    user mistake."""

    sd_list = list(sd_rows)
    la_list = list(la_rows)

    sd_runs = {(r.consultant_number, r.client_number, r.reference_month) for r in sd_list}
    if len(sd_runs) > 1:
        raise PayrollImportError(
            f"SD-Datei enthält mehrere Abrechnungsläufe: {sorted(sd_runs)}"
        )
    la_runs = {(r.consultant_number, r.client_number, r.reference_month) for r in la_list}
    if len(la_runs) > 1:
        raise PayrollImportError(
            f"LA-Datei enthält mehrere Abrechnungsläufe: {sorted(la_runs)}"
        )

    if sd_runs != la_runs:
        raise PayrollImportError(
            "SD- und LA-Datei gehören nicht zum selben Abrechnungslauf "
            f"(SD: {sd_runs}, LA: {la_runs})"
        )

    consultant, client, reference = next(iter(sd_runs))
    return consultant, client, reference
