"""Parser for DATEV Lohn und Gehalt ASCII export files.

LuG exports two complementary CSV-ish files per monthly run:

- **SD file** (Stammdaten + Abrechnungsaggregate): one row per employee
  × run. ~165 columns, the key reference is the Abrechnungsdatum (run
  month). This holds the snapshot of master data plus *all* monthly
  aggregates (Brutto/Netto/Pauschalen/YTD/…).

- **LA file** (Lohnarten): many rows per employee × run, one per
  wage-type amount. Contains a ``Zuordnungsdatum`` which may differ from
  the Abrechnungsdatum when the entry is a retroactive correction (e.g.
  a salary bump posted back to November 2025 in an April 2026 run).

Both files share the ``;`` delimiter, mixed quoting (some header cells
quoted, some bare; same for data cells), German number format with
``.`` thousands separators + ``,`` decimals, and ``TT.MM.JJJJ`` dates.

Callers should use :func:`parse_sd_file` / :func:`parse_la_file` and
then hand the rows to the import service.
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass, field
from datetime import date
from decimal import Decimal, InvalidOperation
from typing import Iterable


# --- primitive parsers ------------------------------------------------------


def _clean(value: str | None) -> str:
    """Strip whitespace and return empty string for None."""
    if value is None:
        return ""
    return value.strip()


def parse_ascii_int(value: str | None) -> int | None:
    """'00004' -> 4, '' -> None. Raises ValueError on garbage."""
    v = _clean(value)
    if not v:
        return None
    return int(v)


def parse_ascii_date(value: str | None) -> date | None:
    """'01.04.2026' -> date(2026, 4, 1). Empty/whitespace -> None.

    DATEV emits blank date cells as 10 spaces, so strip first."""
    v = _clean(value)
    if not v:
        return None
    try:
        day, month, year = v.split(".")
        return date(int(year), int(month), int(day))
    except (ValueError, AttributeError) as exc:
        raise ValueError(f"Invalid date {value!r}: {exc}") from exc


def parse_ascii_decimal(value: str | None) -> Decimal | None:
    """'2.684,60' -> Decimal('2684.60'). '-19,85' -> Decimal('-19.85').

    German number format: dots as thousand separators, comma as decimal.
    Empty / whitespace-only / literal non-numeric placeholders -> None.
    """
    v = _clean(value)
    if not v:
        return None
    # Some free-text DATEV fields leak into amount columns, e.g.
    # "Verb.Insolvenz" in the Pfändung-Rest column. Treat anything that
    # doesn't look like a number as None (the raw_sd JSON keeps the original).
    if not any(ch.isdigit() for ch in v):
        return None
    # 1) Remove thousand separators, 2) comma -> dot.
    normalized = v.replace(".", "").replace(",", ".")
    try:
        return Decimal(normalized)
    except InvalidOperation:
        return None


def parse_ascii_float(value: str | None) -> float | None:
    """Same as decimal but returns native float for SQLAlchemy Numeric columns
    (SQLAlchemy converts transparently)."""
    d = parse_ascii_decimal(value)
    return float(d) if d is not None else None


# --- file-level parsers -----------------------------------------------------


def _decode(data: bytes | str) -> str:
    """DATEV exports are either Windows-1252 or UTF-8 depending on LuG
    version and locale. Try UTF-8 first, fall back to cp1252 — both are
    single-byte-compatible with ASCII-only sections."""
    if isinstance(data, str):
        return data
    for encoding in ("utf-8-sig", "utf-8", "cp1252"):
        try:
            return data.decode(encoding)
        except UnicodeDecodeError:
            continue
    # Last resort: replace undecodable bytes so parsing keeps going and
    # we surface the issue through import_notes.
    return data.decode("cp1252", errors="replace")


def _iter_rows(text: str) -> Iterable[list[str]]:
    """Yield rows honoring LuG's semicolon delimiter and mixed quoting."""
    reader = csv.reader(
        io.StringIO(text),
        delimiter=";",
        quotechar='"',
        doublequote=True,
        skipinitialspace=False,
    )
    for row in reader:
        # Skip completely empty lines (trailing newlines etc.)
        if not any(cell.strip() for cell in row):
            continue
        yield row


def _rows_with_header(text: str) -> tuple[list[str], list[list[str]]]:
    rows = list(_iter_rows(text))
    if not rows:
        raise ValueError("Empty ASCII export")
    header = [h.strip() for h in rows[0]]
    return header, rows[1:]


# --- dataclasses returned to the import service ----------------------------


@dataclass
class SDRow:
    """One parsed Stammdaten row. Keys correspond to German header names
    (raw_fields) so the import service can look up columns by label.
    Hot fields are also parsed into typed attributes."""

    consultant_number: str
    client_number: str
    reference_month: date  # Abrechnungsdatum truncated to day=1
    processing_code: str | None
    personnel_number: int
    raw_fields: dict[str, str] = field(default_factory=dict)


@dataclass
class LARow:
    """One parsed Lohnart row."""

    consultant_number: str
    client_number: str
    reference_month: date  # Abrechnungsdatum
    allocation_date: date  # Zuordnungsdatum
    processing_code: str | None
    personnel_number: int
    salary_type_code: int
    salary_type_name: str | None
    unit: str | None
    quantity: float | None
    factor: float | None
    percentage: float | None
    tax_flag: str | None
    ss_flag: str | None
    gb_flag: str | None
    amount: float  # LA rows always carry an amount, even if 0.00
    raw_fields: dict[str, str] = field(default_factory=dict)


# --- public API -------------------------------------------------------------


def parse_sd_file(data: bytes | str) -> list[SDRow]:
    """Parse a LuG SD export (Stammdaten + Aggregate).

    Never raises on per-row issues; a malformed row is skipped silently —
    the import service aggregates skipped rows into ``import_notes``.
    Raises on file-level issues (empty, unreadable, missing required
    header columns)."""

    text = _decode(data)
    header, data_rows = _rows_with_header(text)

    required = [
        "Beraternummer",
        "Mandantennummer",
        "Abrechnungsdatum",
        "Personalnummer",
    ]
    for col in required:
        if col not in header:
            raise ValueError(f"SD file missing required column {col!r}")

    out: list[SDRow] = []
    for row in data_rows:
        # Tolerate short/long rows by padding to header length.
        if len(row) < len(header):
            row = row + [""] * (len(header) - len(row))
        fields_by_name = {header[i]: row[i] for i in range(len(header))}

        try:
            personnel_number = parse_ascii_int(fields_by_name.get("Personalnummer"))
            reference = parse_ascii_date(fields_by_name.get("Abrechnungsdatum"))
        except ValueError:
            continue
        if personnel_number is None or reference is None:
            continue

        out.append(
            SDRow(
                consultant_number=_clean(fields_by_name.get("Beraternummer")),
                client_number=_clean(fields_by_name.get("Mandantennummer")),
                reference_month=reference.replace(day=1),
                processing_code=_clean(fields_by_name.get("Verarbeitungskennzeichen")) or None,
                personnel_number=personnel_number,
                raw_fields=fields_by_name,
            )
        )
    return out


def parse_la_file(data: bytes | str) -> list[LARow]:
    """Parse a LuG LA export (Lohnarten-Detail)."""

    text = _decode(data)
    header, data_rows = _rows_with_header(text)

    required = [
        "Beraternummer",
        "Mandantennummer",
        "Abrechnungsdatum",
        "Zuordnungsdatum",
        "Personalnummer",
        "Lohnart",
        "Betrag",
    ]
    for col in required:
        if col not in header:
            raise ValueError(f"LA file missing required column {col!r}")

    out: list[LARow] = []
    for row in data_rows:
        if len(row) < len(header):
            row = row + [""] * (len(header) - len(row))
        fields_by_name = {header[i]: row[i] for i in range(len(header))}

        try:
            personnel_number = parse_ascii_int(fields_by_name.get("Personalnummer"))
            reference = parse_ascii_date(fields_by_name.get("Abrechnungsdatum"))
            allocation = parse_ascii_date(fields_by_name.get("Zuordnungsdatum"))
            salary_type_code = parse_ascii_int(fields_by_name.get("Lohnart"))
            amount = parse_ascii_float(fields_by_name.get("Betrag"))
        except ValueError:
            continue
        if (
            personnel_number is None
            or reference is None
            or allocation is None
            or salary_type_code is None
            or amount is None
        ):
            continue

        out.append(
            LARow(
                consultant_number=_clean(fields_by_name.get("Beraternummer")),
                client_number=_clean(fields_by_name.get("Mandantennummer")),
                reference_month=reference.replace(day=1),
                allocation_date=allocation,
                processing_code=_clean(fields_by_name.get("Verarbeitungskennzeichen")) or None,
                personnel_number=personnel_number,
                salary_type_code=salary_type_code,
                salary_type_name=_clean(fields_by_name.get("Bezeichnung")) or None,
                unit=_clean(fields_by_name.get("Einheit der Menge")) or None,
                quantity=parse_ascii_float(fields_by_name.get("Menge")),
                factor=parse_ascii_float(fields_by_name.get("Faktor")),
                percentage=parse_ascii_float(fields_by_name.get("Prozentzuschlag")),
                tax_flag=_clean(fields_by_name.get("ST")) or None,
                ss_flag=_clean(fields_by_name.get("SV")) or None,
                gb_flag=_clean(fields_by_name.get("GB")) or None,
                amount=amount,
                raw_fields=fields_by_name,
            )
        )
    return out
