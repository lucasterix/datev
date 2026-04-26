"""Unit tests for the LuG-ASCII parser.

Fixtures use the real LuG SD/LA column layout (taken from an actual
export, columns are not PII) but synthesized values. Data rows are
constructed from a mapping to guarantee column alignment.
"""

from datetime import date
from decimal import Decimal

import csv
import io

import pytest

from app.services.lug_ascii_parser import (
    parse_ascii_date,
    parse_ascii_decimal,
    parse_ascii_float,
    parse_ascii_int,
    parse_la_file,
    parse_sd_file,
)


# --- primitive parsers ------------------------------------------------------


class TestAsciiInt:
    def test_strips_leading_zeros(self):
        assert parse_ascii_int("00004") == 4

    def test_empty_is_none(self):
        assert parse_ascii_int("") is None
        assert parse_ascii_int("   ") is None
        assert parse_ascii_int(None) is None


class TestAsciiDate:
    def test_german_date(self):
        assert parse_ascii_date("01.04.2026") == date(2026, 4, 1)

    def test_padded_blank_is_none(self):
        assert parse_ascii_date("          ") is None
        assert parse_ascii_date("") is None
        assert parse_ascii_date(None) is None

    def test_invalid_raises(self):
        with pytest.raises(ValueError):
            parse_ascii_date("not-a-date")


class TestAsciiDecimal:
    def test_thousands_and_decimal(self):
        assert parse_ascii_decimal("2.684,60") == Decimal("2684.60")

    def test_negative(self):
        assert parse_ascii_decimal("-19,85") == Decimal("-19.85")

    def test_zero(self):
        assert parse_ascii_decimal("0,00") == Decimal("0.00")

    def test_plain_number(self):
        assert parse_ascii_decimal("20") == Decimal("20")

    def test_padded_space(self):
        assert parse_ascii_decimal("      2.890,30") == Decimal("2890.30")

    def test_non_numeric_text_becomes_none(self):
        # Real-world: "Verb.Insolvenz" appears in a Pfändung-Rest cell.
        assert parse_ascii_decimal("Verb.Insolvenz") is None

    def test_empty_is_none(self):
        assert parse_ascii_decimal("") is None
        assert parse_ascii_decimal("   ") is None
        assert parse_ascii_decimal(None) is None

    def test_float_helper(self):
        assert parse_ascii_float("1.234,56") == pytest.approx(1234.56)
        assert parse_ascii_float("") is None


# --- SD file fixture --------------------------------------------------------

# Real LuG SD header (column layout only — not PII). Copied verbatim so
# our parser's column-name lookups match production.
SD_HEADER = (
    '"Beraternummer";"Mandantennummer";"Abrechnungsdatum";"Verarbeitungskennzeichen";'
    '"Personalnummer";"Familienname";"Vorname";"Straße/Postfach";"Nationalitätskennzeichen";'
    '"PLZ";"Ort";Geburtsdatum;"Geschlecht";"Versicherungsnummer";"Familienstand";'
    '"Staatsangehörigkeit";"Kontonummer";"Bankleitzahl";"IBAN";"BIC";'
    '"Datum erster Eintritt";"Eintrittsdatum";"Austrittsdatum";"Berufsbezeichnung";'
    '"Standardentlohnung";"Personengruppenschlüssel";"Tätigkeitsmerkmal";"Arbeitnehmertyp";'
    '"Kammerbeiträge";"Beitragsgruppenschlüssel";Umlagepflicht;"KV/PV NBL";"RV/AV/NBL";'
    '"Arbeitnehmer trägt PV erste Stufe allein";"Krankenkasse";"KK-Nr.";"KK-Zusatzinformation";'
    '"Beitrag der Kasse/manuell";"Freiw KV - Gesamtbeitrag";"Freiw KV - AG-Zuschuß";'
    '"Freiw PV - Gesamtbeitrag";"Freiw PV - AG-Zuschuß";"Priv KV - Gesamtbeitrag";'
    '"Priv KV - AG-Zuschuß";"Priv PV - Gesamtbeitrag";"Priv PV - AG-Zuschuß";"Gemeinde";'
    '"Finanzamtsnummer";"Finanzamtsbezeichnung";"Steuernummer";Identifikationsnummer;'
    '"Steuerklasse";Steuerfaktor;"Kinderfreibetrag";"Freistellungsbescheinigung";'
    '"Datum der Freistellungsbescheinigung";"Freibetrag jährlich";"Freibetrag mtl";'
    '"Konfession AN";"Konfession Ehegatte";"B-Tabelle";"Großbuchstabe F";"Angabe LJA";'
    '"Festbezug 1 - LA";"Festbezug 1 - Betrag";"Festbezug 2 - LA";"Festbezug 2 - Betrag";'
    '"Festbezug 3 - LA";"Festbezug 3 - Betrag";"Festbezug 4 - LA";"Festbezug 4 - Betrag";'
    '"Festbezug 5 - LA";"Festbezug 5 - Betrag";"Festbezug 6 - LA";"Festbezug 6 - Betrag";'
    '"Festbezug 7 - LA";"Festbezug 7 - Betrag";"Festbezug 8 - LA";"Festbezug 8 - Betrag";'
    '"Festbezug 9 - LA";"Festbezug 9 - Betrag";"Festbezug 10 - LA";"Festbezug 10 - Betrag";'
    '"Stundenlohn 1";"Stundenlohn 2";"Stundenlohn 3";"Stundenlohn 4";"Stundenlohn 5";'
    '"Tagelohn 1";"Tagelohn 2";"Tagelohn 3";"Tagelohn 4";"Tagelohn 5";"Berufsgenossenschaft";'
    '"Gefahrenklasse";"Unfallversicherungspflichtig";"UV-Brutto";'
    '"Nummer der Berufsgenossenschaft 1";"Mitgliedsnummer BG 1";"Gefahrtarifstelle 1";'
    '"Prozent Gefahrtarifstelle 1";"BGNr für Fremdgefahrtarifstelle 1";"UV-Grund 1";'
    '"Nummer der Berufsgenossenschaft 2";"Mitgliedsnummer BG 2";"Gefahrtarifstelle 2";'
    '"Prozent Gefahrtarifstelle 2";"BGNr für Fremdgefahrtarifstelle 2";"UV-Grund 2";'
    '"Nummer der Berufsgenossenschaft 3";"Mitgliedsnummer BG 3";"Gefahrtarifstelle 3";'
    '"Prozent Gefahrtarifstelle 3";"BGNr für Fremdgefahrtarifstelle 3";"UV-Grund 3";'
    '"Betriebsstätte Nr";"Betriebsstätte Bezeichnung";"Kostenstelle/Stammkostenstelle";'
    '"Kostenverteilung 1-5";"Abteilung";"Gruppe";"Wöchentliche Arbeitszeit";'
    '"Urlaubsanspruch pro Jahr";"Sonderurlaub pro Jahr";"Urlaubsanspruch lfd Jahr";'
    '"Resturlaub aus Vorjahr";"Urlaub-Gesamtanspruch";"Genommener Urlaub lfd Jahr";'
    '"Mtl Gen Urlaub";"Resturlaub lfd Jahr";"Beginn Altersteilzeit";"Ende Altersteilzeit";'
    '"Gesamtbrutto";"Steuerbrutto";"Steuerrechtl Abzüge";"KV/PV-Brutto";"RV/AV-Brutto";'
    '"SV-rechtl Abzüge";"Nettoverdienst";"Auszahlungsbetrag DM";"Auszahlungsbetrag Euro";'
    '"Pauschale LST";"Pauschale KiST";"Pauschaler Solz";"Gesamtbrutto-gesamt";'
    '"Steuerbrutto-gesamt";"SV-Brutto-gesamt";"VWL-gesamt";"Pfändung Rest";"Darlehen Rest";'
    '"Direktversicherung";"Anwesenheitstage";"Anwesenheitsstunden";"Bezahlte Stunden";'
    '"Krankheitstage";"Krankheitsstunden";"Zeitlohnstunden";"Fehl-Tage";"Fehl-Stunden";'
    '"Überstunden";"SV-AG-Anteil mtl";"Umlagebeiträge";"SV-AG-Anteil kumuliert";'
    '"Urlaubsstunden"'
)


def _sd_column_names() -> list[str]:
    """Parse the SD header once so tests can address columns by name."""
    return next(csv.reader(io.StringIO(SD_HEADER), delimiter=";", quotechar='"'))


def _build_sd_row(overrides: dict[str, str]) -> str:
    """Build a valid SD data line: one cell per SD_HEADER column, keys
    that aren't in `overrides` default to empty. Guarantees the row has
    the same column count as the header so parser assertions are stable."""
    cols = _sd_column_names()
    unknown = set(overrides) - set(cols)
    assert not unknown, f"Unknown SD columns referenced: {unknown}"
    cells = [overrides.get(col, "") for col in cols]
    return ";".join(cells)


# Synthesized test employee covering the fields the parser/import service
# promote to typed columns. No real PII.
ALICE_SD: dict[str, str] = {
    "Beraternummer": "1694291",
    "Mandantennummer": "99999",
    "Abrechnungsdatum": "01.04.2026",
    "Verarbeitungskennzeichen": "90G",
    "Personalnummer": "00001",
    "Familienname": "Test",
    "Vorname": "Alice",
    "Straße/Postfach": "Teststr. 1",
    "PLZ": "12345",
    "Ort": "Teststadt",
    "Geburtsdatum": "01.01.1990",
    "Geschlecht": "weiblich",
    "Versicherungsnummer": "10010190A000",
    "Familienstand": "ledig",
    "Staatsangehörigkeit": "deutsch",
    "IBAN": "DE00000000000000000001",
    "BIC": "TESTDE00",
    "Eintrittsdatum": "01.01.2025",
    "Datum erster Eintritt": "01.01.2025",
    "Austrittsdatum": "          ",
    "Personengruppenschlüssel": "101",
    "Arbeitnehmertyp": "Angestellter",
    "Beitragsgruppenschlüssel": "1111",
    "Krankenkasse": "29720865",
    "KK-Nr.": "4",
    "KK-Zusatzinformation": "AOK Test",
    "Finanzamtsnummer": "2480",
    "Identifikationsnummer": "99999999999",
    "Steuerklasse": "1",
    "Konfession AN": "ev",
    "Wöchentliche Arbeitszeit": "40,00",
    "Urlaubsanspruch pro Jahr": "20,00",
    "Urlaubsanspruch lfd Jahr": "26,00",
    "Resturlaub lfd Jahr": "26,00",
    "Gesamtbrutto": "2.000,00",
    "Steuerbrutto": "1.980,20",
    "Steuerrechtl Abzüge": "100,00",
    "KV/PV-Brutto": "1.800,00",
    "RV/AV-Brutto": "1.800,00",
    "SV-rechtl Abzüge": "380,00",
    "Nettoverdienst": "1.500,00",
    "Auszahlungsbetrag Euro": "1.500,00",
    "Pauschale LST": "0,00",
    "Pauschale KiST": "0,00",
    "Pauschaler Solz": "0,00",
    "Gesamtbrutto-gesamt": "8.000,00",
    "Steuerbrutto-gesamt": "7.920,00",
    "SV-Brutto-gesamt": "7.200,00",
    "Anwesenheitstage": "20,00",
    "Anwesenheitsstunden": "160,00",
    "Bezahlte Stunden": "160,00",
    "SV-AG-Anteil mtl": "380,00",
    "Umlagebeiträge": "15,00",
    "SV-AG-Anteil kumuliert": "1.520,00",
    "Pfändung Rest": "Verb.Insolvenz",  # real-world non-numeric seepage
}


def _full_sd_fixture(*rows: dict[str, str]) -> str:
    return SD_HEADER + "\n" + "\n".join(_build_sd_row(r) for r in rows) + "\n"


# --- SD file tests ---------------------------------------------------------


class TestParseSDFile:
    def test_basic_parse(self):
        rows = parse_sd_file(_full_sd_fixture(ALICE_SD).encode("utf-8"))
        assert len(rows) == 1

        alice = rows[0]
        assert alice.consultant_number == "1694291"
        assert alice.client_number == "99999"
        assert alice.reference_month == date(2026, 4, 1)
        assert alice.processing_code == "90G"
        assert alice.personnel_number == 1

    def test_raw_fields_preserved_by_column_name(self):
        rows = parse_sd_file(_full_sd_fixture(ALICE_SD))
        alice = rows[0]
        # Every header label is addressable, values intact.
        assert alice.raw_fields["Gesamtbrutto"] == "2.000,00"
        assert alice.raw_fields["KK-Zusatzinformation"] == "AOK Test"
        # Padded blank date cell preserved as raw.
        assert alice.raw_fields["Austrittsdatum"] == "          "

    def test_non_numeric_pfaendung_rest_preserved_raw(self):
        rows = parse_sd_file(_full_sd_fixture(ALICE_SD))
        assert rows[0].raw_fields["Pfändung Rest"] == "Verb.Insolvenz"

    def test_handles_cp1252_encoding(self):
        cp1252_bytes = _full_sd_fixture(ALICE_SD).encode("cp1252")
        rows = parse_sd_file(cp1252_bytes)
        assert len(rows) == 1
        # Umlaut survived the fallback decode.
        assert rows[0].raw_fields["Straße/Postfach"] == "Teststr. 1"

    def test_accepts_string_input(self):
        rows = parse_sd_file(_full_sd_fixture(ALICE_SD))
        assert len(rows) == 1

    def test_empty_file_raises(self):
        with pytest.raises(ValueError, match="Empty"):
            parse_sd_file(b"")

    def test_missing_required_column_raises(self):
        bad = '"foo";"bar"\n1;2\n'
        with pytest.raises(ValueError, match="missing required"):
            parse_sd_file(bad)


# --- LA file tests ---------------------------------------------------------


LA_FIXTURE = (
    '"Beraternummer";"Mandantennummer";"Abrechnungsdatum";"Zuordnungsdatum";'
    '"Verarbeitungskennzeichen";"Personalnummer";"Familienname";"Vorname";'
    '"Lohnart";"Bezeichnung";"Einheit der Menge";"Menge";"Faktor";'
    '"Prozentzuschlag";"ST";"SV";"GB";"Betrag"\n'
    # Current-month base salary
    '1694291;99999;01.04.2026;01.04.2026;"90G";00001;"Test";"Alice";'
    '2000;"Gehalt";"";;;;"L";"L";"J";2.000,00\n'
    # Retroactive correction for November 2025
    '1694291;99999;01.04.2026;01.11.2025;"605";00001;"Test";"Alice";'
    '2000;"Gehalt";"";;;;"L";"L";"J";-500,00\n'
    # Hours-based entry with quantity + factor
    '1694291;99999;01.04.2026;01.04.2026;"90G";00001;"Test";"Alice";'
    '1650;"Lohnfortzahlung, Std.";"Stunden";2,60;13,00;;"L";"L";"J";33,80\n'
)


class TestParseLAFile:
    def test_basic(self):
        rows = parse_la_file(LA_FIXTURE)
        assert len(rows) == 3

        base = rows[0]
        assert base.salary_type_code == 2000
        assert base.salary_type_name == "Gehalt"
        assert base.amount == pytest.approx(2000.00)
        assert base.reference_month == date(2026, 4, 1)
        assert base.allocation_date == date(2026, 4, 1)
        assert base.processing_code == "90G"

    def test_retroactive_allocation_date_differs(self):
        rows = parse_la_file(LA_FIXTURE)
        retro = rows[1]
        assert retro.allocation_date == date(2025, 11, 1)
        assert retro.reference_month == date(2026, 4, 1)
        assert retro.processing_code == "605"
        assert retro.amount == pytest.approx(-500.00)

    def test_quantity_and_factor_parsed(self):
        rows = parse_la_file(LA_FIXTURE)
        hours = rows[2]
        assert hours.unit == "Stunden"
        assert hours.quantity == pytest.approx(2.60)
        assert hours.factor == pytest.approx(13.00)
        assert hours.percentage is None


# --- both files consistent -------------------------------------------------


class TestBothFilesConsistent:
    def test_same_consultant_client_month(self):
        sd = parse_sd_file(_full_sd_fixture(ALICE_SD))
        la = parse_la_file(LA_FIXTURE)
        assert sd[0].consultant_number == la[0].consultant_number
        assert sd[0].client_number == la[0].client_number
        assert sd[0].reference_month == la[0].reference_month
