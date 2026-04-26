"""Integration tests for the payroll import service.

Runs against an in-memory SQLite so we exercise the actual SQLAlchemy
models and the upsert path without needing Postgres.
"""

from datetime import date

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from app.db.base import Base
# Registering the models on the metadata for create_all():
from app.models.payroll_statement import (  # noqa: F401
    PayrollLineItem,
    PayrollStatement,
)
from app.services.payroll_import import (
    PayrollImportError,
    import_lug_ascii,
)

from tests.test_lug_ascii_parser import (
    ALICE_SD,
    LA_FIXTURE,
    _full_sd_fixture,
)


@pytest.fixture()
def db() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


CLIENT_PATH = "1694291-99999"


class TestImportLugAscii:
    def test_creates_statement_and_line_items(self, db: Session):
        result = import_lug_ascii(
            db,
            sd_bytes=_full_sd_fixture(ALICE_SD).encode("utf-8"),
            la_bytes=LA_FIXTURE.encode("utf-8"),
            expected_client_id_path=CLIENT_PATH,
            imported_by_email="tester@example.com",
        )

        assert result.statements_created == 1
        assert result.statements_updated == 0
        assert result.line_items_written == 3
        assert result.reference_month == date(2026, 4, 1)
        assert result.consultant_number == "1694291"
        assert result.client_number == "99999"

        rows = db.execute(select(PayrollStatement)).scalars().all()
        assert len(rows) == 1
        stmt = rows[0]
        assert stmt.personnel_number == 1
        assert stmt.surname == "Test"
        assert stmt.first_name == "Alice"
        assert float(stmt.gross_total) == pytest.approx(2000.00)
        assert float(stmt.net_income) == pytest.approx(1500.00)
        assert stmt.iban == "DE00000000000000000001"
        assert stmt.tax_class == 1
        assert stmt.denomination == "ev"
        assert stmt.health_insurer_name == "AOK Test"
        assert stmt.health_insurer_number == "29720865"
        assert stmt.imported_by_email == "tester@example.com"
        assert stmt.import_batch_id is not None
        # Full SD row kept for auditability
        assert stmt.raw_sd["Gesamtbrutto"] == "2.000,00"

    def test_line_items_flag_retroactive(self, db: Session):
        import_lug_ascii(
            db,
            sd_bytes=_full_sd_fixture(ALICE_SD).encode("utf-8"),
            la_bytes=LA_FIXTURE.encode("utf-8"),
            expected_client_id_path=CLIENT_PATH,
            imported_by_email=None,
        )

        items = db.execute(select(PayrollLineItem)).scalars().all()
        assert len(items) == 3

        retro = next(i for i in items if i.processing_code == "605")
        assert retro.allocation_date == date(2025, 11, 1)
        assert float(retro.amount) == pytest.approx(-500.00)
        assert retro.is_retroactive is True

        current = next(i for i in items if i.salary_type_code == 2000 and i.processing_code == "90G")
        assert current.is_retroactive is False

    def test_reimport_replaces_line_items(self, db: Session):
        """Re-uploading the same month must leave us with a single statement
        (not duplicates) and the line items freshly reset — otherwise monthly
        re-runs would pile up stale corrections."""
        payload = dict(
            sd_bytes=_full_sd_fixture(ALICE_SD).encode("utf-8"),
            la_bytes=LA_FIXTURE.encode("utf-8"),
            expected_client_id_path=CLIENT_PATH,
            imported_by_email=None,
        )
        import_lug_ascii(db, **payload)
        second = import_lug_ascii(db, **payload)

        assert second.statements_created == 0
        assert second.statements_updated == 1
        assert second.line_items_written == 3

        # Only one statement, exactly 3 line items (not 6).
        assert db.execute(select(PayrollStatement)).scalars().all().__len__() == 1
        assert db.execute(select(PayrollLineItem)).scalars().all().__len__() == 3

    def test_rejects_wrong_tenant(self, db: Session):
        with pytest.raises(PayrollImportError, match="Mandant"):
            import_lug_ascii(
                db,
                sd_bytes=_full_sd_fixture(ALICE_SD).encode("utf-8"),
                la_bytes=LA_FIXTURE.encode("utf-8"),
                expected_client_id_path="9999999-12345",  # wrong tenant
                imported_by_email=None,
            )

    def test_rejects_mismatched_files(self, db: Session):
        # LA file for a different run (month): fabricate one.
        la_wrong_month = LA_FIXTURE.replace("01.04.2026", "01.05.2026")
        with pytest.raises(PayrollImportError, match="selben Abrechnungslauf"):
            import_lug_ascii(
                db,
                sd_bytes=_full_sd_fixture(ALICE_SD).encode("utf-8"),
                la_bytes=la_wrong_month.encode("utf-8"),
                expected_client_id_path=CLIENT_PATH,
                imported_by_email=None,
            )

    def test_la_rows_without_matching_sd_row_are_skipped_with_warning(self, db: Session):
        # LA fixture has pnr 00001 only; build an SD with a different pnr.
        other = dict(ALICE_SD)
        other["Personalnummer"] = "00002"
        result = import_lug_ascii(
            db,
            sd_bytes=_full_sd_fixture(other).encode("utf-8"),
            la_bytes=LA_FIXTURE.encode("utf-8"),
            expected_client_id_path=CLIENT_PATH,
            imported_by_email=None,
        )
        assert result.statements_created == 1
        assert result.line_items_written == 0
        assert result.skipped_la_rows == 3
        assert any("LA-Zeilen übersprungen" in w for w in result.warnings)
