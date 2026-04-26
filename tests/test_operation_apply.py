"""Tests for the pending_operation worker.

Handlers are stubbed via monkeypatch so we don't hit real DATEV / Patti.
"""

from datetime import datetime, timezone

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

import app.services.operation_apply as oa
from app.core.datev_local_client import BridgeUnavailable, LocalDatevError
from app.db.base import Base
from app.models.employee import Employee
from app.models.pending_operation import PendingOperation


@pytest.fixture()
def db() -> Session:
    engine = create_engine("sqlite:///:memory:", future=True)
    Base.metadata.create_all(engine)
    with Session(engine) as session:
        e = Employee(client_id_path="1694291-99999", personnel_number=1)
        session.add(e)
        session.commit()
        yield session


def _make_op(db: Session, op_name: str, payload: dict | None = None) -> PendingOperation:
    return oa.enqueue(
        db,
        employee_id=1,
        op=op_name,
        payload=payload or {},
        requested_by_email="t@x",
    )


class TestApplyOne:
    def test_unknown_op_marks_error(self, db: Session, monkeypatch):
        # Bypass enqueue's KNOWN_OPS validation by inserting directly
        op = PendingOperation(
            employee_id=1, op="bogus.handler",
            payload={}, status="pending",
        )
        db.add(op); db.commit()

        outcome = oa.apply_one(db, op)
        db.commit()
        assert outcome == "error"
        assert op.status == "error"
        assert "unknown op" in op.last_error.lower()

    def test_success_marks_done(self, db: Session, monkeypatch):
        called: dict = {}

        def fake_handler(payload):
            called["payload"] = payload

        monkeypatch.setitem(oa.HANDLERS, "datev.update_address", fake_handler)
        op = _make_op(db, "datev.update_address",
                      {"personnel_number": 1, "address": {"city": "Test"}})
        db.commit()

        outcome = oa.apply_one(db, op)
        db.commit()

        assert outcome == "done"
        assert op.status == "done"
        assert op.attempts == 1
        assert op.last_error is None
        assert called["payload"]["personnel_number"] == 1

    def test_bridge_unavailable_reschedules(self, db: Session, monkeypatch):
        def fake_handler(_payload):
            raise BridgeUnavailable(503, "DATEV LuG nicht erreichbar", "url")

        monkeypatch.setitem(oa.HANDLERS, "datev.update_address", fake_handler)
        op = _make_op(db, "datev.update_address", {"personnel_number": 1, "address": {}})
        db.commit()

        outcome = oa.apply_one(db, op)
        db.commit()

        assert outcome == "retry"
        assert op.status == "pending"
        assert op.attempts == 1
        assert op.not_before is not None
        # SQLite stores naive datetimes; just check it's roughly in the future
        # (within an hour from now).
        nb_naive = op.not_before.replace(tzinfo=None)
        delta_seconds = (nb_naive - datetime.utcnow()).total_seconds()
        assert 0 < delta_seconds < 3600
        assert "transient" in op.last_error.lower()

    def test_datev_4xx_is_permanent(self, db: Session, monkeypatch):
        def fake_handler(_payload):
            raise LocalDatevError(400, {"error": "bad"}, "url")

        monkeypatch.setitem(oa.HANDLERS, "datev.update_address", fake_handler)
        op = _make_op(db, "datev.update_address", {"personnel_number": 1, "address": {}})
        db.commit()

        outcome = oa.apply_one(db, op)
        db.commit()

        assert outcome == "error"
        assert op.status == "error"
        assert "datev 400" in op.last_error

    def test_datev_5xx_other_than_503_retries(self, db: Session, monkeypatch):
        def fake_handler(_payload):
            raise LocalDatevError(500, "internal", "url")

        monkeypatch.setitem(oa.HANDLERS, "datev.update_address", fake_handler)
        op = _make_op(db, "datev.update_address", {"personnel_number": 1, "address": {}})
        db.commit()

        outcome = oa.apply_one(db, op)
        db.commit()

        assert outcome == "retry"
        assert op.status == "pending"

    def test_unexpected_exception_caught(self, db: Session, monkeypatch):
        def fake_handler(_payload):
            raise RuntimeError("oops")

        monkeypatch.setitem(oa.HANDLERS, "datev.update_address", fake_handler)
        op = _make_op(db, "datev.update_address", {"personnel_number": 1, "address": {}})
        db.commit()

        outcome = oa.apply_one(db, op)
        db.commit()

        assert outcome == "error"
        assert "RuntimeError" in op.last_error


class TestDrain:
    def test_drain_handles_mixed_outcomes(self, db: Session, monkeypatch):
        def good(_p): return None

        def bad(_p): raise BridgeUnavailable(503, "x", "u")

        monkeypatch.setitem(oa.HANDLERS, "datev.update_address", good)
        monkeypatch.setitem(oa.HANDLERS, "datev.update_account", bad)

        _make_op(db, "datev.update_address", {"personnel_number": 1, "address": {}})
        _make_op(db, "datev.update_account", {"personnel_number": 1, "account": {}})
        _make_op(db, "datev.update_address", {"personnel_number": 1, "address": {}})
        db.commit()

        result = oa.drain(db)
        assert result["total"] == 3
        assert result["done"] == 2
        assert result["retry"] == 1

    def test_drain_skips_not_before_in_future(self, db: Session, monkeypatch):
        def good(_p): return None
        monkeypatch.setitem(oa.HANDLERS, "datev.update_address", good)

        op = _make_op(db, "datev.update_address", {"personnel_number": 1, "address": {}})
        from datetime import timedelta
        # Offset-naive on SQLite, but the comparison in drain() works
        # because both sides come from the same DB.
        op.not_before = datetime.utcnow() + timedelta(hours=1)
        db.commit()

        result = oa.drain(db)
        assert result["total"] == 0
        assert op.status == "pending"


class TestEnqueue:
    def test_known_op_persists(self, db: Session):
        op = oa.enqueue(
            db, employee_id=1,
            op="datev.update_address",
            payload={"personnel_number": 1, "address": {}},
        )
        db.commit()
        assert op.id is not None
        assert op.status == "pending"
        assert op.attempts == 0

    def test_unknown_op_rejected(self, db: Session):
        with pytest.raises(ValueError, match="unknown"):
            oa.enqueue(db, employee_id=1, op="bogus", payload={})
