"""Tests for the DATEV<->Patti name+DOB matching heuristic."""

from datetime import date

from app.services.employee_match import (
    MatchKey,
    _normalize,
    match_datev_to_patti,
    score,
)


class TestNormalize:
    def test_german_umlauts(self):
        assert _normalize("Müller") == "mueller"
        assert _normalize("Schäffer") == "schaeffer"
        assert _normalize("Höfer") == "hoefer"
        assert _normalize("Weiß") == "weiss"

    def test_strips_diacritics(self):
        assert _normalize("Café") == "cafe"
        assert _normalize("Núñez") == "nunez"

    def test_collapses_whitespace(self):
        assert _normalize("  Laura   Therese  ") == "laura therese"

    def test_lowercase(self):
        assert _normalize("MUELLER") == "mueller"

    def test_empty(self):
        assert _normalize("") == ""
        assert _normalize(None) == ""


class TestScore:
    def _key(self, fn, ln, dob):
        return MatchKey(_normalize(fn), _normalize(ln), dob)

    def test_full_match_with_dob(self):
        a = self._key("Anna", "Müller", date(1990, 1, 1))
        b = self._key("Anna", "Mueller", date(1990, 1, 1))
        assert score(a, b) == 1.0

    def test_full_name_no_dob(self):
        a = self._key("Anna", "Müller", None)
        b = self._key("Anna", "Mueller", None)
        # Both DOBs missing -> base score 0.9
        assert score(a, b) == 0.9

    def test_dob_disagreement_kills_match(self):
        a = self._key("Anna", "Müller", date(1990, 1, 1))
        b = self._key("Anna", "Müller", date(1985, 5, 5))
        assert score(a, b) == 0.0

    def test_compound_first_name_partial(self):
        # DATEV has "Laura Therese", Patti has "Laura"
        a = self._key("Laura Therese", "Arnemann", date(1994, 8, 4))
        b = self._key("Laura", "Arnemann", date(1994, 8, 4))
        # Token overlap on first name + last name match + DOB match
        assert score(a, b) >= 0.9

    def test_different_last_name_no_match(self):
        a = self._key("Anna", "Müller", date(1990, 1, 1))
        b = self._key("Anna", "Schmidt", date(1990, 1, 1))
        assert score(a, b) == 0.0

    def test_missing_first_name_no_match(self):
        a = self._key("", "Müller", None)
        b = self._key("Anna", "Müller", None)
        assert score(a, b) == 0.0


class TestMatch:
    def test_basic(self):
        datev = [
            {"id": 1, "first_name": "Anna", "surname": "Müller",
             "personal_data": {"date_of_birth": "1990-01-01"}},
            {"id": 2, "first_name": "Boris", "surname": "Schmidt",
             "personal_data": {"date_of_birth": "1985-06-15"}},
        ]
        patti = [
            # Anna Müller: should match #1 with score 1.0
            {"id": 100, "first_name": "Anna", "last_name": "Mueller",
             "born_at": "1990-01-01T00:00:00"},
            # Charlie: in Patti only, should match nothing
            {"id": 101, "first_name": "Charlie", "last_name": "Patient",
             "born_at": "2000-03-03T00:00:00"},
            # Boris with wrong DOB: should NOT match #2
            {"id": 102, "first_name": "Boris", "last_name": "Schmidt",
             "born_at": "1980-06-15T00:00:00"},
        ]
        matches = match_datev_to_patti(datev, patti, threshold=0.9)
        assert 1 in matches
        assert matches[1]["id"] == 100
        # Boris should NOT match because DOB disagrees
        assert 2 not in matches

    def test_below_threshold_excluded(self):
        # Only base 0.9 (no DOB) — at threshold of 0.95 should be excluded
        datev = [{"id": 1, "first_name": "Anna", "surname": "Müller"}]
        patti = [{"id": 100, "first_name": "Anna", "last_name": "Mueller",
                  "born_at": None}]
        assert match_datev_to_patti(datev, patti, threshold=0.95) == {}
        # But at 0.9 default threshold should match
        assert 1 in match_datev_to_patti(datev, patti, threshold=0.9)

    def test_top_level_date_of_birth(self):
        # Local Payroll-3.1.4 list endpoint may put dob at top level
        datev = [{"id": 5, "first_name": "Sophia",
                  "surname": "Grzebel", "date_of_birth": "1999-10-14"}]
        patti = [{"id": 200, "first_name": "Sophia", "last_name": "Grzebel",
                  "born_at": "1999-10-14T00:00:00"}]
        assert match_datev_to_patti(datev, patti)[5]["id"] == 200
