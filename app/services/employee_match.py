"""Match DATEV employees to Patti people.

DATEV is the master list (88 records on Daniel's tenant), Patti has
139 ``people`` rows mixing patients, caretaker users, and contacts.
There is no shared id, so we match on **first_name + last_name +
date_of_birth** which is unique enough for German payroll data
(collisions on identical name + DOB inside one company are vanishingly
rare).

Returns a tuple per Patti person ``(score, person_dict)`` where score
is 0..1, with 1.0 = exact case-insensitive match on all three fields.
Caller decides the threshold (we use ≥0.9 for auto-link, lower needs
manual confirmation).
"""

from __future__ import annotations

import unicodedata
from dataclasses import dataclass
from datetime import date
from typing import Iterable


def _normalize(s: str | None) -> str:
    """Casefold + strip diacritics + collapse whitespace.

    'Müller-Lüdenscheidt ' → 'mueller-luedenscheidt'.
    """
    if not s:
        return ""
    # Replace common German umlauts before strip-diacritics so Müller
    # and Mueller match.
    replacements = {"ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss",
                    "Ä": "ae", "Ö": "oe", "Ü": "ue"}
    for src, dst in replacements.items():
        s = s.replace(src, dst)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    return " ".join(s.lower().split())


def _parse_iso_date(raw: str | None) -> date | None:
    if not raw:
        return None
    if isinstance(raw, date):
        return raw
    s = raw.strip()
    if not s:
        return None
    # Patti uses ISO datetimes ("2003-10-31T00:00:00.000000Z" or just date)
    # DATEV ASCII would be "31.10.2003" but the local Payroll-3.1.4 API
    # returns ISO too.
    for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%Y-%m-%dT%H:%M:%S",
                "%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ"):
        try:
            from datetime import datetime
            return datetime.strptime(s.split(".")[0].rstrip("Z"), fmt.replace(".%f", "")).date()
        except ValueError:
            continue
    # Fallback: take first 10 chars of an ISO-ish string
    if len(s) >= 10 and s[4] == "-" and s[7] == "-":
        try:
            from datetime import datetime
            return datetime.strptime(s[:10], "%Y-%m-%d").date()
        except ValueError:
            return None
    return None


@dataclass(frozen=True)
class MatchKey:
    """Normalized identity for matching."""
    first_name: str
    last_name: str
    date_of_birth: date | None

    @classmethod
    def from_datev_employee(cls, e: dict) -> "MatchKey":
        # DATEV Payroll-3.1.4 employee: top-level first_name/surname; full
        # masterdata response includes more.
        return cls(
            first_name=_normalize(e.get("first_name")),
            last_name=_normalize(e.get("surname") or e.get("last_name")),
            date_of_birth=_parse_iso_date(
                e.get("date_of_birth") or (e.get("personal_data") or {}).get("date_of_birth")
            ),
        )

    @classmethod
    def from_patti_person(cls, p: dict) -> "MatchKey":
        return cls(
            first_name=_normalize(p.get("first_name")),
            last_name=_normalize(p.get("last_name")),
            date_of_birth=_parse_iso_date(p.get("born_at")),
        )


def score(a: MatchKey, b: MatchKey) -> float:
    """0..1 confidence that a and b describe the same person."""
    if not a.first_name or not a.last_name or not b.first_name or not b.last_name:
        return 0.0

    name_match = (a.first_name == b.first_name and a.last_name == b.last_name)
    if not name_match:
        # Soft variants: one side has compound first names ("Laura Therese")
        # and the other only the first part. Compare token sets.
        a_tokens = set(a.first_name.split()) | {a.last_name}
        b_tokens = set(b.first_name.split()) | {b.last_name}
        overlap = a_tokens & b_tokens
        if a.last_name == b.last_name and overlap >= {a.last_name}:
            # Last name matches, at least one first name token shared
            shared_firsts = (set(a.first_name.split()) & set(b.first_name.split()))
            if shared_firsts:
                # Partial match: 0.7 base
                base = 0.7
            else:
                return 0.0
        else:
            return 0.0
    else:
        base = 0.9  # full name match without DOB info

    # DOB confirms the match strongly. If both have DOB and they
    # disagree, we down-rank to zero (different people sharing a name).
    if a.date_of_birth and b.date_of_birth:
        if a.date_of_birth == b.date_of_birth:
            return 1.0
        return 0.0
    return base


def match_datev_to_patti(
    datev_employees: Iterable[dict],
    patti_people: Iterable[dict],
    *,
    threshold: float = 0.9,
) -> dict[int, dict]:
    """Return ``{personnel_number: matched_patti_person}`` for confident matches.

    Anything below ``threshold`` is omitted (caller should surface
    those for manual confirmation in the UI)."""
    patti_keys = [(MatchKey.from_patti_person(p), p) for p in patti_people]

    out: dict[int, dict] = {}
    for emp in datev_employees:
        ek = MatchKey.from_datev_employee(emp)
        if not ek.first_name or not ek.last_name:
            continue
        best_score = 0.0
        best_person: dict | None = None
        for pk, p in patti_keys:
            s = score(ek, pk)
            if s > best_score:
                best_score = s
                best_person = p
        if best_person is not None and best_score >= threshold:
            pnr = emp.get("id") or emp.get("personnel_number")
            try:
                pnr_int = int(str(pnr))
            except (ValueError, TypeError):
                continue
            out[pnr_int] = best_person
    return out
