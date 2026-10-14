"""
Synthetic tests for the cash-flow D&A tag by name (ПНЛ4д): in a filing carrying both
DepreciationDepletionAndAmortization and DepreciationAndAmortization only the cash-flow
statement's one stays (build_panel._DA_CF_*), and G6a's DA_NOTE check. No cache needed.

    python -m pytest research/fundamentals/tests/test_da_cash_flow.py -q
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from research.fundamentals import build_panel, validate_gates  # noqa: E402

DDA, DA = "DepreciationDepletionAndAmortization", "DepreciationAndAmortization"


def _f(start, end, val, filed, accn):
    return {"start": start, "end": end, "val": val, "filed": filed, "accn": accn,
            "form": "10-Q", "fy": 2026, "fp": "Q2"}


def _facts(**tags):
    return {"us-gaap": {t: {"units": {"USD": list(v)}} for t, v in tags.items()}}


# one 10-Q: the cash flow carries the YTD, the note a discrete quarter and the same YTD
_CF_YTD = _f("2026-01-01", "2026-06-30", 2228.0, "2026-08-05", "q2")
_NOTE_YTD = _f("2026-01-01", "2026-06-30", 442.0, "2026-08-05", "q2")
_NOTE_Q = _f("2026-04-01", "2026-06-30", 222.0, "2026-08-05", "q2")


def _da(ticker, facts):
    stats = {"missing_concept": {}}
    rows = build_panel._extract_company(ticker, "0000000001", facts, stats)
    return sorted((r[4], r[5], r[10]) for r in rows if r[2] == "depreciation_amortization"), stats


def test_dda_name_drops_the_note_on_every_key_of_the_filing():
    got, stats = _da("AMGN", _facts(**{DDA: [_CF_YTD], DA: [_NOTE_YTD, _NOTE_Q]}))
    assert got == [("2026-01-01", "2026-06-30", 2228.0)]
    assert stats["da_cf_dropped"] == {"AMGN": 2}


def test_danda_name_takes_danda_on_the_shared_key():
    got, _ = _da("CRM", _facts(**{DDA: [_NOTE_YTD, _NOTE_Q], DA: [_CF_YTD]}))
    assert got == [("2026-01-01", "2026-06-30", 2228.0)]


def test_name_outside_the_table_keeps_the_old_union():
    got, stats = _da("ZZZ", _facts(**{DDA: [_NOTE_YTD], DA: [_CF_YTD, _NOTE_Q]}))
    assert got == [("2026-01-01", "2026-06-30", 442.0), ("2026-04-01", "2026-06-30", 222.0)]
    assert "da_cf_dropped" not in stats


def test_a_filing_with_only_the_note_tag_keeps_it():
    alone = _f("2025-01-01", "2025-12-31", 900.0, "2026-02-13", "k25")   # no cash-flow tag in k25
    got, _ = _da("AMGN", _facts(**{DDA: [_CF_YTD], DA: [_NOTE_Q, alone]}))
    assert got == [("2025-01-01", "2025-12-31", 900.0), ("2026-01-01", "2026-06-30", 2228.0)]


def test_ben_switches_by_filing_date():
    old_cf = _f("2018-10-01", "2019-09-30", 70.0, "2019-11-12", "k19")
    old_note = _f("2018-10-01", "2019-09-30", 50.0, "2019-11-12", "k19")
    new_cf = _f("2019-10-01", "2020-09-30", 80.0, "2020-11-23", "k20")
    new_note = _f("2019-10-01", "2020-09-30", 60.0, "2020-11-23", "k20")
    got, _ = _da("BEN", _facts(**{DA: [old_cf, new_note], DDA: [old_note, new_cf]}))
    assert got == [("2018-10-01", "2019-09-30", 70.0), ("2019-10-01", "2020-09-30", 80.0)]


def test_table_is_well_formed():
    assert not (build_panel._DA_CF_DDA & build_panel._DA_CF_DANDA)
    named = build_panel._DA_CF_DDA | build_panel._DA_CF_DANDA | set(build_panel._DA_CF_SWITCH)
    assert not named & {"PNC", "COF", "DVA", "KEY", "MCK", "WTW"}   # Р2; own cash-flow tags
    assert not named & set(build_panel._DA_NA)
    assert len(build_panel._DA_CF_DDA) == 43 and len(build_panel._DA_CF_DANDA) == 20
    assert {"PSX", "IT"} <= build_panel._DA_CF_DANDA   # ПНЛ4е


def test_g6a_da_note_fails_the_old_rule(tmp_path):
    facts = _facts(**{DDA: [_CF_YTD], DA: [_NOTE_YTD, _NOTE_Q]})
    (tmp_path / "CIK0000000001.json").write_text(json.dumps({"facts": facts}), encoding="utf-8")
    row = dict(ticker="AMGN", cik=1, concept="depreciation_amortization", unit="USD", form="10-Q", fy=2026, fp="Q2")

    def frame(recs):
        return pd.DataFrame([dict(row, period_start=pd.Timestamp(s), period_end=pd.Timestamp(e),
                                  filed=pd.Timestamp("2026-08-05"), value=v) for s, e, v in recs])

    new, _ = _da("AMGN", facts)
    assert validate_gates.g6a_da_full_line(frame(new), tmp_path) == []
    # the old rule: DDA wins the shared key, the note's discrete quarter rides along
    _used, old = build_panel._union_chain(facts, "us-gaap", [DDA, DA], "USD")
    fails = validate_gates.g6a_da_full_line(frame([(r["start"], r["end"], r["val"]) for r in old]), tmp_path)
    assert len(fails) == 1 and fails[0].startswith("DA_NOTE AMGN: 1 rows")
