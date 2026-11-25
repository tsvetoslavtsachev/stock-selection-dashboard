"""
25.11.2026: DOW capex (PaymentsToAcquireMachineryAndEquipment as a proven continuation tag)
and CHD D&A (Depreciation + AdjustmentForAmortization, a second proven sum, CHD only).
Synthetic, no cache.

    python -m pytest research/fundamentals/tests/test_g7_chains.py -q
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from research.fundamentals import build_panel  # noqa: E402


def _q(end, start, val, filed, accn="a1", form="10-Q"):
    return {"end": end, "start": start, "val": val, "filed": filed, "accn": accn,
            "form": form, "fy": 2026, "fp": "Q1"}


def _facts(**tags):
    return {"us-gaap": {t: {"units": {"USD": recs}} for t, recs in tags.items()}}


_CAPEX = [_q("2025-09-30", "2025-01-01", 900.0, "2025-10-30", "o1"),
          _q("2026-03-31", "2026-01-01", 503.0, "2026-04-24", "o2")]


def test_dow_capex_machinery_tag_continues_after_proof():
    machinery = [_q("2025-09-30", "2025-01-01", 900.0, "2026-10-23", "n1"),   # re-filed comparative
                 _q("2026-03-31", "2026-01-01", 503.0, "2026-10-23", "n1"),
                 _q("2026-09-30", "2026-01-01", 1841.0, "2026-10-23", "n1")]
    facts = _facts(PaymentsToAcquireProductiveAssets=_CAPEX, PaymentsToAcquireMachineryAndEquipment=machinery)
    recs, admitted = build_panel._continue_with_fallbacks(facts, "capex", list(_CAPEX), "DOW")
    assert admitted == ["PaymentsToAcquireMachineryAndEquipment"]
    assert [(r["end"], r["val"]) for r in recs][-1] == ("2026-09-30", 1841.0) and len(recs) == 3


def test_capex_machinery_refused_when_it_is_only_a_part():
    part = [_q("2025-09-30", "2025-01-01", 600.0, "2026-10-23", "n1"),        # the DAL shape: a component
            _q("2026-03-31", "2026-01-01", 300.0, "2026-10-23", "n1"),
            _q("2026-09-30", "2026-01-01", 1841.0, "2026-10-23", "n1")]
    facts = _facts(PaymentsToAcquireProductiveAssets=_CAPEX, PaymentsToAcquireMachineryAndEquipment=part)
    recs, admitted = build_panel._continue_with_fallbacks(facts, "capex", list(_CAPEX), "DOW")
    assert admitted == [] and len(recs) == 2


_DDA = [_q("2025-09-30", "2025-01-01", 190.0, "2025-10-30", "o1"),
        _q("2026-03-31", "2026-01-01", 62.9, "2026-05-01", "o2")]


def _chd_facts(adjust_q1=None):
    dep = [_q("2025-09-30", "2025-01-01", 70.0, "2025-10-30", "o1"),
           _q("2026-03-31", "2026-01-01", 21.6, "2026-05-01", "o2"),
           _q("2026-09-30", "2026-01-01", 63.6, "2026-10-30", "n1")]
    adj = [_q("2025-09-30", "2025-01-01", 120.0, "2025-10-30", "o1"),
           _q("2026-03-31", "2026-01-01", adjust_q1 or 41.3, "2026-05-01", "o2"),
           _q("2026-09-30", "2026-01-01", 129.2, "2026-10-30", "n1")]
    amort = [_q("2025-09-30", "2025-01-01", 55.0, "2025-10-30", "o1"),        # the other, different number
             _q("2026-03-31", "2026-01-01", 55.4, "2026-05-01", "o2"),
             _q("2026-09-30", "2026-01-01", 104.9, "2026-10-30", "n1")]
    return _facts(DepreciationDepletionAndAmortization=_DDA, Depreciation=dep,
                  AdjustmentForAmortization=adj, AmortizationOfIntangibleAssets=amort)


def test_chd_sum_with_adjustment_for_amortization_is_proven_and_not_shadowed():
    recs, admitted = build_panel._continue_with_fallbacks(
        _chd_facts(), "depreciation_amortization", list(_DDA), "CHD")
    assert admitted == ["Depreciation+AdjustmentForAmortization"]
    assert recs[-1]["end"] == "2026-09-30" and round(recs[-1]["val"], 1) == 192.8   # 63.6 + 129.2


def test_chd_sum_not_admitted_for_other_names():
    _recs, admitted = build_panel._continue_with_fallbacks(
        _chd_facts(), "depreciation_amortization", list(_DDA), "ECL")
    assert admitted == []


def test_chd_sum_refused_when_a_shared_period_differs():
    recs, admitted = build_panel._continue_with_fallbacks(
        _chd_facts(adjust_q1=60.0), "depreciation_amortization", list(_DDA), "CHD")
    assert admitted == [] and len(recs) == 2
