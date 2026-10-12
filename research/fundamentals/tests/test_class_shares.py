"""
Per-class share counts (ПНЛ1, 12.10.2026): the coefficient table share_classes.csv
and the economic sum in build_panel._class_records. The fixtures are the real cover
pages of the 10-Qs for the quarter ended 2026-06-30 (BRK 0001193125-26-341032,
V 0001403161-26-000104); the table is the REAL one, so dropping or flattening its
BRK.B / V rows makes these tests fail (гейт Г4).

    python -m pytest research/fundamentals/tests/test_class_shares.py -q
"""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from research.fundamentals import build_panel as bp  # noqa: E402

A, B, C = "us-gaap:CommonClassAMember", "us-gaap:CommonClassBMember", "us-gaap:CommonClassCMember"
B1, B2, B3 = "v:CommonClassB1Member", "v:CommonClassB2Member", "v:CommonClassB3Member"


def _entry(ecso, eps=(), ni=(), wbas=(), epsb=(), period="2026-06-30", filed="2026-08-01"):
    return {"form": "10-Q", "filed": filed, "period": period, "fy": "2026", "fp": "Q2",
            "instance": "x_htm.xml", "ecso": [list(r) for r in ecso], "eps": [list(r) for r in eps],
            "epsb": [list(r) for r in epsb], "ni": [list(r) for r in ni], "wbas": [list(r) for r in wbas],
            "wdil": []}


@pytest.fixture(scope="module")
def table():
    return bp.load_class_table()


def test_brk_b_counts_class_a_as_1500_b(table):
    e = _entry([(A, "2026-07-29", 488450.0), (B, "2026-07-29", 1408035161.0)])
    recs = bp._class_records("BRK.B", table["BRK.B"], {"acc": e}, {})
    assert len(recs) == 1
    assert recs[0]["val"] == 488450 * 1500 + 1408035161          # 2 140 710 161 B-equivalents
    assert recs[0]["end"] == "2026-07-29"
    # the naive class sum (the bug this table exists for) is a third short
    assert 1408523611 / recs[0]["val"] < 0.70


def test_visa_converts_b_and_c_by_the_filings_own_eps_ratio(table):
    nine = ("2025-10-01", "2026-06-30")
    eps = [(A, *nine, 9.14), (B1, *nine, 14.17), (B2, *nine, 13.83), (B3, *nine, 13.76), (C, *nine, 36.55)]
    cover = [(A, "2026-07-21", 1704112694.0), (B1, "2026-07-21", 2180148.0), (B2, "2026-07-21", 486669.0),
             (B3, "2026-07-21", 60589871.0), (C, "2026-07-21", 17059152.0)]
    recs = bp._class_records("V", table["V"], {"acc": _entry(cover, eps=eps)}, {})
    want = (1704112694 + 2180148 * 14.17 / 9.14 + 486669 * 13.83 / 9.14
            + 60589871 * 13.76 / 9.14 + 17059152 * 36.55 / 9.14)
    assert recs[0]["val"] == round(want)                           # ~1.87 bn class-A equivalents
    assert recs[0]["val"] / 1784428534 > 1.04                      # vs the naive class sum


def test_unknown_member_with_shares_is_refused_not_guessed(table):
    stats: dict = {}
    e = _entry([(A, "2026-07-29", 488450.0), ("x:NewClassMember", "2026-07-29", 5e6)])
    assert bp._class_records("BRK.B", table["BRK.B"], {"acc": e}, stats) == []
    assert ("BRK.B", "x:NewClassMember") in stats["class_unknown_member"]


def test_guard_falls_back_to_company_weighted_count():
    # ACN shape: the cover count includes the Class A shares the registrant holds.
    spec = {"coef": {A: 1.0, "acn:ClassXMember": 0.0}, "unit": A, "eps": None, "weighted": True}
    per = ("2025-09-01", "2026-05-31")
    e = _entry([(A, "2026-06-12", 667.5e6), ("acn:ClassXMember", "2026-06-12", 0.3e6)],
               eps=[("", *per, 10.0)], ni=[(*per, 6.2e9)], wbas=[("", *per, 619.0e6)], period="2026-05-31")
    stats: dict = {}
    recs = bp._class_records("ACN", spec, {"acc": e}, stats)
    assert recs[0]["val"] == 619_000_000                           # exact weighted count, agrees with NI/EPS
    assert stats["class_fallback"][0][0] == "ACN"
    # the same breach without role=weighted is logged and the cover count is kept
    stats2: dict = {}
    recs2 = bp._class_records("ACN", {**spec, "weighted": False}, {"acc": e}, stats2)
    assert recs2[0]["val"] == 667_500_000                          # class X counts 0
    assert stats2["class_guard_breach"][0][0] == "ACN" and not stats2.get("class_fallback")


def test_acn_is_the_declared_weighted_name(table):
    assert table["ACN"]["weighted"] and not table["META"]["weighted"]


def test_guard_passes_and_up_c_voting_class_counts_zero():
    # TKO shape: class B votes but has no claim on the parent's net income.
    spec = {"coef": {A: 1.0, B: 0.0}, "unit": A, "eps": None}
    per = ("2026-01-01", "2026-06-30")
    e = _entry([(A, "2026-07-25", 73.11e6), (B, "2026-07-25", 116.2e6)],
               eps=[("", *per, 2.50)], ni=[(*per, 190.9e6)])
    stats: dict = {}
    recs = bp._class_records("TKO", spec, {"acc": e}, stats)
    assert recs[0]["val"] == 73_110_000
    assert not stats.get("class_fallback")
