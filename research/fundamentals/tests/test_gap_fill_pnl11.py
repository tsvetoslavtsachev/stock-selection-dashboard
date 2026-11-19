"""
Synthetic tests for the ПНЛ11 gap rules in build_panel (Р4-1 ... Р4-6, В1, В2 of
19.11.2026), positive and negative. No cache needed.

    python -m pytest research/fundamentals/tests/test_gap_fill_pnl11.py -q
"""

from __future__ import annotations

import calendar
import sys
from datetime import date, timedelta
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from research.fundamentals import build_panel as bp  # noqa: E402

ORD = "PaymentsOfOrdinaryDividends"
ALT_NI = "NetIncomeLossAvailableToCommonStockholdersBasic"


def _filed(end: str, days: int) -> str:
    return (date.fromisoformat(end) + timedelta(days=days)).isoformat()


def _q(y: int, n: int, val: float, accn: str | None = None) -> dict:
    """A 3-month 10-Q fact for calendar quarter n of year y."""
    m0, m1 = 3 * (n - 1) + 1, 3 * n
    start, end = date(y, m0, 1).isoformat(), date(y, m1, calendar.monthrange(y, m1)[1]).isoformat()
    return {"start": start, "end": end, "val": val, "filed": _filed(end, 40),
            "accn": accn or f"q{y}{n}", "form": "10-Q", "fy": y, "fp": f"Q{n}"}


def _fy(y: int, val: float, accn: str | None = None) -> dict:
    """A 12-month 10-K fact for calendar year y."""
    end = f"{y}-12-31"
    return {"start": f"{y}-01-01", "end": end, "val": val, "filed": _filed(end, 60),
            "accn": accn or f"k{y}", "form": "10-K", "fy": y, "fp": "FY"}


def _facts(usd: dict | None = None, per_share: dict | None = None, shares: dict | None = None) -> dict:
    g: dict = {}
    for unit, tags in (("USD", usd or {}), ("USD/shares", per_share or {}), ("shares", shares or {})):
        for t, recs in tags.items():
            g.setdefault(t, {"units": {}})["units"][unit] = recs
    return {"us-gaap": g}


def _quarters(years, val=lambda y, n: 100.0 * y + n, skip=()):
    return [_q(y, n, val(y, n)) for y in years for n in (1, 2, 3, 4) if (y, n) not in skip]


def _rules(recs):
    return sorted({(r.get("_rule"), r.get("_tag")) for r in recs if "_rule" in r})


# ---------------------------------------------------------------------------
# Р4-1: PaymentsOfOrdinaryDividends without the identity proof
# ---------------------------------------------------------------------------
def test_r41_ordinary_admitted_without_identity_when_filer_has_no_chain_and_dps():
    # CL shape: no chain tag; paid != DPS x shares by 10% (identity fails), DPS in 4 years
    years = range(2022, 2026)
    facts = _facts(usd={ORD: [_fy(y, 1100.0) for y in years]},
                   per_share={"CommonStockDividendsPerShareDeclared": [_fy(y, 2.0) for y in years]},
                   shares={bp._WASO_TAG: [_fy(y, 500.0) for y in years]})
    out, admitted = bp._continue_with_fallbacks(facts, "dividends_paid", [])
    assert admitted == [ORD]
    assert len(out) == 4 and _rules(out) == [("Р4-1", ORD)]


def test_r41_refused_without_dividend_per_share():
    # COHR shape: the ordinary tag carries preferred payments, no DPS for the common
    facts = _facts(usd={ORD: [_fy(y, 50.0) for y in range(2022, 2026)]})
    out, admitted = bp._continue_with_fallbacks(facts, "dividends_paid", [])
    assert out == [] and admitted == []


def test_filer_with_both_tags_gets_no_unproven_ordinary_rows():
    # KMI shape: the chain has annual facts; the ordinary tag is another number and the
    # chain lacks FY2024 -> the gap proof fails, no ordinary row fills it
    chain = [_fy(y, 1000.0) for y in (2021, 2022, 2023, 2025)]
    facts = _facts(usd={"PaymentsOfDividends": chain, ORD: [_fy(y, 2000.0) for y in range(2021, 2026)]},
                   per_share={"CommonStockDividendsPerShareDeclared": [_fy(y, 1.0) for y in range(2021, 2026)]})
    _, recs = bp._union_chain(facts, "us-gaap", bp._CONCEPTS["dividends_paid"][1], "USD")
    out, _ = bp._continue_with_fallbacks(facts, "dividends_paid", recs)
    assert not [r for r in out if r.get("_tag") == ORD]
    assert len(out) == 4


# ---------------------------------------------------------------------------
# Р4-2: gaps across the whole series, proven from both sides; В1
# ---------------------------------------------------------------------------
def _ni_case(alt_val=lambda y, n: 100.0 * y + n, skip=((2020, 1), (2020, 2), (2020, 3), (2020, 4)),
             after=(2021,)):
    ni = _quarters((2019, 2020, *after), skip=skip)
    alt = [_q(y, n, alt_val(y, n)) for y in (2019, 2020, *after) for n in (1, 2, 3, 4)]
    facts = _facts(usd={bp._NI_TAG: ni, ALT_NI: alt})
    chain, _ = bp._extract_net_income(facts)
    return bp._continue_with_fallbacks(facts, "net_income", chain)


def test_r42_middle_gap_filled_when_both_sides_agree():
    out, admitted = _ni_case()
    assert admitted == [ALT_NI]
    filled = [r for r in out if r.get("_rule") == "Р4-2 по средата"]
    assert sorted(r["end"][:7] for r in filled) == ["2020-03", "2020-06", "2020-09", "2020-12"]


def test_r42_refused_when_the_tag_differs_before_the_gap():
    # EIX shape: before the gap the alternative tag is another line (after preferred)
    out, admitted = _ni_case(alt_val=lambda y, n: (100.0 * y + n) * (0.8 if y == 2019 else 1.0))
    assert admitted == [] and not [r for r in out if "_rule" in r]


def test_r42_refused_when_the_tag_differs_after_the_gap():
    out, _ = _ni_case(alt_val=lambda y, n: (100.0 * y + n) * (1.1 if y == 2021 else 1.0))
    assert not [r for r in out if r.get("_rule", "").startswith("Р4-2")]


def test_b1_one_shared_period_after_the_gap_is_enough_when_the_chain_has_no_more():
    # KIM / DVN shape: after the gap the chain has a single quarter
    out, _ = _ni_case(skip=((2020, 1), (2020, 2), (2020, 3)), after=())
    assert sorted(r["end"][:7] for r in out if r.get("_rule") == "Р4-2 по средата") == \
        ["2020-03", "2020-06", "2020-09"]


def test_b1_single_period_after_the_gap_must_still_agree():
    out, _ = _ni_case(skip=((2020, 1), (2020, 2), (2020, 3)), after=(),
                      alt_val=lambda y, n: (100.0 * y + n) * (1.02 if (y, n) == (2020, 4) else 1.0))
    assert not [r for r in out if r.get("_rule") == "Р4-2 по средата"]


# ---------------------------------------------------------------------------
# Р4-4: the declared tags continue a dead paid series forward (annual proof)
# ---------------------------------------------------------------------------
def _declared_case(factor=1.0):
    paid = [_fy(y, 300.0 + y) for y in range(2019, 2023)]
    declared = [_fy(y, (300.0 + y) * (factor if y <= 2022 else 1.0)) for y in range(2019, 2026)]
    facts = _facts(usd={"PaymentsOfDividends": paid, "DividendsCommonStockCash": declared})
    _, recs = bp._union_chain(facts, "us-gaap", bp._CONCEPTS["dividends_paid"][1], "USD")
    return bp._continue_with_fallbacks(facts, "dividends_paid", recs)


def test_r44_declared_continues_forward_only():
    out, admitted = _declared_case()
    assert admitted == ["DividendsCommonStockCash"]
    assert sorted(r["end"] for r in out if r.get("_rule") == "Р4-4") == \
        ["2023-12-31", "2024-12-31", "2025-12-31"]


def test_r44_refused_when_declared_differs_from_paid():
    # PCG / TRGP shape: the declared figure is not the paid line
    out, admitted = _declared_case(factor=1.05)
    assert admitted == [] and len(out) == 4


# ---------------------------------------------------------------------------
# Р4-3: dividend per share x weighted shares when there is no cash fact at all
# ---------------------------------------------------------------------------
def test_r43_product_is_the_series_without_any_cash_fact():
    # FRT / CRH shape
    years = range(2022, 2026)
    facts = _facts(per_share={"CommonStockDividendsPerShareCashPaid": [_fy(y, 4.25) for y in years]},
                   shares={bp._WASO_TAG: [_fy(y, 80.0) for y in years]})
    out, admitted = bp._continue_with_fallbacks(facts, "dividends_paid", [])
    assert admitted == [bp._PRODUCT_TAG]
    assert [r["val"] for r in out] == [340] * 4 and _rules(out) == [("Р4-3", bp._PRODUCT_TAG)]


def test_r43_product_needs_shares_from_the_same_filing():
    years = range(2022, 2026)
    facts = _facts(per_share={"CommonStockDividendsPerShareCashPaid": [_fy(y, 4.25) for y in years]},
                   shares={bp._WASO_TAG: [_fy(y, 80.0, accn=f"other{y}") for y in years]})
    out, admitted = bp._continue_with_fallbacks(facts, "dividends_paid", [])
    assert out == [] and admitted == []


# ---------------------------------------------------------------------------
# Р4-5 with В2: rows of another line, contradicted by the same filing
# ---------------------------------------------------------------------------
def test_r45_drops_zeros_and_larger_figures_the_same_filing_contradicts():
    chain = [_fy(2016, 993.0, accn="a"), _fy(2017, 0.0, accn="a"), _fy(2018, 198.0, accn="b"),
             _fy(2015, 0.0, accn="c")]
    facts = _facts(usd={ORD: [_fy(2016, 142.0, accn="a"), _fy(2017, 198.0, accn="a"),
                              _fy(2018, 198.0, accn="b")]})
    keep, n = bp._drop_other_line(facts, chain)
    assert n == 2
    assert sorted((r["end"], r["val"]) for r in keep) == [("2015-12-31", 0.0), ("2018-12-31", 198.0)]


# ---------------------------------------------------------------------------
# Р4-6: gross profit = revenue - cost of revenue, forward only, proven
# ---------------------------------------------------------------------------
def _gp_case(cost_factor=1.0):
    rev = [_q(y, n, 1000.0 + 10 * n) for y in range(2019, 2023) for n in (1, 2, 3, 4)]
    cost = [_q(y, n, 600.0 + n) for y in range(2019, 2023) for n in (1, 2, 3, 4)]
    gp = [_q(y, n, (1000.0 + 10 * n) - (600.0 + n) * cost_factor) for y in (2019, 2020) for n in (1, 2, 3, 4)]
    facts = _facts(usd={"Revenues": rev, "CostOfRevenue": cost, "GrossProfit": gp})
    return gp, bp._gross_from_cost(facts, gp, "CostOfRevenue")


def test_r46_revenue_minus_cost_continues_forward():
    gp, out = _gp_case()
    new = [r for r in out if r.get("_rule") == "Р4-6"]
    assert len(new) == 8 and min(r["end"] for r in new) > max(r["end"] for r in gp)
    assert {r["val"] for r in new} == {(1000.0 + 10 * n) - (600.0 + n) for n in (1, 2, 3, 4)}
    assert out[:len(gp)] == gp


def test_r46_refused_when_cost_is_another_line():
    gp, out = _gp_case(cost_factor=1.02)
    assert out == gp
