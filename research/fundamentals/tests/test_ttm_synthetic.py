"""
Synthetic-input tests for the flow -> quarter -> TTM machinery in pit.py.

These do not touch the network or the real cache: they build tiny panels by hand so
the arithmetic (YTD differencing, Q4 = FY - 9M, "last 4 distinct quarters") is
pinned exactly. Run:

    python -m pytest research/fundamentals/tests/test_ttm_synthetic.py -q
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from research.fundamentals import pit  # noqa: E402


PANEL_COLS = [
    "ticker", "cik", "concept", "unit",
    "period_start", "period_end", "filed", "form", "fy", "fp", "value",
]


def _row(concept, start, end, filed, value, fy, fp="", form="10-Q", unit="USD", ticker="TST"):
    return {
        "ticker": ticker, "cik": "1", "concept": concept, "unit": unit,
        "period_start": start, "period_end": end, "filed": filed,
        "form": form, "fy": fy, "fp": fp, "value": value,
    }


def _panel(rows):
    df = pd.DataFrame(rows, columns=PANEL_COLS)
    for c in ("period_start", "period_end", "filed"):
        df[c] = pd.to_datetime(df[c])
    df["value"] = pd.to_numeric(df["value"])
    df["fy"] = pd.to_numeric(df["fy"])
    return df


def test_ttm_from_four_discrete_quarters():
    """Four clean ~90d quarters -> TTM is their sum."""
    rows = [
        _row("revenues", "2022-01-01", "2022-03-31", "2022-05-01", 100, 2022, "Q1"),
        _row("revenues", "2022-04-01", "2022-06-30", "2022-08-01", 110, 2022, "Q2"),
        _row("revenues", "2022-07-01", "2022-09-30", "2022-11-01", 120, 2022, "Q3"),
        _row("revenues", "2022-10-01", "2022-12-31", "2023-02-01", 130, 2022, "Q4", form="10-K"),
    ]
    wide = pit.as_known_at(_panel(rows), "2023-03-01")
    assert wide.loc["TST", "revenues_ttm"] == pytest.approx(460.0)


def test_ytd_differencing_to_quarters():
    """Filer reports YTD cumulative levels (H1, 9M) plus discrete Q1; the 10-K gives
    FY. The machinery must recover Q2 = H1-Q1, Q3 = 9M-H1, Q4 = FY-9M and TTM the
    four discrete quarters."""
    rows = [
        # Q1 discrete (90d)
        _row("net_income", "2022-01-01", "2022-03-31", "2022-05-01", 10, 2022, "Q1"),
        # H1 YTD (180d) -> Q2 = 25-10 = 15
        _row("net_income", "2022-01-01", "2022-06-30", "2022-08-01", 25, 2022, "Q2"),
        # 9M YTD (270d) -> Q3 = 45-25 = 20
        _row("net_income", "2022-01-01", "2022-09-30", "2022-11-01", 45, 2022, "Q3"),
        # FY (365d) -> Q4 = 70-45 = 25
        _row("net_income", "2022-01-01", "2022-12-31", "2023-02-01", 70, 2022, "FY", form="10-K"),
    ]
    wide = pit.as_known_at(_panel(rows), "2023-03-01")
    # TTM = 10 + 15 + 20 + 25 = 70 (== the FY, a good internal consistency check)
    assert wide.loc["TST", "net_income_ttm"] == pytest.approx(70.0)


def test_q4_derivation_only_from_fy_minus_9m():
    """When only YTD levels + FY are available (no discrete quarters tagged), Q4 is
    recoverable ONLY as FY - 9M; verify the full ladder differences correctly."""
    rows = [
        _row("operating_cash_flow", "2021-01-01", "2021-03-31", "2021-05-01", 40, 2021, "Q1"),   # Q1=40
        _row("operating_cash_flow", "2021-01-01", "2021-06-30", "2021-08-01", 90, 2021, "Q2"),   # H1=90 -> Q2=50
        _row("operating_cash_flow", "2021-01-01", "2021-09-30", "2021-11-01", 150, 2021, "Q3"),  # 9M=150 -> Q3=60
        _row("operating_cash_flow", "2021-01-01", "2021-12-31", "2022-02-01", 220, 2021, "FY", form="10-K"),  # FY=220 -> Q4=70
    ]
    q = pit._quarters_for_flow(_panel(rows)[lambda d: d["concept"] == "operating_cash_flow"])
    by_q = dict(zip(q["q_index"], q["value"]))
    assert by_q[1] == pytest.approx(40)
    assert by_q[2] == pytest.approx(50)
    assert by_q[3] == pytest.approx(60)
    assert by_q[4] == pytest.approx(70)


def test_mixed_discrete_q1_plus_cumulative_ladder():
    """The real-world AAPL shape: a discrete Q1 (~90d) coexists with cumulative H1 /
    9M / FY levels (all sharing the fiscal-year start). Q2 = H1-Q1, Q3 = 9M-H1,
    Q4 = FY-9M must all come out clean, and the discrete Q1 must be used directly."""
    rows = [
        _row("revenues", "2022-01-01", "2022-03-31", "2022-05-01", 100, 2022, "Q1"),               # discrete Q1 = 100
        _row("revenues", "2022-01-01", "2022-06-30", "2022-08-01", 210, 2022, "Q2"),               # H1 cum -> Q2 = 110
        _row("revenues", "2022-01-01", "2022-09-30", "2022-11-01", 330, 2022, "Q3"),               # 9M cum -> Q3 = 120
        _row("revenues", "2022-01-01", "2022-12-31", "2023-02-01", 460, 2022, "FY", form="10-K"),  # FY    -> Q4 = 130
    ]
    q = pit._quarters_for_flow(_panel(rows)[lambda d: d["concept"] == "revenues"])
    by_q = dict(zip(q["q_index"], q["value"]))
    assert by_q[1] == pytest.approx(100)
    assert by_q[2] == pytest.approx(110)
    assert by_q[3] == pytest.approx(120)
    assert by_q[4] == pytest.approx(130)
    wide = pit.as_known_at(_panel(rows), "2023-03-01")
    assert wide.loc["TST", "revenues_ttm"] == pytest.approx(460.0)


def test_ttm_rolls_across_fiscal_years():
    """TTM at a date mid-next-year uses the most recent 4 distinct quarters, which
    span two fiscal years."""
    rows = [
        _row("revenues", "2021-01-01", "2021-03-31", "2021-05-01", 10, 2021, "Q1"),
        _row("revenues", "2021-04-01", "2021-06-30", "2021-08-01", 10, 2021, "Q2"),
        _row("revenues", "2021-07-01", "2021-09-30", "2021-11-01", 10, 2021, "Q3"),
        _row("revenues", "2021-10-01", "2021-12-31", "2022-02-01", 10, 2021, "Q4", form="10-K"),
        _row("revenues", "2022-01-01", "2022-03-31", "2022-05-01", 20, 2022, "Q1"),
        _row("revenues", "2022-04-01", "2022-06-30", "2022-08-01", 20, 2022, "Q2"),
    ]
    # As of 2022-09-01: last 4 distinct quarters = 2021Q3,Q4 + 2022Q1,Q2 = 10+10+20+20 = 60
    wide = pit.as_known_at(_panel(rows), "2022-09-01")
    assert wide.loc["TST", "revenues_ttm"] == pytest.approx(60.0)


def test_ttm_needs_four_quarters():
    """Only three known quarters -> TTM is NaN (no partial-year figure)."""
    rows = [
        _row("revenues", "2022-01-01", "2022-03-31", "2022-05-01", 10, 2022, "Q1"),
        _row("revenues", "2022-04-01", "2022-06-30", "2022-08-01", 10, 2022, "Q2"),
        _row("revenues", "2022-07-01", "2022-09-30", "2022-11-01", 10, 2022, "Q3"),
    ]
    wide = pit.as_known_at(_panel(rows), "2022-12-01")
    assert pd.isna(wide.loc["TST", "revenues_ttm"])


def test_stock_concept_latest_period_end():
    """A stock concept takes the value at the latest period_end filed on/before D;
    a restatement (same end, later filed) supersedes."""
    rows = [
        _row("stockholders_equity", "", "2022-03-31", "2022-05-01", 500, 2022, "Q1"),
        _row("stockholders_equity", "", "2022-06-30", "2022-08-01", 520, 2022, "Q2"),
        # restatement of Q1 filed later, but Q2 has a later period_end -> Q2 wins
        _row("stockholders_equity", "", "2022-03-31", "2022-09-01", 505, 2022, "Q1"),
    ]
    wide = pit.as_known_at(_panel(rows), "2022-10-01")
    assert wide.loc["TST", "stockholders_equity"] == pytest.approx(520.0)


def test_stock_restatement_same_end_later_filed_wins():
    rows = [
        _row("total_assets", "", "2022-12-31", "2023-02-01", 1000, 2022, "FY", form="10-K"),
        _row("total_assets", "", "2022-12-31", "2023-05-01", 1010, 2022, "FY", form="10-K/A"),
    ]
    wide = pit.as_known_at(_panel(rows), "2023-06-01")
    assert wide.loc["TST", "total_assets"] == pytest.approx(1010.0)
