"""
ПНЛ7 (14.10.2026): the fiscal year of a flow fact is its period START, never the
filing's ``fy`` stamp; a TTM never skips a quarter.

Synthetic panels in the shapes found in the real one:
  * WDAY: 10-Qs stamped fy = year - 1, the 10-K fy = year, comparatives carry the
    NEW filing's fy -> grouping by fy paired FY25 with 9M FY26 and left FY26 alone.
  * FFIV: no Q1 fact at all, only H1 / 9M / FY -> twelve months from blocks.
  * FIX: a 10-Q carrying a "12-month" fact worth the Q1 figure.
  * BDX: 9M restated after a divestiture; Q4 must come from the same vintage.
  * INTC D&A: the last four quarters are years old -> stale, NaN (a switch, off).
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


def _row(concept, start, end, filed, value, fy, fp="", form="10-Q", ticker="TST"):
    return {
        "ticker": ticker, "cik": "1", "concept": concept, "unit": "USD",
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


def _wday_rows(c="net_income"):
    """Fiscal year ends 31 Jan. Cumulative: FY25 Q1 10 / H1 25 / 9M 45 / FY 70;
    FY26 20 / 50 / 90 / 140; FY27 40 / 100. Stamps as WDAY files them."""
    R = []
    # FY25 10-Qs (stamped 2024) and 10-K (stamped 2025)
    R += [_row(c, "2024-02-01", "2024-04-30", "2024-05-25", 10, 2024, "Q1"),
          _row(c, "2024-02-01", "2024-07-31", "2024-08-25", 25, 2024, "Q2"),
          _row(c, "2024-05-01", "2024-07-31", "2024-08-25", 15, 2024, "Q2"),
          _row(c, "2024-02-01", "2024-10-31", "2024-11-25", 45, 2024, "Q3"),
          _row(c, "2024-08-01", "2024-10-31", "2024-11-25", 20, 2024, "Q3"),
          _row(c, "2024-02-01", "2025-01-31", "2025-03-10", 70, 2025, "FY", form="10-K")]
    # FY26 10-Qs (stamped 2025, comparatives too) and 10-K (stamped 2026)
    R += [_row(c, "2025-02-01", "2025-04-30", "2025-05-25", 20, 2025, "Q1"),
          _row(c, "2024-02-01", "2024-04-30", "2025-05-25", 10, 2025, "Q1"),
          _row(c, "2025-02-01", "2025-07-31", "2025-08-25", 50, 2025, "Q2"),
          _row(c, "2025-05-01", "2025-07-31", "2025-08-25", 30, 2025, "Q2"),
          _row(c, "2024-02-01", "2024-07-31", "2025-08-25", 25, 2025, "Q2"),
          _row(c, "2025-02-01", "2025-10-31", "2025-11-25", 90, 2025, "Q3"),
          _row(c, "2025-08-01", "2025-10-31", "2025-11-25", 40, 2025, "Q3"),
          _row(c, "2024-02-01", "2024-10-31", "2025-11-25", 45, 2025, "Q3"),
          _row(c, "2025-02-01", "2026-01-31", "2026-03-10", 140, 2026, "FY", form="10-K"),
          _row(c, "2024-02-01", "2025-01-31", "2026-03-10", 70, 2026, "FY", form="10-K")]
    # FY27 10-Qs (stamped 2026)
    R += [_row(c, "2026-02-01", "2026-04-30", "2026-05-25", 40, 2026, "Q1"),
          _row(c, "2025-02-01", "2025-04-30", "2026-05-25", 20, 2026, "Q1"),
          _row(c, "2026-02-01", "2026-07-31", "2026-08-25", 100, 2026, "Q2"),
          _row(c, "2026-05-01", "2026-07-31", "2026-08-25", 60, 2026, "Q2"),
          _row(c, "2025-02-01", "2025-07-31", "2026-08-25", 50, 2026, "Q2")]
    return R


def test_wday_q4_is_fy_minus_9m_of_the_same_year():
    q = pit._quarters_for_flow(_panel(_wday_rows()))
    by_end = dict(zip(q["period_end"], q["value"]))
    assert by_end[pd.Timestamp("2025-01-31")] == pytest.approx(25)   # 70 - 45, not 70 - 90
    assert by_end[pd.Timestamp("2026-01-31")] == pytest.approx(50)   # 140 - 90, was missing


def test_wday_ttm_has_no_skipped_quarter():
    # Q3 FY26 40 + Q4 FY26 50 + Q1 FY27 40 + Q2 FY27 60 (old pit: 30 + 40 + 40 + 60, Q4 skipped)
    wide = pit.as_known_at(_panel(_wday_rows()), "2026-09-01")
    assert wide.loc["TST", "net_income_ttm"] == pytest.approx(190)


def test_wday_ttm_at_fy_end_equals_the_10k():
    wide = pit.as_known_at(_panel(_wday_rows()), "2026-04-01")
    assert wide.loc["TST", "net_income_ttm"] == pytest.approx(140)


def test_ffiv_no_q1_twelve_months_from_blocks():
    """Fiscal year from 1 Oct; Q1 never tagged, only H1 / 9M / FY. TTM at 9M FY26 =
    H1 FY26 + Q3 FY26 + Q4 FY25, no quarter skipped."""
    c = "buybacks"
    R = [_row(c, "2024-10-01", "2025-03-31", "2025-05-06", 250, 2025, "Q2"),
         _row(c, "2024-10-01", "2025-06-30", "2025-08-06", 375, 2025, "Q3"),
         _row(c, "2024-10-01", "2025-09-30", "2025-11-25", 500, 2025, "FY", form="10-K"),
         _row(c, "2025-10-01", "2026-03-31", "2026-05-05", 400, 2026, "Q2"),
         _row(c, "2025-10-01", "2026-06-30", "2026-08-06", 500, 2026, "Q3")]
    w = pit.ttm_window(_panel(R))
    assert w["value"] == pytest.approx(400 + 100 + 125)
    assert w["end"] == pd.Timestamp("2026-06-30")
    assert w["start"] == pd.Timestamp("2025-07-01")


def test_fix_twelve_month_fact_in_a_10q_is_ignored():
    """A 10-Q filed after the 10-K carries '2025 full year = Q1 2025'; the 10-K wins."""
    c = "revenues"
    R = [_row(c, "2025-01-01", "2025-03-31", "2025-04-24", 18, 2025, "Q1"),
         _row(c, "2025-01-01", "2025-06-30", "2025-07-24", 40, 2025, "Q2"),
         _row(c, "2025-01-01", "2025-09-30", "2025-10-23", 64, 2025, "Q3"),
         _row(c, "2025-01-01", "2025-12-31", "2026-02-19", 91, 2025, "FY", form="10-K"),
         _row(c, "2026-01-01", "2026-03-31", "2026-04-23", 28, 2026, "Q1"),
         _row(c, "2025-01-01", "2025-12-31", "2026-04-23", 18, 2026, "Q1")]   # the stray fact
    q = pit._quarters_for_flow(_panel(R))
    assert dict(zip(q["period_end"], q["value"]))[pd.Timestamp("2025-12-31")] == pytest.approx(27)
    wide = pit.as_known_at(_panel(R), "2026-05-01")
    assert wide.loc["TST", "revenues_ttm"] == pytest.approx(22 + 24 + 27 + 28)


def test_bdx_q4_from_the_same_vintage_after_a_restatement():
    """FY25 10-K = 100 when 9M was 75; a later 10-Q restates 9M FY25 to 60
    (continuing operations). Q4 FY25 stays 100 - 75 = 25, not 100 - 60 = 40."""
    c = "revenues"
    R = [_row(c, "2024-10-01", "2025-06-30", "2025-08-07", 75, 2025, "Q3"),
         _row(c, "2024-10-01", "2025-09-30", "2025-11-25", 100, 2025, "FY", form="10-K"),
         _row(c, "2024-10-01", "2025-06-30", "2026-08-06", 60, 2026, "Q3")]
    q = pit._quarters_for_flow(_panel(R))
    assert dict(zip(q["period_end"], q["value"]))[pd.Timestamp("2025-09-30")] == pytest.approx(25)


def test_stale_ttm_is_nan_when_the_switch_is_on(monkeypatch):
    """D&A's last four quarters end in 2019 while revenues run to 2026: not a TTM.
    The rule is a switch (off since СТОП А, 14.10.2026); off, the old sum stays."""
    R = [_row("depreciation_amortization", f"2019-{m:02d}-01", e, f, 10, 2019)
         for m, e, f in [(1, "2019-03-31", "2019-05-01"), (4, "2019-06-30", "2019-08-01"),
                         (7, "2019-09-30", "2019-11-01"), (10, "2019-12-31", "2020-02-01")]]
    R += [_row("revenues", s, e, f, 100, 2026) for s, e, f in
          [("2025-07-01", "2025-09-30", "2025-11-01"), ("2025-10-01", "2025-12-31", "2026-02-01"),
           ("2026-01-01", "2026-03-31", "2026-05-01"), ("2026-04-01", "2026-06-30", "2026-08-01")]]
    monkeypatch.setattr(pit, "_STALE_TTM_DAYS", 100, raising=False)
    wide = pit.as_known_at(_panel(R), "2026-09-01")
    assert wide.loc["TST", "revenues_ttm"] == pytest.approx(400)
    assert pd.isna(wide.loc["TST", "depreciation_amortization_ttm"])
    monkeypatch.setattr(pit, "_STALE_TTM_DAYS", None, raising=False)
    wide = pit.as_known_at(_panel(R), "2026-09-01")
    assert wide.loc["TST", "depreciation_amortization_ttm"] == pytest.approx(40)


def test_eix_no_fy_no_years_old_window():
    """EIX net income: FY2021 never tagged, so Q4 2021 cannot be derived. The latest
    quarter (Q3 2022) cannot be tiled; the last tileable window ends 2021-09-30, a
    year back -> NaN, not a stale sum standing in for 2022 (old pit: Q1-Q3 2021 +
    Q1 2022 across the hole)."""
    c = "net_income"
    R = [_row(c, "2020-01-01", "2020-09-30", "2020-11-01", 213, 2020, "Q3"),
         _row(c, "2020-01-01", "2020-12-31", "2021-02-25", 739, 2020, "FY", form="10-K")]
    R += [_row(c, s, e, f, v, fy) for s, e, f, v, fy in [
        ("2021-01-01", "2021-03-31", "2021-04-27", 290, 2021), ("2021-04-01", "2021-06-30", "2021-07-29", 318, 2021),
        ("2021-07-01", "2021-09-30", "2021-11-02", -341, 2021), ("2022-01-01", "2022-03-31", "2022-05-03", 136, 2022),
        ("2022-04-01", "2022-06-30", "2022-08-03", 100, 2022), ("2022-07-01", "2022-09-30", "2022-11-03", 120, 2022)]]
    assert pd.isna(pit.as_known_at(_panel(R), "2022-12-01").loc["TST", "net_income_ttm"])
    # before Q1 2022 is filed the 2021-09-30 window IS the latest: Q4 2020 + Q1-Q3 2021
    w = pit.ttm_window(_panel(R)[lambda d: d["filed"] <= "2022-01-31"])
    assert w["value"] == pytest.approx(526 + 290 + 318 - 341)
    assert w["end"] == pd.Timestamp("2021-09-30")
    # once Q1 2022 is filed, 2021-09-30 is two quarters back -> NaN
    assert pd.isna(pit.ttm_window(_panel(R)[lambda d: d["filed"] <= "2022-06-01"])["value"])
