"""
ПНЛ12 (19.11.2026): the guard against a filed quarter that contradicts the
cumulative levels, the stale rule with "zero does not go stale", the 16-week
quarter and the shifted ladder start (Ц.'s decisions in PNL10-RESHENIA.md: Р2,
Р1 (в) with (б′), Р5.1, Р5.2).

Synthetic panels in the shapes found in the real one:
  * TSCO: the whole year's net income filed under a Q4 label (1.881bn vs FY - 9M
    = 1.012bn) -> P/E 9 instead of 17.
  * a quarter filed AFTER the levels is a restatement and wins;
  * a filing that restates a known period is a new vintage: its FY minus the old
    9M is no quarter and does not beat the filed one (DD after a divestiture);
  * stale sums are NaN, a stale sum of exactly 0 stays 0 (buybacks that stopped);
  * KR: a 16-week first quarter ends 112 days after the year before it;
  * a 10-Q tag that starts 1-7 days after the 10-K's (52/53-week filers);
  * JCI after the Tyco merger: two years starting 3 days apart, both with a full
    year, stay apart.
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


def _row(start, end, filed, value, form="10-Q", concept="net_income", ticker="TST"):
    return {"ticker": ticker, "cik": "1", "concept": concept, "unit": "USD",
            "period_start": start, "period_end": end, "filed": filed,
            "form": form, "fy": None, "fp": "", "value": value}


def _panel(rows):
    df = pd.DataFrame(rows, columns=PANEL_COLS)
    for c in ("period_start", "period_end", "filed"):
        df[c] = pd.to_datetime(df[c])
    df["value"] = pd.to_numeric(df["value"])
    return df


def _ttm(rows, bar):
    df = _panel(rows)
    return pit.ttm_window(df[df["filed"] <= pd.Timestamp(bar)])


# 2025: Q1 10 / H1 30 / 9M 60 / FY 100 -> Q4 = 40; 2026: Q1 20, Q2 25 (discrete).
_YEAR = [
    _row("2025-01-01", "2025-03-31", "2025-05-01", 10),
    _row("2025-01-01", "2025-06-30", "2025-08-01", 30),
    _row("2025-01-01", "2025-09-30", "2025-11-01", 60),
    _row("2025-01-01", "2025-12-31", "2026-02-20", 100, form="10-K"),
    _row("2026-01-01", "2026-03-31", "2026-05-01", 20),
    _row("2026-04-01", "2026-06-30", "2026-08-01", 25),
    _row("2025-04-01", "2025-06-30", "2025-08-01", 20),
    _row("2025-07-01", "2025-09-30", "2025-11-01", 30),
]


def test_tsco_year_under_a_q4_label_is_dropped():
    """TSCO: the 10-K files Q4 = the whole year (100) next to FY 100 and 9M 60.
    The quarter is FY - 9M = 40; the TTM to 2026-06-30 is 30 + 40 + 20 + 25."""
    R = _YEAR + [_row("2025-10-01", "2025-12-31", "2026-02-20", 100, form="10-K")]
    w = _ttm(R, "2026-09-11")
    assert w["end"] == pd.Timestamp("2026-06-30")
    assert w["value"] == pytest.approx(30 + 40 + 20 + 25)


def test_a_quarter_filed_after_the_levels_wins():
    """Q4 refiled later (10-K/A, 2026-04-15) with 44: a restatement, it wins
    over FY - 9M = 40 from the earlier 10-K."""
    R = _YEAR + [_row("2025-10-01", "2025-12-31", "2026-04-15", 44, form="10-K/A")]
    w = _ttm(R, "2026-09-11")
    assert w["value"] == pytest.approx(30 + 44 + 20 + 25)


def test_a_restating_filing_does_not_trigger_the_guard():
    """The 10-K of 2026-02-20 also refiles FY2024 (90 -> 95): it restates, so its
    FY 2025 minus the old 9M (100 - 60 = 40) is a mix of vintages and the filed
    Q4 of 46 stays."""
    R = _YEAR + [
        _row("2024-01-01", "2024-12-31", "2025-02-20", 90, form="10-K"),
        _row("2024-01-01", "2024-12-31", "2026-02-20", 95, form="10-K"),
        _row("2025-10-01", "2025-12-31", "2026-02-20", 46, form="10-K"),
    ]
    w = _ttm(R, "2026-09-11")
    assert w["value"] == pytest.approx(30 + 46 + 20 + 25)


def test_within_tolerance_the_filed_quarter_stays():
    """Q4 filed 40.1 vs FY - 9M = 40: 0.25% apart, under _GUARD_TOL; nothing drops."""
    R = _YEAR + [_row("2025-10-01", "2025-12-31", "2026-02-20", 40.1, form="10-K")]
    assert _ttm(R, "2026-09-11")["value"] == pytest.approx(30 + 40.1 + 20 + 25)


def _stale_rows(concept, value):
    """`concept`'s last four quarters end in 2019; revenues run to 2026-06-30."""
    R = [_row(s, e, f, value, concept=concept) for s, e, f in
         [("2019-01-01", "2019-03-31", "2019-05-01"), ("2019-04-01", "2019-06-30", "2019-08-01"),
          ("2019-07-01", "2019-09-30", "2019-11-01"), ("2019-10-01", "2019-12-31", "2020-02-01")]]
    R += [_row(s, e, f, 100, concept="revenues") for s, e, f in
          [("2025-07-01", "2025-09-30", "2025-11-01"), ("2025-10-01", "2025-12-31", "2026-02-01"),
           ("2026-01-01", "2026-03-31", "2026-05-01"), ("2026-04-01", "2026-06-30", "2026-08-01")]]
    return R


def test_a_stale_sum_is_empty():
    wide = pit.as_known_at(_panel(_stale_rows("depreciation_amortization", 10)), "2026-09-01")
    assert wide.loc["TST", "revenues_ttm"] == pytest.approx(400)
    assert pd.isna(wide.loc["TST", "depreciation_amortization_ttm"])


def test_a_stale_zero_stays_zero():
    """Buybacks that stopped in 2019: zero then, zero now; not unknown."""
    df = _panel(_stale_rows("buybacks", 0))
    assert pit.as_known_at(df, "2026-09-01").loc["TST", "buybacks_ttm"] == 0
    rev_end = df[df["concept"] == "revenues"]["period_end"].max()
    w = pit.ttm_window(df[df["concept"] == "buybacks"], stale_ref=rev_end)
    assert w["value"] == 0 and w["stale"] is True


def test_a_16_week_first_quarter_falls_back_to_the_year():
    """KR: Q1 of 16 weeks (2026-01-04..2026-04-25) cannot be tiled; the window
    falls back to the 10-K's year, 112 days earlier (> 100, <= 120)."""
    R = [_row("2024-12-29", "2026-01-03", "2026-02-20", 400, form="10-K"),
         _row("2023-12-31", "2024-12-28", "2025-02-20", 380, form="10-K"),
         _row("2026-01-04", "2026-04-25", "2026-05-10", 50)]
    w = _ttm(R, "2026-06-30")
    assert w["value"] == pytest.approx(400)
    assert w["end"] == pd.Timestamp("2026-01-03")


@pytest.mark.parametrize("shift", [1, 3, 7])
def test_a_10q_start_shifted_from_the_10k_start(shift):
    """The 10-Q levels start `shift` days after the 10-K's year (52/53-week tag);
    the ladder is still one year: TTM to 2026-06-30 = H1'26 + FY'25 - H1'25."""
    s = (pd.Timestamp("2025-01-01") + pd.Timedelta(days=shift)).strftime("%Y-%m-%d")
    R = [_row(s, "2025-03-31", "2025-05-01", 10),
         _row(s, "2025-06-30", "2025-08-01", 30),
         _row(s, "2025-09-30", "2025-11-01", 60),
         _row("2025-01-01", "2025-12-31", "2026-02-20", 100, form="10-K"),
         _row("2026-01-01", "2026-03-31", "2026-05-01", 20),
         _row("2026-01-01", "2026-06-30", "2026-08-01", 45),
         _row("2024-01-01", "2024-12-31", "2025-02-20", 90, form="10-K"),
         _row(s, "2025-06-30", "2026-08-01", 30)]
    assert _ttm(R, "2026-09-11")["value"] == pytest.approx(45 + 100 - 30)


def test_two_full_years_a_few_days_apart_stay_apart():
    """JCI after the Tyco merger: the old fiscal year from 2013-09-28 (Q1 25, H1 50,
    9M 76, FY 103) and the merged company's restated one from 2013-10-01 (FY 387,
    filed 2016) both carry a full year. They are different vintages: the merged FY
    must not become the old ladder's top (387 - 76 is no quarter)."""
    R = [_row("2013-09-28", "2013-12-27", "2014-01-31", 25),
         _row("2013-09-28", "2014-03-28", "2014-04-25", 50),
         _row("2013-09-28", "2014-06-27", "2014-07-25", 76),
         _row("2013-09-28", "2014-09-26", "2014-11-14", 103, form="10-K"),
         _row("2013-10-01", "2014-09-30", "2016-11-23", 387, form="10-K")]
    q = pit._quarters_for_flow(_panel(R))
    q4 = dict(zip(q["period_end"], q["value"]))[pd.Timestamp("2014-09-26")]
    assert q4 == pytest.approx(103 - 76)
