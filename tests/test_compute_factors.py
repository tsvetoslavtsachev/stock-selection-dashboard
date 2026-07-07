"""
Tests for price-feature computation and the missing-price data-quality handling.

Closes the second missing-data trap: a stock with no/short price history used to
get volatility = 0.0, which ranks as the *calmest* (= safest) stock and inflates
its risk_score. It now gets NaN (flagged), which flows into the NaN-aware scoring
instead of masquerading as a real value.

Run:  python -m pytest tests/test_compute_factors.py -v
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.jobs.compute_factors import _price_features
from src.lib.scoring import build_scores


def _daily_series(n: int) -> pd.Series:
    """A clean geometric daily close series of length n (business days) — feeds
    _price_features, which resamples to W-FRI internally for the weekly features
    and reads the daily series directly for 12-1 momentum."""
    idx = pd.bdate_range("2022-01-03", periods=n)
    return pd.Series(100.0 * (1.001 ** np.arange(n)), index=idx)


def test_price_features_full_history_are_real_numbers():
    feats = _price_features(_daily_series(400))  # > 253 daily → every feature real
    assert all(not pd.isna(v) for v in feats.values())
    assert feats["volatility_26w"] >= 0.0
    assert set(feats) == {"ret_12_1", "ret_13w", "volatility_26w"}


def test_price_features_short_history_returns_nan_not_zero():
    """
    ~16 weeks of daily bars: ret_13w is computable, but 12-1 momentum (needs ~253
    daily bars) and 26-week volatility cannot be — NaN, NOT a misleading 0.0.
    """
    feats = _price_features(_daily_series(80))
    assert not pd.isna(feats["ret_13w"])
    assert pd.isna(feats["ret_12_1"])
    assert pd.isna(feats["volatility_26w"])


def test_ret_12_1_matches_by_hand_skipping_last_month():
    """12-1 momentum = price[t-21] / price[t-252] - 1 on the daily series."""
    s = _daily_series(300)
    expected = round(s.iloc[-22] / s.iloc[-253] - 1.0, 6)
    assert np.isclose(_price_features(s)["ret_12_1"], expected)


def test_ret_12_1_ignores_the_most_recent_month():
    """Skip-month: a violent move in the last 21 bars must NOT change 12-1
    momentum — that window is deliberately excluded (no short-term reversal, no
    lookahead into the skipped tail)."""
    s = _daily_series(300)
    base = _price_features(s)["ret_12_1"]
    s2 = s.copy()
    s2.iloc[-21:] *= 1.5
    assert np.isclose(base, _price_features(s2)["ret_12_1"])


def _universe_with_missing_price_stock() -> pd.DataFrame:
    """4 normal stocks + NODATA whose price features are NaN (data outage)."""
    return pd.DataFrame(
        {
            "ticker":         ["A", "B", "C", "D", "NODATA"],
            "name":           ["A", "B", "C", "D", "No history"],
            "sector":         ["x"] * 5,
            "cik":            [""] * 5,
            "ret_12_1":       [0.20, 0.15, 0.10, 0.05, np.nan],
            "ret_13w":        [0.05, 0.04, 0.03, 0.02, np.nan],
            # A is genuinely the safest (lowest real vol); NODATA's vol is MISSING.
            "volatility_26w": [0.15, 0.30, 0.35, 0.40, np.nan],
            "pe_ratio":       [15.0, 18.0, 20.0, 22.0, 19.0],
            "ev_ebitda":      [8.0, 9.0, 10.0, 11.0, 9.5],
            "pb_ratio":       [2.0, 2.5, 3.0, 3.5, 2.8],
            "dividend_yield": [0.02] * 5,
            "net_payout_yield":[0.03] * 5,
            "roe":            [0.15] * 5,
            "oper_margin_ttm":[0.20] * 5,
            "fcf_margin_ttm": [0.15] * 5,
            "roic":           [0.12] * 5,
            "gpa":            [0.30] * 5,
            # A also has the lowest debt & beta → unambiguously the safest stock.
            "debt_equity":    [0.20, 0.60, 0.70, 0.80, 0.90],
            "beta":           [0.80, 1.10, 1.20, 1.30, 1.40],
        }
    )


def test_missing_prices_do_not_make_stock_look_safest():
    """
    The headline of trap #2: NODATA (missing volatility) must NOT be ranked the
    safest. With NaN volatility its risk reweights onto debt/beta — where it is
    actually the *worst* — so the genuinely-safe stock A wins on risk.
    """
    scored = build_scores(_universe_with_missing_price_stock()).set_index("ticker")

    assert scored.loc["A", "risk_score"] > scored.loc["NODATA", "risk_score"]
    assert scored["risk_score"].idxmax() == "A"
    # And NODATA's risk is not the fake "calmest" extreme it used to be.
    assert scored.loc["NODATA", "risk_score"] < scored.loc["A", "risk_score"]


def test_sector_guards_null_ev_ebitda_and_gpa_for_financials():
    """Financials must not carry EV/EBITDA (no operating EBITDA) or GP/A (the
    fallback fills banks with a fictitious ~0 gross profit) into scoring — both
    NaN, so the buckets reweight onto metrics that do apply to them."""
    from src.jobs.compute_factors import _apply_sector_guards

    bank = {"sector": "Financials", "ev_ebitda": 9.5, "gpa": 0.001, "roe": 0.12}
    _apply_sector_guards(bank)
    assert np.isnan(bank["ev_ebitda"]) and np.isnan(bank["gpa"])
    assert bank["roe"] == 0.12  # untouched

    tech = {"sector": "Information Technology", "ev_ebitda": 15.0, "gpa": 0.45}
    _apply_sector_guards(tech)
    assert tech["ev_ebitda"] == 15.0 and tech["gpa"] == 0.45


def test_missing_prices_trend_falls_back_to_neutral():
    """All trend inputs NaN → trend_score is the neutral centre 0.0 (the sector-
    neutral mean after the M1 rework), NOT the old misleading 0.50 that beat real
    stocks."""
    scored = build_scores(_universe_with_missing_price_stock()).set_index("ticker")
    assert np.isclose(scored.loc["NODATA", "trend_score"], 0.0)
