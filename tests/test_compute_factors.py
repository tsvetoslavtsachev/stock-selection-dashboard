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


def _weekly_series(n: int) -> pd.Series:
    """A clean upward-drifting weekly close series of length n."""
    idx = pd.date_range("2022-01-01", periods=n, freq="W")
    return pd.Series(np.linspace(100.0, 100.0 + n, n), index=idx)


def test_price_features_full_history_are_real_numbers():
    feats = _price_features(_weekly_series(120))
    assert all(not pd.isna(v) for v in feats.values())
    assert feats["volatility_26w"] >= 0.0


def test_price_features_short_history_returns_nan_not_zero():
    """
    13 weeks: ret_13w is computable, but ret_26w/ret_52w and volatility_26w
    cannot be — they must be NaN, NOT a misleading 0.0.
    """
    feats = _price_features(_weekly_series(14))  # 14 points → 13-week window only
    assert not pd.isna(feats["ret_13w"])
    assert pd.isna(feats["ret_26w"])
    assert pd.isna(feats["ret_52w"])
    assert pd.isna(feats["volatility_26w"])


def _universe_with_missing_price_stock() -> pd.DataFrame:
    """4 normal stocks + NODATA whose price features are NaN (data outage)."""
    return pd.DataFrame(
        {
            "ticker":         ["A", "B", "C", "D", "NODATA"],
            "name":           ["A", "B", "C", "D", "No history"],
            "sector":         ["x"] * 5,
            "cik":            [""] * 5,
            "ret_13w":        [0.05, 0.04, 0.03, 0.02, np.nan],
            "ret_26w":        [0.10, 0.08, 0.06, 0.04, np.nan],
            "ret_52w":        [0.20, 0.15, 0.10, 0.05, np.nan],
            # A is genuinely the safest (lowest real vol); NODATA's vol is MISSING.
            "volatility_26w": [0.15, 0.30, 0.35, 0.40, np.nan],
            "pe_ratio":       [15.0, 18.0, 20.0, 22.0, 19.0],
            "ev_ebitda":      [8.0, 9.0, 10.0, 11.0, 9.5],
            "pb_ratio":       [2.0, 2.5, 3.0, 3.5, 2.8],
            "dividend_yield": [0.02] * 5,
            "roe":            [0.15] * 5,
            "oper_margin_ttm":[0.20] * 5,
            "fcf_margin_ttm": [0.15] * 5,
            "roic":           [0.12] * 5,
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


def test_missing_prices_trend_falls_back_to_neutral():
    """All trend inputs NaN → trend_score is the neutral 0.50 fallback, not 0.0."""
    scored = build_scores(_universe_with_missing_price_stock()).set_index("ticker")
    assert np.isclose(scored.loc["NODATA", "trend_score"], 0.50)
