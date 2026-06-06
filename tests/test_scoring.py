"""
Tests for the factor scoring engine — focused on NaN propagation.

Run from the project root:

    python -m pytest tests/test_scoring.py -v

Regression context
-------------------
A missing ``dividend_yield`` (any non-dividend payer, e.g. PLTR) used to turn the
*entire* value_score into NaN, which build_scores then filled with a misleading
neutral 0.50 — so an extremely expensive growth stock looked "average value".
The same NaN-propagation hit quality (missing roic/fcf) and risk (missing beta).
These tests pin the corrected behaviour.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.lib.scoring import (
    _quality_score,
    _risk_score,
    _value_score,
    build_scores,
    percentile_rank,
)


# ── Headline regression: expensive non-dividend payer ─────────────────────────

def _universe_with_expensive_nonpayer() -> pd.DataFrame:
    """4 cheap-ish dividend payers + 1 PLTR-like expensive non-payer (no div)."""
    return pd.DataFrame(
        {
            "ticker":         ["A", "B", "C", "D", "PLTR"],
            "name":           ["A", "B", "C", "D", "Palantir"],
            "sector":         ["x"] * 5,
            "cik":            [""] * 5,
            # price/trend — all flat, irrelevant to the value check
            "ret_13w":        [0.0, 0.0, 0.0, 0.0, 0.0],
            "ret_26w":        [0.0, 0.0, 0.0, 0.0, 0.0],
            "ret_52w":        [0.0, 0.0, 0.0, 0.0, 0.0],
            "volatility_26w": [0.2, 0.2, 0.2, 0.2, 0.2],
            # value inputs — PLTR is by far the most expensive on every multiple
            "pe_ratio":       [10.0, 15.0, 20.0, 30.0, 152.0],
            "ev_ebitda":      [6.0, 8.0, 10.0, 14.0, 120.0],
            "pb_ratio":       [1.0, 1.5, 2.0, 3.0, 25.0],
            "dividend_yield": [0.04, 0.03, 0.02, 0.01, None],  # PLTR pays nothing
            # quality / risk — present for everyone here
            "roe":            [0.2, 0.18, 0.15, 0.12, 0.1],
            "oper_margin_ttm":[0.3, 0.25, 0.2, 0.15, 0.1],
            "fcf_margin_ttm": [0.25, 0.2, 0.15, 0.1, 0.05],
            "roic":           [0.18, 0.15, 0.12, 0.1, 0.08],
            "debt_equity":    [0.5, 0.6, 0.7, 0.8, 0.9],
            "beta":           [1.0, 1.1, 1.2, 1.3, 1.4],
        }
    )


def test_expensive_nonpayer_gets_low_value_score_not_neutral():
    """The headline bug: PLTR (no dividend, sky-high multiples) must score LOW."""
    scored = build_scores(_universe_with_expensive_nonpayer())
    pltr = scored.loc[scored["ticker"] == "PLTR"].iloc[0]

    # Must be genuinely cheap-averse, well below the old neutral fill of 0.50.
    assert pltr["value_score"] < 0.3, f"value_score={pltr['value_score']}"
    # And specifically NOT the neutral 0.50 the old NaN-fill produced.
    assert not np.isclose(pltr["value_score"], 0.50)
    # It should be the worst value in the universe.
    assert pltr["value_score"] == scored["value_score"].min()


def test_no_nan_in_value_score_for_nonpayers():
    """A missing dividend must not propagate NaN into value_score."""
    out = _value_score(_universe_with_expensive_nonpayer())
    assert out.notna().all(), "value_score still contains NaN for a non-payer"


# ── nan-aware reweighting in the other factors ────────────────────────────────

def test_quality_score_reweights_when_roic_missing():
    """
    A stock missing roic must score from the three present pieces with their
    weights renormalised (0.30+0.25+0.25 → 0.80) — never collapse to neutral 0.50.
    """
    df = pd.DataFrame(
        {
            "roe":             [0.30, 0.10, 0.05],
            "oper_margin_ttm": [0.40, 0.15, 0.05],
            "fcf_margin_ttm":  [0.35, 0.12, 0.04],
            "roic":            [None, 0.10, 0.03],  # top stock missing roic
        }
    )
    q = _quality_score(df)
    assert q.notna().all()

    # Recompute the beta-of-quality by hand: only roe/opm/fcfm present, so their
    # 0.30/0.25/0.25 weights renormalise to a 0.80 denominator.
    roe  = percentile_rank(df["roe"])
    opm  = percentile_rank(df["oper_margin_ttm"])
    fcfm = percentile_rank(df["fcf_margin_ttm"])
    expected0 = (
        0.30 * roe.iloc[0] + 0.25 * opm.iloc[0] + 0.25 * fcfm.iloc[0]
    ) / (0.30 + 0.25 + 0.25)

    assert np.isclose(q.iloc[0], expected0), f"reweighted quality={q.iloc[0]}"
    assert not np.isclose(q.iloc[0], 0.50)


def test_risk_score_reweights_when_beta_missing():
    """
    Missing beta must reweight onto vol/debt (0.50+0.30 → 0.80 denominator),
    not neutralise the risk score to 0.50.
    """
    df = pd.DataFrame(
        {
            "volatility_26w": [0.10, 0.30, 0.50],  # lowest vol = safest
            "debt_equity":    [0.80, 0.50, 0.20],  # distinct from vol ordering
            "beta":           [None, 1.20, 1.80],  # stock 0 missing beta
        }
    )
    r = _risk_score(df)
    assert r.notna().all()

    # Recompute stock 0 by hand from the two present (inverted) components.
    vol_inv  = 1.0 - percentile_rank(df["volatility_26w"])
    debt_inv = 1.0 - percentile_rank(df["debt_equity"])
    expected0 = (
        0.50 * vol_inv.iloc[0] + 0.30 * debt_inv.iloc[0]
    ) / (0.50 + 0.30)

    assert np.isclose(r.iloc[0], expected0), f"reweighted risk={r.iloc[0]}"
    assert not np.isclose(r.iloc[0], 0.50)


# ── Regression guard: complete data must be untouched ─────────────────────────

def test_value_score_matches_plain_weighted_sum_when_complete():
    """
    With no missing inputs the NaN-aware blend must equal the original plain
    weighted sum (weights sum to 1.0 → denominator 1.0). Guards against silently
    re-ranking complete-data stocks.
    """
    df = pd.DataFrame(
        {
            "pe_ratio":       [10.0, 20.0, 30.0, 40.0],
            "ev_ebitda":      [5.0, 10.0, 15.0, 20.0],
            "pb_ratio":       [1.0, 2.0, 3.0, 4.0],
            "dividend_yield": [0.04, 0.03, 0.02, 0.01],
        }
    )
    got = _value_score(df)

    pe = 1.0 - percentile_rank(df["pe_ratio"])
    ev = 1.0 - percentile_rank(df["ev_ebitda"])
    pb = 1.0 - percentile_rank(df["pb_ratio"])
    dv = percentile_rank(df["dividend_yield"])
    expected = 0.35 * pe + 0.30 * ev + 0.20 * pb + 0.15 * dv

    assert np.allclose(got.to_numpy(), expected.to_numpy())


# ── Fallback: only when EVERY component is missing ────────────────────────────

def test_quality_falls_back_to_neutral_only_when_all_inputs_missing():
    """
    Reweighting rescues a *partially* present factor, but a stock missing EVERY
    quality input has nothing to reweight onto → NaN → build_scores fills the
    genuine neutral 0.50. This confirms the fallback still works where it should.
    """
    df = pd.DataFrame(
        {
            "ticker":         ["FULL", "EMPTY"],
            "name":           ["full", "empty"],
            "sector":         ["x", "x"],
            "cik":            ["", ""],
            "ret_13w":        [0.0, 0.0],
            "ret_26w":        [0.0, 0.0],
            "ret_52w":        [0.0, 0.0],
            "volatility_26w": [0.2, 0.2],
            "pe_ratio":       [12.0, 18.0],
            "ev_ebitda":      [7.0, 9.0],
            "pb_ratio":       [1.5, 2.0],
            "dividend_yield": [0.03, 0.02],
            # EMPTY is missing every single quality input
            "roe":            [0.2, None],
            "oper_margin_ttm":[0.3, None],
            "fcf_margin_ttm": [0.25, None],
            "roic":           [0.18, None],
            "debt_equity":    [0.5, 0.6],
            "beta":           [1.0, 1.1],
        }
    )
    scored = build_scores(df)
    empty = scored.loc[scored["ticker"] == "EMPTY"].iloc[0]
    full = scored.loc[scored["ticker"] == "FULL"].iloc[0]

    assert np.isclose(empty["quality_score"], 0.50)  # genuine neutral fallback
    assert not np.isnan(empty["value_score"])        # value still computed fine
    assert not np.isnan(full["quality_score"])
