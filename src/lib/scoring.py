"""
Factor scoring engine.

Factor weights
--------------
Trend score    = 40% ret_13w  + 30% ret_26w  + 30% ret_52w
Quality score  = 40% revenue_growth_ttm + 30% oper_margin_ttm + 30% fcf_margin_ttm
Value score    = inverse percentile rank of ev_ebit  (lower EV/EBIT → higher score)
Risk score     = inverse percentile rank of volatility_26w  (lower vol → higher score)

Composite      = 35% trend + 30% quality + 20% value + 15% risk

All component scores are first converted to percentile ranks [0, 1] before
weighting, so every factor contributes on the same normalised scale.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Composite weights
# ---------------------------------------------------------------------------

TREND_W = 0.35
QUALITY_W = 0.30
VALUE_W = 0.20
RISK_W = 0.15

# Sub-factor weights inside Trend
_TREND_W_13 = 0.40
_TREND_W_26 = 0.30
_TREND_W_52 = 0.30

# Sub-factor weights inside Quality
_QUAL_W_REV = 0.40
_QUAL_W_OPM = 0.30
_QUAL_W_FCF = 0.30


# ---------------------------------------------------------------------------
# Core utility
# ---------------------------------------------------------------------------

def percentile_rank(series: pd.Series) -> pd.Series:
    """
    Convert a numeric Series to percentile ranks in [0, 1].

    NaN values are excluded from the ranking denominator and remain NaN in
    the output (they are handled downstream in ``build_scores``).

    Parameters
    ----------
    series : pd.Series
        Raw factor values (any numeric dtype).

    Returns
    -------
    pd.Series
        Percentile ranks, same index as *series*, values in [0.0, 1.0].
        Equal values receive the average of their rank positions (method="average").
    """
    return series.rank(method="average", ascending=True, pct=True, na_option="keep")


# ---------------------------------------------------------------------------
# Score builders
# ---------------------------------------------------------------------------

def _trend_score(df: pd.DataFrame) -> pd.Series:
    """
    Weighted average of momentum percentile ranks.

    Requires columns: ret_13w, ret_26w, ret_52w
    """
    r13 = percentile_rank(df["ret_13w"])
    r26 = percentile_rank(df["ret_26w"])
    r52 = percentile_rank(df["ret_52w"])

    score = _TREND_W_13 * r13 + _TREND_W_26 * r26 + _TREND_W_52 * r52
    return score.rename("trend_score")


def _quality_score(df: pd.DataFrame) -> pd.Series:
    """
    Weighted average of profitability percentile ranks.

    Requires columns: revenue_growth_ttm, oper_margin_ttm, fcf_margin_ttm
    """
    rg = percentile_rank(df["revenue_growth_ttm"])
    om = percentile_rank(df["oper_margin_ttm"])
    fm = percentile_rank(df["fcf_margin_ttm"])

    score = _QUAL_W_REV * rg + _QUAL_W_OPM * om + _QUAL_W_FCF * fm
    return score.rename("quality_score")


def _value_score(df: pd.DataFrame) -> pd.Series:
    """
    Inverted percentile rank of EV/EBIT.

    Lower EV/EBIT (cheaper) → higher value score.
    Requires column: ev_ebit
    """
    # 1 − rank so that cheap (low EV/EBIT) gets high score
    score = 1.0 - percentile_rank(df["ev_ebit"])
    return score.rename("value_score")


def _risk_score(df: pd.DataFrame) -> pd.Series:
    """
    Inverted percentile rank of 26-week realised volatility.

    Lower volatility → higher risk score (less risky = preferred).
    Requires column: volatility_26w
    """
    score = 1.0 - percentile_rank(df["volatility_26w"])
    return score.rename("risk_score")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute all factor scores and the composite score for the full universe.

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame with (at minimum) the following columns:

        Trend inputs:
            ret_13w            — 13-week total return (decimal, e.g. 0.12 = +12 %)
            ret_26w            — 26-week total return
            ret_52w            — 52-week total return

        Quality inputs:
            revenue_growth_ttm — trailing 12-month revenue growth (decimal)
            oper_margin_ttm    — trailing 12-month operating margin (decimal)
            fcf_margin_ttm     — trailing 12-month free-cash-flow margin (decimal)

        Value inputs:
            ev_ebit            — EV / EBIT ratio (positive = profitable)

        Risk inputs:
            volatility_26w     — annualised 26-week return volatility (decimal)

        Optional pass-through columns (kept as-is):
            ticker, name, sector, cik, …

    Missing numeric values are filled with 0 before scoring so that a gap in
    one factor does not silently drop the entire row.

    Returns
    -------
    pd.DataFrame
        Original columns plus:
            trend_score, quality_score, value_score, risk_score,
            composite_score
        Sorted by composite_score descending (rank 1 = best stock).
    """
    # Work on a copy to avoid mutating caller's DataFrame
    out = df.copy()

    # Factor input columns and their fill-forward value for missing data
    factor_cols = [
        "ret_13w", "ret_26w", "ret_52w",
        "revenue_growth_ttm", "oper_margin_ttm", "fcf_margin_ttm",
        "ev_ebit", "volatility_26w",
    ]

    missing_cols = [c for c in factor_cols if c not in out.columns]
    if missing_cols:
        logger.warning("Missing factor columns — filling with 0: %s", missing_cols)
        for col in missing_cols:
            out[col] = 0.0

    # Replace NaN with 0 for all factor inputs
    out[factor_cols] = out[factor_cols].fillna(0.0)

    # Compute individual factor scores
    out["trend_score"]   = _trend_score(out)
    out["quality_score"] = _quality_score(out)
    out["value_score"]   = _value_score(out)
    out["risk_score"]    = _risk_score(out)

    # Fill any NaN scores produced by zero-variance columns (all ranks equal)
    score_cols = ["trend_score", "quality_score", "value_score", "risk_score"]
    out[score_cols] = out[score_cols].fillna(0.5)

    # Composite score (weighted average of factor scores)
    out["composite_score"] = (
        TREND_W   * out["trend_score"]
        + QUALITY_W * out["quality_score"]
        + VALUE_W   * out["value_score"]
        + RISK_W    * out["risk_score"]
    )

    # Round scores for readability
    score_cols_all = score_cols + ["composite_score"]
    out[score_cols_all] = out[score_cols_all].round(4)

    # Sort best → worst
    out = out.sort_values("composite_score", ascending=False).reset_index(drop=True)

    logger.info(
        "Scoring complete: %d stocks | top=%s (%.4f) | bottom=%s (%.4f)",
        len(out),
        out.iloc[0]["ticker"] if len(out) else "—",
        out.iloc[0]["composite_score"] if len(out) else 0,
        out.iloc[-1]["ticker"] if len(out) else "—",
        out.iloc[-1]["composite_score"] if len(out) else 0,
    )

    return out
