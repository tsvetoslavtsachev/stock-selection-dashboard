"""
Factor scoring engine — S&P 500 edition.

Factor weights
--------------
Trend score   = 40% rank(ret_13w) + 30% rank(ret_26w) + 30% rank(ret_52w)
Quality score = 30% rank(roe) + 25% rank(oper_margin_ttm) + 25% rank(fcf_margin_ttm) + 20% rank(roic)
Value score   = 35% inv_rank(pe_ratio) + 30% inv_rank(ev_ebitda) + 20% inv_rank(pb_ratio) + 15% rank(dividend_yield)
Risk score    = 50% inv_rank(volatility_26w) + 30% inv_rank(debt_equity) + 20% inv_rank(beta)

Composite     = 30% Trend + 30% Quality + 25% Value + 15% Risk

All inputs are percentile-ranked [0,1] before weighting.
"""

from __future__ import annotations

import logging
import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ── Composite weights ────────────────────────────────────────────────────────
TREND_W   = 0.30
QUALITY_W = 0.30
VALUE_W   = 0.25
RISK_W    = 0.15


def percentile_rank(series: pd.Series) -> pd.Series:
    """Convert to percentile ranks [0, 1]. NaN stays NaN."""
    return series.rank(method="average", ascending=True, pct=True, na_option="keep")


def _safe_col(df: pd.DataFrame, col: str) -> pd.Series:
    """Return column as float, or a series of NaN if missing."""
    if col in df.columns:
        return pd.to_numeric(df[col], errors="coerce")
    return pd.Series(np.nan, index=df.index)


# ── Sub-factor builders ──────────────────────────────────────────────────────

def _trend_score(df: pd.DataFrame) -> pd.Series:
    r13 = percentile_rank(_safe_col(df, "ret_13w"))
    r26 = percentile_rank(_safe_col(df, "ret_26w"))
    r52 = percentile_rank(_safe_col(df, "ret_52w"))
    return (0.40 * r13 + 0.30 * r26 + 0.30 * r52).rename("trend_score")


def _quality_score(df: pd.DataFrame) -> pd.Series:
    roe    = percentile_rank(_safe_col(df, "roe"))
    opm    = percentile_rank(_safe_col(df, "oper_margin_ttm"))
    fcfm   = percentile_rank(_safe_col(df, "fcf_margin_ttm"))
    roic   = percentile_rank(_safe_col(df, "roic"))
    return (0.30 * roe + 0.25 * opm + 0.25 * fcfm + 0.20 * roic).rename("quality_score")


def _value_score(df: pd.DataFrame) -> pd.Series:
    # Lower PE/EV_EBITDA/PB = cheaper = higher score → invert
    pe     = 1.0 - percentile_rank(_safe_col(df, "pe_ratio"))
    ev_eb  = 1.0 - percentile_rank(_safe_col(df, "ev_ebitda"))
    pb     = 1.0 - percentile_rank(_safe_col(df, "pb_ratio"))
    # Higher dividend = better → normal rank
    div_y  = percentile_rank(_safe_col(df, "dividend_yield"))
    return (0.35 * pe + 0.30 * ev_eb + 0.20 * pb + 0.15 * div_y).rename("value_score")


def _risk_score(df: pd.DataFrame) -> pd.Series:
    # Lower vol / debt / beta = less risky = higher score → invert
    vol    = 1.0 - percentile_rank(_safe_col(df, "volatility_26w"))
    debt   = 1.0 - percentile_rank(_safe_col(df, "debt_equity"))
    beta   = 1.0 - percentile_rank(_safe_col(df, "beta"))
    return (0.50 * vol + 0.30 * debt + 0.20 * beta).rename("risk_score")


# ── Public API ────────────────────────────────────────────────────────────────

def build_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute all factor scores and composite for the full universe.

    Parameters
    ----------
    df : pd.DataFrame with columns:
        ticker, name, sector, cik,
        ret_13w, ret_26w, ret_52w, volatility_26w,
        pe_ratio, pb_ratio, ev_ebitda, ev_ebit,
        roe, roic, debt_equity,
        eps_ttm, dividend_yield,
        revenue_growth_ttm, oper_margin_ttm, gross_margin_ttm,
        fcf_margin_ttm, market_cap, beta

    Returns
    -------
    pd.DataFrame — original + trend_score, quality_score, value_score,
    risk_score, composite_score. Sorted by composite descending.
    """
    out = df.copy()

    out["trend_score"]   = _trend_score(out)
    out["quality_score"] = _quality_score(out)
    out["value_score"]   = _value_score(out)
    out["risk_score"]    = _risk_score(out)

    # Fill NaN scores (e.g. all-NaN columns) with 0.5 (neutral)
    score_cols = ["trend_score", "quality_score", "value_score", "risk_score"]
    out[score_cols] = out[score_cols].fillna(0.5)

    out["composite_score"] = (
        TREND_W   * out["trend_score"]
        + QUALITY_W * out["quality_score"]
        + VALUE_W   * out["value_score"]
        + RISK_W    * out["risk_score"]
    )

    all_scores = score_cols + ["composite_score"]
    out[all_scores] = out[all_scores].round(4)

    out = out.sort_values("composite_score", ascending=False).reset_index(drop=True)

    logger.info(
        "Scoring: %d stocks | top=%s (%.4f) | bottom=%s (%.4f)",
        len(out),
        out.iloc[0]["ticker"] if len(out) else "—",
        out.iloc[0]["composite_score"] if len(out) else 0,
        out.iloc[-1]["ticker"] if len(out) else "—",
        out.iloc[-1]["composite_score"] if len(out) else 0,
    )

    return out
