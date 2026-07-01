"""
Factor scoring engine — S&P 500 edition (INIT-22 M1 rework).

Normalization
-------------
Every sub-factor is expressed as a *signal where higher = better*, then run
through ONE uniform pipeline — there is exactly one place a direction is applied
(``_signal``), which is what makes a stray sign flip catchable by a test:

  1. Direction   — risk inputs (vol / debt / beta) are negated (lower = safer =
     better); value multiples are inverted to yields (E/P, EBITDA/EV, B/P) so a
     cheaper multiple *and* a negative multiple (negative book / EBITDA = bad)
     both fall out with the correct sign; everything else is already higher-better.
  2. Gaussianize — inverse-normal (rankit) transform: percentile rank -> Phi^-1.
     Robust to outliers like a rank, but z-scaled so the tails keep magnitude
     (where the signal lives) and factors are averagable. Dissolves the winsorize
     +/- MAD scaling question entirely.
  3. Sector-neutralize — full within-GICS-sector standardization (de-mean AND
     de-vol). The composite therefore carries NO sector bet ("most attractive
     *within its sector*"); the sector tilt is a separate, later attribution.
     Sectors with < 10 members fall back to universe standardization.

Missing data
------------
Combining is coverage-aware per stock (``_combine_z``): a missing sub-factor is
dropped and its weight redistributed across the present ones, but a bucket is
only scored when >= 50% of its weight is present — below that it is NaN and
falls back to the NEUTRAL centre 0 (post-neutralization, 0 is the sector mean),
never a 0.5 that would beat real stocks. Low-coverage buckets are shrunk toward 0
in proportion to their coverage (a mean of fewer z's has inflated variance and
would otherwise drift to the tails).

Composite weights are applied in the publish/compose step; here every bucket is
the sector-neutral blend of its sub-factor z's.
"""

from __future__ import annotations

import logging
from statistics import NormalDist

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

_NORM = NormalDist(0.0, 1.0)

_MIN_SECTOR_N = 10     # below this, a sector standardizes against the universe
_MIN_COVERAGE = 0.50   # a bucket needs >= 50% of its weight present to score

# Composite bucket weights (Etap D reads these from config; equal-weight default).
TREND_W   = 0.25
QUALITY_W = 0.25
VALUE_W   = 0.25
RISK_W    = 0.25


def _safe_col(df: pd.DataFrame, col: str) -> pd.Series:
    """Return column as float, or a series of NaN if missing."""
    if col in df.columns:
        return pd.to_numeric(df[col], errors="coerce")
    return pd.Series(np.nan, index=df.index)


# ── Normalization primitives ──────────────────────────────────────────────────

def gaussian_rank(s: pd.Series) -> pd.Series:
    """Inverse-normal (rankit) transform of a cross-section: percentile rank ->
    Phi^-1((rank - 0.5) / n). Robust like a rank, z-scaled so tails carry
    magnitude. NaN stays NaN; a constant / all-NaN column returns all-NaN or 0."""
    r = s.rank(method="average", na_option="keep")
    n = int(r.notna().sum())
    if n == 0:
        return pd.Series(np.nan, index=s.index, dtype=float)
    p = (r - 0.5) / n
    return p.map(lambda x: _NORM.inv_cdf(x) if pd.notna(x) else np.nan).astype(float)


def sector_neutralize(z: pd.Series, sector: pd.Series, min_n: int = _MIN_SECTOR_N) -> pd.Series:
    """Full within-sector standardization (de-mean + de-vol). Sectors with < min_n
    scored members fall back to universe standardization; a degenerate (zero-var)
    block is only de-meaned. NaN in z stays NaN."""
    out = pd.Series(np.nan, index=z.index, dtype=float)
    uni = z.dropna()
    u_mean = float(uni.mean()) if len(uni) else 0.0
    u_std = float(uni.std(ddof=0)) if len(uni) else 0.0
    for _, members in z.groupby(sector, dropna=False).groups.items():
        block = z.loc[members]
        vals = block.dropna()
        if len(vals) >= min_n and vals.std(ddof=0) > 1e-9:
            out.loc[members] = (block - vals.mean()) / vals.std(ddof=0)
        elif u_std > 1e-9:
            out.loc[members] = (block - u_mean) / u_std
        else:
            out.loc[members] = block - (vals.mean() if len(vals) else 0.0)
    return out


def _combine_z(components: list[tuple[float, pd.Series]], name: str,
               min_coverage: float = _MIN_COVERAGE) -> pd.Series:
    """Coverage-aware weighted blend of sector-neutral z components.

    A missing component is dropped and its weight redistributed across the present
    ones. A stock scores only when the present weight is >= ``min_coverage`` of the
    total; otherwise NaN (neutral 0 downstream). The blend is shrunk toward 0 by
    the coverage fraction so low-coverage names (a mean of fewer, higher-variance
    z's) do not drift into the tails on thin data.
    """
    index = components[0][1].index
    num = pd.Series(0.0, index=index)
    w_present = pd.Series(0.0, index=index)
    total_w = sum(w for w, _ in components)
    for weight, z in components:
        num = num + z.fillna(0.0) * weight
        w_present = w_present + weight * z.notna().astype(float)
    cover = w_present / total_w
    blend = num / w_present.where(w_present > 0, np.nan)
    blend = blend.where(cover >= min_coverage, np.nan)
    return (blend * cover).rename(name)


# ── Sub-factor buckets ────────────────────────────────────────────────────────

def _signal(df: pd.DataFrame, col: str, direction: int, transform=None) -> pd.Series:
    """One sub-factor as a sector-neutral 'higher = better' z. ``direction`` -1
    negates a lower-is-better raw (risk); ``transform`` overrides direction for
    value yields. This is the SINGLE place a sign is applied."""
    raw = _safe_col(df, col)
    if transform is not None:
        raw = transform(raw)
    elif direction < 0:
        raw = -raw
    z = gaussian_rank(raw)
    return sector_neutralize(z, df.get("sector", pd.Series("", index=df.index)))


def _inv_yield(s: pd.Series) -> pd.Series:
    """Multiple -> yield (1/x): a cheaper multiple gives a higher yield, and a
    NEGATIVE multiple (negative earnings/book/EBITDA) gives a negative yield that
    sorts to the bottom instead of masquerading as 'cheap'. x==0 -> NaN."""
    return 1.0 / s.where(s != 0)


def _trend_score(df: pd.DataFrame) -> pd.Series:
    return _combine_z([
        (0.70, _signal(df, "ret_12_1", +1)),   # 12-1 skip-month momentum (primary)
        (0.30, _signal(df, "ret_13w", +1)),    # 13-week return (responsiveness)
    ], "trend_score")


def _quality_score(df: pd.DataFrame) -> pd.Series:
    return _combine_z([
        (0.30, _signal(df, "roe", +1)),
        (0.25, _signal(df, "oper_margin_ttm", +1)),
        (0.25, _signal(df, "fcf_margin_ttm", +1)),
        (0.20, _signal(df, "roic", +1)),
    ], "quality_score")


def _value_score(df: pd.DataFrame) -> pd.Series:
    # Yield-form: higher = cheaper = better; negatives sort to the bottom.
    return _combine_z([
        (0.35, _signal(df, "pe_ratio", +1, transform=_inv_yield)),
        (0.30, _signal(df, "ev_ebitda", +1, transform=_inv_yield)),
        (0.20, _signal(df, "pb_ratio", +1, transform=_inv_yield)),
        # No dividend = zero yield (a real value), not missing data -> fill 0.
        (0.15, _signal(df, "dividend_yield", +1, transform=lambda s: s.fillna(0.0))),
    ], "value_score")


def _risk_score(df: pd.DataFrame) -> pd.Series:
    # Lower vol / debt / beta = less risky = better -> direction -1.
    return _combine_z([
        (0.50, _signal(df, "volatility_26w", -1)),
        (0.30, _signal(df, "debt_equity", -1)),
        (0.20, _signal(df, "beta", -1)),
    ], "risk_score")


# ── Public API ────────────────────────────────────────────────────────────────

def build_scores(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute sector-neutral factor buckets and the composite for the full universe.

    Returns the input frame plus trend_score / quality_score / value_score /
    risk_score (sector-neutral z's, mean ~0 per sector) and composite_score,
    sorted by composite descending. A bucket with too little data sits at the
    neutral centre 0, never above a real stock.
    """
    out = df.copy()

    out["trend_score"]   = _trend_score(out)
    out["quality_score"] = _quality_score(out)
    out["value_score"]   = _value_score(out)
    out["risk_score"]    = _risk_score(out)

    # Neutral fill: a bucket that could not be scored (all inputs missing / below
    # min-coverage) sits at 0 — the sector-neutral mean — NOT a 0.5 that would
    # beat genuinely-scored stocks.
    score_cols = ["trend_score", "quality_score", "value_score", "risk_score"]
    out[score_cols] = out[score_cols].fillna(0.0)

    # Composite: equal-weight mean of the sector-neutral buckets (Etap D adds the
    # bucket re-standardization + config-driven weights).
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
