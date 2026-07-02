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

Composite = weighted sum of the buckets after each is re-standardized to unit
variance (so equal weight -> equal influence); the weights come from the committed
``config/scoring.yml`` (equal-weight default, since M1 ships no backtest to justify
anything else).
"""

from __future__ import annotations

import logging
from pathlib import Path
from statistics import NormalDist

import numpy as np
import pandas as pd
import yaml

logger = logging.getLogger(__name__)

_NORM = NormalDist(0.0, 1.0)

_MIN_SECTOR_N = 10     # below this, a sector standardizes against the universe
_MIN_COVERAGE = 0.50   # a bucket needs >= 50% of its weight present to score

# Committed weights live in config/scoring.yml (a PUBLIC dashboard's production
# weights must be in git, not the gitignored settings.yml). Default = equal weight
# at BOTH levels — the honest no-information prior absent a validated IC (M1 ships
# no backtest). load_weights() falls back to this if the file is absent/malformed.
_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "scoring.yml"

_DEFAULT_WEIGHTS: dict = {
    "composite": {"trend": 0.25, "quality": 0.25, "value": 0.25, "risk": 0.25},
    "subfactors": {
        "trend":   {"ret_12_1": 0.5, "ret_13w": 0.5},
        "quality": {"roe": 0.25, "oper_margin_ttm": 0.25, "fcf_margin_ttm": 0.25, "roic": 0.25},
        "value":   {"pe_ratio": 0.25, "ev_ebitda": 0.25, "pb_ratio": 0.25, "dividend_yield": 0.25},
        "risk":    {"volatility_26w": 1 / 3, "debt_equity": 1 / 3, "beta": 1 / 3},
    },
}


def _valid_weights(data) -> bool:
    """Deep-validate a loaded config against the default SCHEMA: the composite and
    every sub-factor bucket must carry EXACTLY the expected keys with numeric
    values. A shallow "has composite+subfactors" check would let a mistyped or
    renamed sub-factor key (this file is hand-edited) pass, then crash build_scores
    with a KeyError deep in the blend instead of falling back to equal weight."""
    if not isinstance(data, dict):
        return False
    try:
        if set(data.get("composite", {})) != set(_DEFAULT_WEIGHTS["composite"]):
            return False
        if set(data.get("subfactors", {})) != set(_DEFAULT_WEIGHTS["subfactors"]):
            return False
        for bucket, subs in _DEFAULT_WEIGHTS["subfactors"].items():
            got = data["subfactors"][bucket]
            if set(got) != set(subs):
                return False
            for v in got.values():
                float(v)
        for v in data["composite"].values():
            float(v)
    except (KeyError, TypeError, ValueError, AttributeError):
        return False
    return True


def load_weights(path: Path = _CONFIG_PATH) -> dict:
    """Read committed scoring weights; fall back to the equal-weight default if the
    file is missing or malformed (so a bare checkout — or a fat-fingered edit —
    still scores sensibly instead of crashing the pipeline)."""
    try:
        data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        data = None
    if _valid_weights(data):
        return data
    logger.warning("scoring.yml missing/malformed — using equal-weight defaults")
    return _DEFAULT_WEIGHTS


def _restandardize(s: pd.Series) -> pd.Series:
    """Re-standardize a bucket to unit variance so that equal composite weights
    mean equal INFLUENCE — a bucket of more-correlated inputs is otherwise more
    dispersed and silently dominates. A degenerate (zero-variance) bucket stays
    neutral."""
    sd = float(s.std(ddof=0))
    if sd < 1e-9:
        return s * 0.0
    return (s - s.mean()) / sd


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
    # Below the min-coverage floor the blend is dropped to NaN (-> neutral 0
    # downstream). Present-but-partial names keep the honest weighted mean of the
    # signals they DO have: an ad-hoc coverage shrink here would only pull them
    # toward neutral (a soft return of the very bias this rewrite removes) and is
    # anyway redundant with the unit-variance re-standardization in build_scores.
    blend = blend.where(cover >= min_coverage, np.nan)
    return blend.rename(name)


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


def _trend_score(df: pd.DataFrame, w: dict) -> pd.Series:
    return _combine_z([
        (w["ret_12_1"], _signal(df, "ret_12_1", +1)),   # 12-1 skip-month momentum
        (w["ret_13w"],  _signal(df, "ret_13w", +1)),    # 13-week return (responsiveness)
    ], "trend_score")


def _quality_score(df: pd.DataFrame, w: dict) -> pd.Series:
    return _combine_z([
        (w["roe"],             _signal(df, "roe", +1)),
        (w["oper_margin_ttm"], _signal(df, "oper_margin_ttm", +1)),
        (w["fcf_margin_ttm"],  _signal(df, "fcf_margin_ttm", +1)),
        (w["roic"],            _signal(df, "roic", +1)),
    ], "quality_score")


def _value_score(df: pd.DataFrame, w: dict) -> pd.Series:
    # Yield-form: higher = cheaper = better; negatives sort to the bottom.
    return _combine_z([
        (w["pe_ratio"],  _signal(df, "pe_ratio", +1, transform=_inv_yield)),
        (w["ev_ebitda"], _signal(df, "ev_ebitda", +1, transform=_inv_yield)),
        (w["pb_ratio"],  _signal(df, "pb_ratio", +1, transform=_inv_yield)),
        # No dividend = zero yield (a real value), not missing data -> fill 0.
        (w["dividend_yield"], _signal(df, "dividend_yield", +1, transform=lambda s: s.fillna(0.0))),
    ], "value_score")


def _risk_score(df: pd.DataFrame, w: dict) -> pd.Series:
    # Lower vol / debt / beta = less risky = better -> direction -1.
    return _combine_z([
        (w["volatility_26w"], _signal(df, "volatility_26w", -1)),
        (w["debt_equity"],    _signal(df, "debt_equity", -1)),
        (w["beta"],           _signal(df, "beta", -1)),
    ], "risk_score")


# ── Public API ────────────────────────────────────────────────────────────────

def build_scores(df: pd.DataFrame, weights: dict | None = None) -> pd.DataFrame:
    """
    Compute sector-neutral factor buckets and the composite for the full universe.

    ``weights`` overrides the committed config/scoring.yml (equal-weight default).
    Returns the input frame plus trend_score / quality_score / value_score /
    risk_score (sector-neutral z's, mean ~0 per sector, neutral == 0) and
    composite_score, sorted by composite descending.
    """
    w = weights or load_weights()
    sub = w["subfactors"]
    comp = w["composite"]
    out = df.copy()

    out["trend_score"]   = _trend_score(out, sub["trend"])
    out["quality_score"] = _quality_score(out, sub["quality"])
    out["value_score"]   = _value_score(out, sub["value"])
    out["risk_score"]    = _risk_score(out, sub["risk"])

    # Neutral fill: a bucket that could not be scored (all inputs missing / below
    # min-coverage) sits at 0 — the sector-neutral mean — NOT a 0.5 that would
    # beat genuinely-scored stocks.
    score_cols = ["trend_score", "quality_score", "value_score", "risk_score"]
    out[score_cols] = out[score_cols].fillna(0.0)

    # Composite = weighted sum of RE-STANDARDIZED buckets, so equal weights mean
    # equal influence. The DISPLAYED bucket scores stay the interpretable sector-
    # neutral z's (neutral == 0); the re-standardization is internal to the blend.
    out["composite_score"] = (
        comp["trend"]   * _restandardize(out["trend_score"])
        + comp["quality"] * _restandardize(out["quality_score"])
        + comp["value"]   * _restandardize(out["value_score"])
        + comp["risk"]    * _restandardize(out["risk_score"])
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
