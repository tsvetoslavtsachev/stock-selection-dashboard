"""
Factor scoring engine -- S&P 500 edition (INIT-22 M2 rework).

Normalization
-------------
Every sub-factor is expressed as a *signal where higher = better*, then run
through ONE uniform pipeline -- there is exactly one place a direction is applied
(``_signal``), which is what makes a stray sign flip catchable by a test:

  1. Direction   -- risk inputs (vol / debt / beta) are negated (lower = safer =
     better); the two remaining value MULTIPLES are inverted to yields (E/P,
     EBITDA/EV) so a cheaper multiple *and* a negative multiple (negative
     earnings / EBITDA = bad) both fall out with the correct sign. net_payout_yield
     is ALREADY a yield (higher = better), so it is not inverted. Everything else
     is already higher-better.
  2. Gaussianize -- inverse-normal (rankit) transform: percentile rank -> Phi^-1.
     Robust to outliers like a rank, but z-scaled so the tails keep magnitude
     (where the signal lives) and factors are averagable. Dissolves the winsorize
     +/- MAD scaling question entirely.
  3. Sector-neutralize -- full within-GICS-sector standardization (de-mean AND
     de-vol). The composite therefore carries NO sector bet ("most attractive
     *within its sector*"); the sector tilt is a separate, later attribution.
     Sectors with < 10 members fall back to universe standardization.

Missing data
------------
Combining is coverage-aware per stock (``_combine_z``): a missing sub-factor is
dropped and its weight redistributed across the present ones, but a bucket is
only scored when >= 50% of its weight is present -- below that it is NaN and
falls back to the NEUTRAL centre 0 (post-neutralization, 0 is the sector mean),
never a 0.5 that would beat real stocks. Present-but-partial buckets keep the
honest weighted mean of the signals they have -- there is no coverage shrink (it
would softly re-introduce the very missing-data bias this rework removes, and is
redundant with the unit-variance re-standardization below).

Composite (M2)
--------------
The composite is Trend + Quality + Value only. Risk is scored and displayed but is
NO LONGER a composite bucket -- it is a separate regime lens (the regime bet lives
in VRM, not in this cross-sectional ranker).

Each bucket is re-standardized to unit variance, then the three are blended with
EQUAL-CONTRIBUTION (ERC) weights solved on the buckets' realized covariance
(``_erc_weights``, ported from the backtest variance-share solver). The nominal
``config/scoring.yml`` composite weights (equal thirds) are the ERC TARGET shares;
the solver then finds the coefficients that make each bucket carry an equal SHARE
of the composite variance. This makes "equal weight = equal influence" true by
construction -- correlated buckets no longer silently dominate a plain weighted
mean. If the covariance degenerates the composite falls back to the nominal config
weights (renormalized over the present buckets).
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

# Buckets that make up the composite (M2: Risk is out -- it is a regime lens).
_COMPOSITE_BUCKETS = ("trend", "quality", "value")

# Committed weights live in config/scoring.yml (a PUBLIC dashboard's production
# weights must be in git, not the gitignored settings.yml). The composite weights
# are the equal-share ERC TARGETS; the sub-factor weights are equal within each
# bucket. load_weights() falls back to this if the file is absent/malformed.
_CONFIG_PATH = Path(__file__).resolve().parents[2] / "config" / "scoring.yml"

_DEFAULT_WEIGHTS: dict = {
    "composite": {"trend": 0.3334, "quality": 0.3333, "value": 0.3333},
    "subfactors": {
        "trend":   {"ret_12_1": 1.0},
        "quality": {"roe": 0.25, "oper_margin_ttm": 0.25, "fcf_margin_ttm": 0.25, "gpa": 0.25},
        "value":   {"pe_ratio": 0.3334, "ev_ebitda": 0.3333, "net_payout_yield": 0.3333},
        "risk":    {"volatility_26w": 0.3334, "debt_equity": 0.3333, "beta": 0.3333},
    },
}


def _valid_weights(data) -> bool:
    """Deep-validate a loaded config against the default SCHEMA: the composite and
    every sub-factor bucket must carry EXACTLY the expected keys with numeric
    values. A shallow "has composite+subfactors" check would let a mistyped or
    renamed sub-factor key (this file is hand-edited) pass, then crash build_scores
    with a KeyError deep in the blend instead of falling back to equal weight.

    The composite carries only the three composite buckets (Trend/Quality/Value);
    ``risk`` lives under ``subfactors`` (it is still scored) but NOT under
    ``composite`` (M2: it is a regime lens, out of the composite sum)."""
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
    file is missing or malformed (so a bare checkout -- or a fat-fingered edit --
    still scores sensibly instead of crashing the pipeline)."""
    try:
        data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    except (OSError, yaml.YAMLError):
        data = None
    if _valid_weights(data):
        return data
    logger.warning("scoring.yml missing/malformed -- using equal-weight defaults")
    return _DEFAULT_WEIGHTS


def _restandardize(s: pd.Series, scored: pd.Series | None = None) -> pd.Series:
    """Re-standardize a bucket to unit variance so that equal composite weights
    mean equal INFLUENCE -- a bucket of more-correlated inputs is otherwise more
    dispersed and silently dominates. A degenerate (zero-variance) bucket stays
    neutral.

    ``scored`` is the boolean mask of names that were genuinely scored (non-NaN
    BEFORE the neutral-0 fill). The mean/std are computed on that sub-population
    ONLY -- otherwise the neutral zeros of the unscored names shrink the std, and
    a fundamental-coverage collapse silently AMPLIFIES the scored names' influence
    (a 60/500-coverage bucket blew up 2.7x). The zeros stay neutral (0 maps to a
    small +offset only via the shared mean, negligible and correct). When the mask
    is omitted (or empty) the whole series is used -- back-compatible."""
    ref = s if scored is None else s[scored]
    ref = ref.dropna()
    if len(ref) == 0:
        return s * 0.0
    sd = float(ref.std(ddof=0))
    if sd < 1e-9:
        return s * 0.0
    return (s - ref.mean()) / sd


def _erc_weights(bucket_df: pd.DataFrame, fallback: dict,
                 max_iter: int = 10000, tol: float = 1e-10) -> dict:
    """Equal-contribution (ERC) composite weights over the re-standardized bucket
    columns of ``bucket_df`` (one column per composite bucket, rows = stocks).

    Ported from research/backtest/composites.solve_variance_share -- the SAME
    Spinu (2013) multiplicative fixed point, so the dashboard composite and the
    backtest composite share one solver. For buckets with covariance S and weights
    w, bucket i's share of the composite variance is w_i*(Sw)_i / (w'Sw); we drive
    every share to the equal target 1/k:

        w_i <- w_i * sqrt( (1/k) / ( w_i*(Sw)_i / (w'Sw) ) ) , renormalize to sum 1.

    Each step raises a bucket below target and lowers one above it, converging to
    equal shares. Deterministic (fixed start, no randomness). A degenerate
    covariance (a constant / zero-variance bucket, e.g. a tiny or single-sector
    universe) falls back to ``fallback`` renormalized over the present buckets --
    the nominal config thirds -- so the pipeline never divides by zero."""
    cols = list(bucket_df.columns)
    k = len(cols)

    def _renorm_fallback() -> dict:
        sub = {c: float(fallback.get(c, 0.0)) for c in cols}
        tot = sum(sub.values())
        if tot <= 0:
            return {c: 1.0 / k for c in cols} if k else {}
        return {c: v / tot for c, v in sub.items()}

    if k == 0:
        return {}
    if k == 1:
        return {cols[0]: 1.0}

    S = bucket_df.cov().values
    if not np.all(np.isfinite(S)) or np.any(np.diag(S) <= 0):
        logger.warning("ERC composite: degenerate covariance -- nominal config weights")
        return _renorm_fallback()

    target = 1.0 / k
    w = np.full(k, target)
    for _ in range(max_iter):
        Sw = S @ w
        total = float(w @ Sw)
        if total <= 0:
            return _renorm_fallback()
        share = (w * Sw) / total
        share = np.where(share <= 0, 1e-15, share)
        w_new = w * np.sqrt(target / share)
        w_new = w_new / w_new.sum()
        if np.max(np.abs(w_new - w)) < tol:
            w = w_new
            break
        w = w_new
    return {c: float(wi) for c, wi in zip(cols, w)}


def _safe_col(df: pd.DataFrame, col: str) -> pd.Series:
    """Return column as float, or a series of NaN if missing."""
    if col in df.columns:
        return pd.to_numeric(df[col], errors="coerce")
    return pd.Series(np.nan, index=df.index)


# -- Normalization primitives --------------------------------------------------

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
    total; otherwise NaN (neutral 0 downstream). Present-but-partial names keep the
    honest weighted mean of the signals they have -- no coverage shrink (see the
    inline note below on why an ad-hoc shrink is deliberately omitted).

    A single-component bucket (e.g. Trend = ret_12_1 only) reduces cleanly to that
    one z where present, NaN where absent -- the min-coverage floor is either fully
    met or not met at all.
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


# -- Sub-factor buckets --------------------------------------------------------

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
    NEGATIVE multiple (negative earnings / EBITDA) gives a negative yield that
    sorts to the bottom instead of masquerading as 'cheap'. x==0 -> NaN.

    Applies to E/P and EBITDA/EV. It is NOT used for net_payout_yield (already a
    yield) nor for P/B (dropped from scoring in M2; a negative-book P/B is
    neither cheap nor distress-signalling in a buyback-heavy large cap, so B/P is
    simply not scored -- see the value bucket)."""
    return 1.0 / s.where(s != 0)


def _trend_score(df: pd.DataFrame, w: dict) -> pd.Series:
    # M2: 12-1 skip-month momentum only. ret_13w is no longer scored (it added no
    # incremental signal in the backtest); it remains a displayed UI column.
    return _combine_z([
        (w["ret_12_1"], _signal(df, "ret_12_1", +1)),
    ], "trend_score")


def _quality_score(df: pd.DataFrame, w: dict) -> pd.Series:
    # M2: ROIC dropped (noisy proxy, ~0.85 z-correlation with ROE, undocumented
    # biases); GP/A added (Novy-Marx gross profitability). ROIC remains a UI column.
    return _combine_z([
        (w["roe"],             _signal(df, "roe", +1)),
        (w["oper_margin_ttm"], _signal(df, "oper_margin_ttm", +1)),
        (w["fcf_margin_ttm"],  _signal(df, "fcf_margin_ttm", +1)),
        (w["gpa"],             _signal(df, "gpa", +1)),
    ], "quality_score")


def _value_score(df: pd.DataFrame, w: dict) -> pd.Series:
    # M2 yield-form value: E/P + EBITDA/EV (both 1/multiple, higher = cheaper =
    # better, negatives sort to the bottom) + net_payout_yield (already a yield).
    # P/B and dividend_yield are no longer scored (both remain UI columns): P/B is
    # the audited defect (quality inversion + sector noise); dividend_yield is
    # dominated by the broader net payout yield.
    return _combine_z([
        (w["pe_ratio"],  _signal(df, "pe_ratio", +1, transform=_inv_yield)),
        (w["ev_ebitda"], _signal(df, "ev_ebitda", +1, transform=_inv_yield)),
        # net_payout_yield is a yield already: higher = more cash returned = better.
        # A non-payer is a real 0 (set in compute_factors), NOT missing; a genuinely
        # missing value (neither dividends nor buybacks known) stays NaN and reweights.
        (w["net_payout_yield"], _signal(df, "net_payout_yield", +1)),
    ], "value_score")


def _risk_score(df: pd.DataFrame, w: dict) -> pd.Series:
    # Lower vol / debt / beta = less risky = better -> direction -1. M2: scored and
    # displayed as a REGIME LENS, but NOT summed into the composite.
    return _combine_z([
        (w["volatility_26w"], _signal(df, "volatility_26w", -1)),
        (w["debt_equity"],    _signal(df, "debt_equity", -1)),
        (w["beta"],           _signal(df, "beta", -1)),
    ], "risk_score")


# -- Public API ----------------------------------------------------------------

def build_scores(df: pd.DataFrame, weights: dict | None = None) -> pd.DataFrame:
    """
    Compute sector-neutral factor buckets and the composite for the full universe.

    ``weights`` overrides the committed config/scoring.yml. Returns the input frame
    plus trend_score / quality_score / value_score / risk_score (sector-neutral
    z's, mean ~0 per sector, neutral == 0) and composite_score, sorted by composite
    descending.

    M2: the composite is Trend + Quality + Value only (Risk is scored + shown but is
    a separate regime lens, out of the composite). The three buckets are
    re-standardized and blended with equal-contribution (ERC) weights.
    """
    w = weights or load_weights()
    sub = w["subfactors"]
    comp = w["composite"]
    out = df.copy()

    out["trend_score"]   = _trend_score(out, sub["trend"])
    out["quality_score"] = _quality_score(out, sub["quality"])
    out["value_score"]   = _value_score(out, sub["value"])
    out["risk_score"]    = _risk_score(out, sub["risk"])

    # Capture which names were GENUINELY scored (non-NaN) per bucket BEFORE the
    # neutral-0 fill -- the re-standardization below standardizes against this
    # sub-population, not the zero-padded column (see _restandardize).
    score_cols = ["trend_score", "quality_score", "value_score", "risk_score"]
    scored_mask = {c: out[c].notna() for c in score_cols}

    # Neutral fill: a bucket that could not be scored (all inputs missing / below
    # min-coverage) sits at 0 -- the sector-neutral mean -- NOT a 0.5 that would
    # beat genuinely-scored stocks.
    out[score_cols] = out[score_cols].fillna(0.0)

    # Composite = ERC-weighted sum of the RE-STANDARDIZED composite buckets (Trend,
    # Quality, Value -- Risk excluded). Each bucket is standardized against ONLY its
    # scored names (scored_mask) so a coverage collapse does not amplify the scored
    # contribution; then equal-contribution weights are solved on the buckets'
    # covariance so each carries an equal SHARE of the composite variance. The
    # DISPLAYED bucket scores stay the interpretable sector-neutral z's (neutral ==
    # 0); the re-standardization + ERC are internal to the blend.
    restd = {
        b: _restandardize(out[f"{b}_score"], scored_mask[f"{b}_score"])
        for b in _COMPOSITE_BUCKETS
    }
    erc = _erc_weights(pd.DataFrame(restd), comp)
    out["composite_score"] = sum(erc[b] * restd[b] for b in _COMPOSITE_BUCKETS)

    all_scores = score_cols + ["composite_score"]
    out[all_scores] = out[all_scores].round(4)

    out = out.sort_values("composite_score", ascending=False).reset_index(drop=True)

    logger.info(
        "Scoring: %d stocks | ERC weights %s | top=%s (%.4f) | bottom=%s (%.4f)",
        len(out),
        {b: round(erc[b], 3) for b in _COMPOSITE_BUCKETS},
        out.iloc[0]["ticker"] if len(out) else "-",
        out.iloc[0]["composite_score"] if len(out) else 0,
        out.iloc[-1]["ticker"] if len(out) else "-",
        out.iloc[-1]["composite_score"] if len(out) else 0,
    )

    return out
