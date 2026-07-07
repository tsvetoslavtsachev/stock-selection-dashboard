"""
Composite weighting-scheme experiments over the available factor set.

A composite is a weighted blend of bucket scores (each a cross-sectional z). This
module compares three weighting schemes on the SAME slices, so a difference is
attributable to the weights alone:

  (a) product        -- the committed config/scoring.yml composite weights.
  (b) variance_share -- weights that EQUALIZE each bucket's realized contribution
                        to the variance of the composite. See solve_variance_share.
  (c) literature     -- a fixed illustrative prior (trend .40 / quality .30 /
                        value .15 / risk .15), labeled "illustrative prior, not
                        optimized" -- a sanity anchor, never a recommendation.

PRICE-ONLY RUN: with no fundamentals panel, only the price buckets exist (trend =
{ret_12_1, ret_13w}; risk = {volatility_26w, beta_52w}). The schemes are then
renormalized over the AVAILABLE buckets (a) and (c), and (b) is solved on the
present buckets' covariance. The full four-bucket run waits on Interface P.

VARIANCE-SHARE SOLUTION. For buckets with covariance matrix S and weights w, the
composite variance is w'Sw and bucket i's variance contribution is w_i * (Sw)_i.
We want every bucket's SHARE of the variance equal (target b_i = 1/k). This is the
classic Equal Risk Contribution (ERC) problem; we solve it with the multiplicative
fixed-point update of Spinu (2013) -- convergent, no scipy needed:

    w_i <- w_i * sqrt( b_i / ( w_i*(Sw)_i / (w'Sw) ) ) , then renormalize to sum 1.

Each step raises a bucket whose current variance share is below target and lowers
one above it, converging to exactly equal shares. (The naive w_i <- w_i/(Sw)_i
update is NOT the ERC solution -- it collapses to a corner.) The solver's output is
pinned by the correctness test: contributions sum to 100% AND are equal.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# Illustrative literature prior (NOT optimized). Momentum/trend is the most
# robustly documented cross-sectional premium, hence the largest slice; the split
# is a round-number prior, deliberately not tuned to this sample.
LITERATURE_PRIOR = {"trend": 0.40, "quality": 0.30, "value": 0.15, "risk": 0.15}


def renormalize(weights: dict, present: list) -> dict:
    """Restrict ``weights`` to the ``present`` buckets and renormalize to sum 1.
    An all-absent intersection falls back to equal weight over ``present``."""
    sub = {k: float(weights[k]) for k in present if k in weights}
    tot = sum(sub.values())
    if tot <= 0:
        return {k: 1.0 / len(present) for k in present} if present else {}
    return {k: v / tot for k, v in sub.items()}


def product_weights(scoring_yml_composite: dict, present: list) -> dict:
    """Scheme (a): the committed product composite weights, renormalized over the
    buckets actually present in this run."""
    return renormalize(scoring_yml_composite, present)


def literature_weights(present: list) -> dict:
    """Scheme (c): the illustrative literature prior, renormalized over present
    buckets."""
    return renormalize(LITERATURE_PRIOR, present)


def solve_variance_share(
    bucket_scores: pd.DataFrame, max_iter: int = 10000, tol: float = 1e-10
) -> dict:
    """Scheme (b): equal-variance-contribution (ERC) weights over the bucket
    score columns of ``bucket_scores`` (rows = stock-slices stacked across all
    rebalance dates, columns = buckets). Long-only, sums to 1.

    Returns {bucket -> weight}. Solved by the Spinu (2013) multiplicative ERC
    fixed point on the buckets' covariance S (pairwise-complete):

        w_i <- w_i * sqrt( (1/k) / ( w_i*(Sw)_i / (w'Sw) ) )

    which converges to equal variance shares. Degenerate (zero-variance) buckets
    fall back to equal weight."""
    cols = list(bucket_scores.columns)
    S = bucket_scores.cov().values
    k = len(cols)
    if k == 0:
        return {}
    if k == 1:
        return {cols[0]: 1.0}
    # Guard a degenerate covariance (a constant bucket): equal-weight fallback.
    if not np.all(np.isfinite(S)) or np.any(np.diag(S) <= 0):
        logger.warning("variance_share: degenerate covariance -> equal weight")
        return {c: 1.0 / k for c in cols}
    target = 1.0 / k
    w = np.full(k, target)
    for _ in range(max_iter):
        Sw = S @ w
        total = float(w @ Sw)
        if total <= 0:
            break
        share = (w * Sw) / total            # current variance share per bucket
        share = np.where(share <= 0, 1e-15, share)
        w_new = w * np.sqrt(target / share)
        w_new = w_new / w_new.sum()
        if np.max(np.abs(w_new - w)) < tol:
            w = w_new
            break
        w = w_new
    return {c: float(wi) for c, wi in zip(cols, w)}


def variance_contributions(bucket_scores: pd.DataFrame, weights: dict) -> dict:
    """Each bucket's SHARE of the composite variance under ``weights`` (fractions
    summing to ~1). Contribution_i = w_i*(Sw)_i / (w'Sw). Used by the correctness
    test (equal shares for the variance_share solution; shares sum to 100%)."""
    cols = list(bucket_scores.columns)
    w = np.array([weights[c] for c in cols])
    S = bucket_scores.cov().values
    Sw = S @ w
    total = float(w @ Sw)
    if total <= 0:
        return {c: float("nan") for c in cols}
    return {c: float(w[i] * Sw[i] / total) for i, c in enumerate(cols)}


def composite_score(bucket_scores: pd.DataFrame, weights: dict) -> pd.Series:
    """Weighted-sum composite of the (already unit-comparable z) bucket columns.
    Bucket columns are cross-sectional z's; a straight weighted sum keeps 'equal
    weight == equal influence' (the buckets are pre-standardized upstream)."""
    cols = [c for c in bucket_scores.columns if c in weights]
    w = pd.Series({c: weights[c] for c in cols})
    return bucket_scores[cols].mul(w, axis=1).sum(axis=1, min_count=1)
