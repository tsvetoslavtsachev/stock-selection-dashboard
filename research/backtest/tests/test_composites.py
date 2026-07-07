"""
Composite weighting tests: variance-share correctness + scheme renormalization.

The load-bearing assertion (per the mandate): the variance-share solution's
contributions sum to 100% and are EQUAL across buckets.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from research.backtest import composites


def _bucket_scores(seed=11, n=800):
    """Three correlated bucket-score columns with unequal dispersions -- so an
    equal-WEIGHT composite would NOT have equal variance shares (the whole point
    of the variance-share solve)."""
    rng = np.random.default_rng(seed)
    common = rng.normal(size=n)
    a = 0.8 * common + 0.6 * rng.normal(size=n)          # moderate vol
    b = 2.0 * (0.5 * common + 0.9 * rng.normal(size=n))  # high vol
    c = 0.4 * (0.3 * common + 0.95 * rng.normal(size=n)) # low vol
    return pd.DataFrame({"trend": a, "risk": b, "value": c})


def test_variance_share_contributions_sum_to_100pct():
    df = _bucket_scores()
    w = composites.solve_variance_share(df)
    contribs = composites.variance_contributions(df, w)
    assert abs(sum(contribs.values()) - 1.0) < 1e-9


def test_variance_share_contributions_are_equal():
    df = _bucket_scores()
    w = composites.solve_variance_share(df)
    contribs = composites.variance_contributions(df, w)
    vals = list(contribs.values())
    # Equal-risk-contribution target: every bucket ~ 1/k of the variance.
    k = len(vals)
    assert max(vals) - min(vals) < 1e-4
    for v in vals:
        assert abs(v - 1.0 / k) < 1e-4


def test_variance_share_weights_sum_to_one():
    df = _bucket_scores()
    w = composites.solve_variance_share(df)
    assert abs(sum(w.values()) - 1.0) < 1e-9


def test_high_vol_bucket_gets_lower_weight():
    """The high-dispersion bucket ('risk') must receive the SMALLEST weight so its
    contribution is pulled down to parity."""
    df = _bucket_scores()
    w = composites.solve_variance_share(df)
    assert w["risk"] < w["trend"]
    assert w["risk"] < w["value"]


def test_renormalize_over_present_buckets():
    """Product weights restricted to present buckets renormalize to sum 1."""
    prod = {"trend": 0.25, "quality": 0.25, "value": 0.25, "risk": 0.25}
    w = composites.product_weights(prod, present=["trend", "risk"])
    assert set(w) == {"trend", "risk"}
    assert abs(sum(w.values()) - 1.0) < 1e-12
    assert w["trend"] == 0.5 and w["risk"] == 0.5


def test_literature_prior_renormalized():
    w = composites.literature_weights(present=["trend", "risk"])
    # 0.40 / 0.15 renormalized -> 0.7273 / 0.2727.
    assert abs(w["trend"] - 0.40 / 0.55) < 1e-9
    assert abs(w["risk"] - 0.15 / 0.55) < 1e-9
