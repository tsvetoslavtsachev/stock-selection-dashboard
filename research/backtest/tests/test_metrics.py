"""
Metrics tests: bootstrap determinism, IC sign/summary sanity, quintile spread.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.backtest import metrics


def _ic_series(seed=1, n=48):
    rng = np.random.default_rng(seed)
    idx = pd.date_range("2021-08-31", periods=n, freq="ME")
    return pd.Series(rng.normal(0.03, 0.10, size=n), index=idx)


def test_bootstrap_determinism():
    """Same series + same seed -> bit-identical CI across two calls."""
    ic = _ic_series()
    ci1 = metrics.block_bootstrap_ci(ic, seed=metrics.BOOTSTRAP_SEED)
    ci2 = metrics.block_bootstrap_ci(ic, seed=metrics.BOOTSTRAP_SEED)
    assert ci1 == ci2


def test_bootstrap_seed_changes_result():
    """A different seed generally changes the CI (guards a frozen/ignored seed)."""
    ic = _ic_series()
    ci_a = metrics.block_bootstrap_ci(ic, seed=1)
    ci_b = metrics.block_bootstrap_ci(ic, seed=2)
    assert ci_a != ci_b


def test_spearman_ic_perfect_rank():
    """A score that ranks identically to the forward return gives IC == 1."""
    score = pd.Series({"a": 3.0, "b": 2.0, "c": 1.0, "d": 0.0})
    fwd = pd.Series({"a": 0.30, "b": 0.20, "c": 0.10, "d": 0.00})
    assert metrics.spearman_ic(score, fwd) == 1.0


def test_spearman_ic_inverse_rank():
    score = pd.Series({"a": 3.0, "b": 2.0, "c": 1.0, "d": 0.0})
    fwd = pd.Series({"a": 0.00, "b": 0.10, "c": 0.20, "d": 0.30})
    assert metrics.spearman_ic(score, fwd) == -1.0


def test_ic_summary_power_yardstick():
    """ic_for_t2 == 2*std/sqrt(n) exactly."""
    ic = _ic_series(n=36)
    summ = metrics.ic_summary(ic)
    expected = 2.0 * summ["std"] / np.sqrt(summ["n"])
    assert summ["ic_for_t2"] == expected


def test_newey_west_reduces_to_plain_at_lag0():
    """At lag 0 the NW SE == the plain SE of the mean (sqrt(gamma0/n))."""
    ic = _ic_series()
    x = ic.values
    nw0 = metrics._newey_west_se(x, lag=0)
    plain = np.sqrt(np.var(x, ddof=0) / len(x))
    assert nw0 == pytest.approx(plain, rel=1e-12)


def test_quintile_spread_direction():
    """A score positively related to forward return -> positive mean spread."""
    rng = np.random.default_rng(3)
    scores_by_date, fwd_by_date = {}, {}
    for k in range(6):
        t = pd.Timestamp("2022-01-31") + pd.offsets.MonthEnd(k)
        n = 50
        names = [f"s{i}" for i in range(n)]
        sc = pd.Series(rng.normal(size=n), index=names)
        fw = 0.05 * sc + pd.Series(rng.normal(0, 0.01, n), index=names)  # aligned
        scores_by_date[t] = sc
        fwd_by_date[t] = fw
    spread = metrics.quintile_spread_series(scores_by_date, fwd_by_date)
    assert spread["spread"].mean() > 0
