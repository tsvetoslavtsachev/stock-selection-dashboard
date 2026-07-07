"""
Signal parity: the framework's price signals == the PRODUCT's _price_features.

The whole framework is only trustworthy if a signal it computes at date t equals
the value the live dashboard would compute from the same series. We pin that here
against ``src.jobs.compute_factors._price_features`` on a synthetic daily series:
ret_12_1, ret_13w, volatility_26w must match to rounding.

Synthetic series: a deterministic geometric random walk on business days, long
enough (>2y) for every window. Because production reads the newest bar as the
as-of point, we evaluate the framework at t = the series' last date.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from research.backtest import signals
from src.jobs.compute_factors import _price_features


def _synthetic_series(n=700, seed=7):
    rng = np.random.default_rng(seed)
    idx = pd.bdate_range("2021-01-01", periods=n)
    rets = rng.normal(0.0004, 0.012, size=n)
    price = 100.0 * np.exp(np.cumsum(rets))
    return pd.Series(price, index=idx)


def test_ret_12_1_parity():
    s = _synthetic_series()
    prod = _price_features(s)["ret_12_1"]
    ours = signals.ret_12_1(s, s.index[-1])
    assert prod == pytest.approx(ours, abs=1e-6)


def test_ret_13w_parity():
    s = _synthetic_series()
    prod = _price_features(s)["ret_13w"]
    ours = signals.ret_13w(s, s.index[-1])
    assert prod == pytest.approx(ours, abs=1e-6)


def test_volatility_26w_parity():
    s = _synthetic_series()
    prod = _price_features(s)["volatility_26w"]
    ours = signals.volatility_26w(s, s.index[-1])
    assert prod == pytest.approx(ours, abs=1e-6)


def test_asof_indexing_matches_tail():
    """The as-of series up to t (dropna) is exactly the tail production reads, so a
    signal evaluated at the last date uses the identical -253 / -22 bars."""
    s = _synthetic_series()
    asof = signals._asof_series(s, s.index[-1])
    pd.testing.assert_series_equal(asof, s.dropna())


def test_ret_12_1_insufficient_history_is_nan():
    s = _synthetic_series(n=100)  # < 253 bars
    assert np.isnan(signals.ret_12_1(s, s.index[-1]))
