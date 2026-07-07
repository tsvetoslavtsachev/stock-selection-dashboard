"""
Interface P CONSUMER test on a synthetic accessor -- proves the signal-builder
code path without the real EDGAR panel (which a separate agent builds).

We stub the Interface P accessor as a callable ``(panel, as_of) -> wide DataFrame``
with the exact column contract (stock levels + <flow>_ttm), then check that the
consumer assembles ep / bp / fcf_yield / net_payout_yield / gpa / roe /
oper_margin / ev_ebitda_yield with the correct arithmetic.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from research.backtest.fundamentals_signals import (
    FUNDAMENTAL_SIGNALS,
    FundamentalsConsumer,
)


def _wide_fixture():
    """One-ticker wide frame in the Interface P as_known_at shape."""
    return pd.DataFrame(
        {
            "stockholders_equity": [500.0],
            "total_assets": [2000.0],
            "current_liabilities": [300.0],
            "cash_and_equivalents": [100.0],
            "total_debt": [400.0],
            "shares_outstanding": [10.0],
            "revenues_ttm": [1000.0],
            "gross_profit_ttm": [600.0],
            "operating_income_ttm": [200.0],
            "net_income_ttm": [150.0],
            "depreciation_amortization_ttm": [50.0],
            "operating_cash_flow_ttm": [250.0],
            "capex_ttm": [80.0],
            "buybacks_ttm": [30.0],
            "dividends_paid_ttm": [20.0],
        },
        index=pd.Index(["AAA"], name="ticker"),
    )


def _consumer():
    wide = _wide_fixture()
    # Accessor ignores as_of here (fixture is a single snapshot); the PIT slicing
    # itself is the sibling module's own responsibility, tested there.
    def accessor(panel, as_of):
        return wide
    return FundamentalsConsumer(panel=object(), accessor=accessor)


def test_signals_present_and_named():
    c = _consumer()
    close_t = pd.Series({"AAA": 100.0})   # mktcap = 10 shares * 100 = 1000
    out = c.signals_at(pd.Timestamp("2023-06-30"), close_t)
    assert list(out.columns) == list(FUNDAMENTAL_SIGNALS)
    assert list(out.index) == ["AAA"]


def test_signal_arithmetic():
    c = _consumer()
    close_t = pd.Series({"AAA": 100.0})
    mktcap = 10.0 * 100.0                 # 1000
    out = c.signals_at(pd.Timestamp("2023-06-30"), close_t).loc["AAA"]

    assert out["ep"] == np.float64(150.0 / mktcap)                 # 0.15
    assert out["bp"] == np.float64(500.0 / mktcap)                 # 0.50
    assert out["fcf_yield"] == np.float64((250.0 - 80.0) / mktcap) # 0.17
    assert out["net_payout_yield"] == np.float64((20.0 + 30.0) / mktcap)  # 0.05
    assert out["gpa"] == np.float64(600.0 / 2000.0)               # 0.30
    assert out["roe"] == np.float64(150.0 / 500.0)                # 0.30
    assert out["oper_margin"] == np.float64(200.0 / 1000.0)       # 0.20
    # EV = mktcap + debt - cash = 1000 + 400 - 100 = 1300; EBITDA = 200 + 50 = 250.
    assert out["ev_ebitda_yield"] == np.float64(250.0 / 1300.0)


def test_unavailable_returns_empty():
    c = FundamentalsConsumer(panel=None, accessor=None)
    assert c.available is False
    assert c.signals_at(pd.Timestamp("2023-06-30"), pd.Series(dtype=float)).empty


def test_missing_input_is_nan_not_crash():
    """A ticker with no shares_outstanding -> mktcap NaN -> yield NaN, no crash."""
    wide = _wide_fixture()
    wide.loc["BBB"] = {c: np.nan for c in wide.columns}
    def accessor(panel, as_of):
        return wide
    c = FundamentalsConsumer(panel=object(), accessor=accessor)
    out = c.signals_at(pd.Timestamp("2023-06-30"), pd.Series({"AAA": 100.0, "BBB": 50.0}))
    assert np.isnan(out.loc["BBB", "ep"])
    assert not np.isnan(out.loc["AAA", "ep"])
