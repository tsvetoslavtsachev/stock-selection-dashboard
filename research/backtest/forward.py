"""
Forward total-return over a fixed horizon of trading days.

On the daily TR-Close panel, the forward return from rebalance ``t`` over ``h``
trading days is ``close[t+h] / close[t] - 1`` per stock, where t and t+h are
positions on the SHARED panel trading index (so every stock's forward window
spans the same calendar span -- the correct alignment for a cross-sectional IC).

A stock with no bar exactly at t or t+h (a hole / late inception / it left the
panel) yields NaN there and drops out of that slice's cross-section. The primary
horizon is 21 trading days (~1 month, matching the monthly rebalance); 63 (~one
quarter) is the secondary / IC-decay horizon.
"""

from __future__ import annotations

import pandas as pd


def forward_return(close: pd.DataFrame, t: pd.Timestamp, horizon: int) -> pd.Series:
    """Cross-section of ``close[t+h]/close[t]-1`` per ticker (NaN where either
    endpoint bar is missing). t and t+h are positions on the shared panel index;
    if t+h runs past the panel end, returns all-NaN (caller filters those slices
    out via ``panel.testable_rebalances``)."""
    idx = close.index
    if t not in idx:
        return pd.Series(index=close.columns, dtype=float)
    i = idx.get_loc(t)
    j = i + horizon
    if j >= len(idx):
        return pd.Series(index=close.columns, dtype=float)
    start = close.iloc[i]
    end = close.iloc[j]
    return (end / start - 1.0).rename(None)
