"""
Factor signals recomputed on the panel -- EXACT product definitions.

Every price signal here is the SAME arithmetic as the production pipeline
(``src/jobs/compute_factors.py :: _price_features``), evaluated at an arbitrary
as-of date ``t`` instead of only "today". The parity test
(``tests/test_parity.py``) pins this: on a shared window the framework's
ret_12_1 / ret_13w / volatility_26w equal the production values to rounding.

Definitions (t = the rebalance date; series = daily TR-Close up to and
including t):

  ret_12_1      = close[t-21] / close[t-252] - 1        (trading-day skip-month)
  ret_13w       = W-FRI resample, 13 weekly steps point-to-point
  volatility_26w= 26 W-FRI log-returns, std(pop) * sqrt(52)
  beta_52w      = OLS slope of 52 W-FRI stock returns on the market proxy's 52
                  W-FRI returns (px_spy_daily if in the panel/archive, else the
                  equal-weight universe mean return -- documented per run)

PRODUCT INDEXING NOTE (verified against _price_features): production takes
``series.iloc[-253]`` and ``series.iloc[-22]`` on the daily series, i.e. the
252-trading-day and 21-trading-day lookbacks are counted on the AVAILABLE (non-
NaN, per-stock) daily bars, NOT on the shared calendar index. We reproduce that
exactly: the per-stock series is dropna()'d, then positionally indexed from its
own tail as of t. This is what makes the parity hold.

Normalization (gaussian_rank, sector_neutralize) is IMPORTED from
``src.lib.scoring`` -- never re-implemented -- so a signal tested "neutral" here
is neutralized by the identical production code.
"""

from __future__ import annotations

import logging

import numpy as np
import pandas as pd

# Import the production normalization primitives -- do NOT re-implement them.
from src.lib.scoring import gaussian_rank, sector_neutralize  # noqa: F401 (re-exported)

logger = logging.getLogger(__name__)

# The price factor set this framework computes on the panel. quality/value need
# fundamentals (Interface P) and are added only under --with-fundamentals.
PRICE_FACTORS = ("ret_12_1", "ret_13w", "volatility_26w", "beta_52w")

# Direction: +1 higher-is-better, -1 lower-is-better (risk). Matches scoring.py:
# ret_* are +1; volatility_26w and beta are risk (-1). Used when a signal is
# turned into a "higher = better" score and when building bucket composites.
FACTOR_DIRECTION = {
    "ret_12_1": +1,
    "ret_13w": +1,
    "volatility_26w": -1,
    "beta_52w": -1,
}


def _asof_series(close_col: pd.Series, t: pd.Timestamp) -> pd.Series:
    """The per-stock daily series up to and including ``t``, NaNs dropped.

    Production reads each stock's own CSV (already gap-free per stock) and indexes
    from its tail. On the panel a stock has NaNs where it had no bar (late
    inception / a hole); dropping them reproduces the production per-stock series
    exactly, so the positional -253 / -22 / weekly resample line up."""
    s = close_col.loc[:t].dropna()
    return s


def ret_12_1(close_col: pd.Series, t: pd.Timestamp) -> float:
    """12-1 skip-month momentum: close[t-21] / close[t-252] - 1 on the stock's own
    daily bars (needs >= 253 bars). Mirror of _price_features exactly:
    ``past = series.iloc[-253]``, ``recent = series.iloc[-22]``."""
    s = _asof_series(close_col, t)
    if len(s) < 253:
        return float("nan")
    past = s.iloc[-253]
    recent = s.iloc[-22]
    if past == 0 or np.isnan(past) or np.isnan(recent):
        return float("nan")
    return float(recent / past - 1.0)


def ret_13w(close_col: pd.Series, t: pd.Timestamp) -> float:
    """13-week point-to-point on the W-FRI weekly series (needs >= 14 weekly bars).
    Mirror of _price_features: ``wk.iloc[-1] / wk.iloc[-14] - 1``."""
    s = _asof_series(close_col, t)
    wk = s.resample("W-FRI").last().dropna()
    if len(wk) < 14:
        return float("nan")
    past13 = wk.iloc[-14]
    if past13 == 0 or np.isnan(past13):
        return float("nan")
    return float(wk.iloc[-1] / past13 - 1.0)


def volatility_26w(close_col: pd.Series, t: pd.Timestamp) -> float:
    """26-week annualized volatility of W-FRI log-returns (needs >= 27 weekly bars).
    Mirror of _price_features: ``std(log(wk[-26:]/wk[-27:-1])) * sqrt(52)``,
    population std via np.nanstd (ddof=0)."""
    s = _asof_series(close_col, t)
    wk = s.resample("W-FRI").last().dropna()
    if len(wk) < 27:
        return float("nan")
    weekly_rets = np.log(wk.iloc[-26:].values / wk.iloc[-27:-1].values)
    return float(np.nanstd(weekly_rets) * np.sqrt(52))


def _weekly_returns(close_col: pd.Series, t: pd.Timestamp, n: int) -> pd.Series:
    """The last ``n`` W-FRI simple returns of a stock as of ``t`` (index = week end).
    Used by beta; empty if too little history."""
    s = _asof_series(close_col, t)
    wk = s.resample("W-FRI").last().dropna()
    rets = wk.pct_change().dropna()
    return rets.iloc[-n:] if len(rets) >= 1 else rets


def beta_52w(
    close_col: pd.Series,
    t: pd.Timestamp,
    market_weekly_ret: pd.Series,
    n_weeks: int = 52,
) -> float:
    """52-week beta: OLS slope of the stock's W-FRI returns on the market proxy's
    W-FRI returns, aligned on common weeks, over the last ``n_weeks`` overlapping
    weeks (needs >= ~30 aligned weeks to be meaningful; returns NaN below that).

    ``market_weekly_ret`` is the proxy's FULL W-FRI simple-return series (built
    once per run by ``market_weekly_returns``); we align the stock's weekly
    returns to it and regress. This is the house 1y-weekly beta convention (52
    W-FRI returns), tactical not valuation."""
    stock = _weekly_returns(close_col, t, n_weeks)
    if len(stock) == 0:
        return float("nan")
    mkt = market_weekly_ret.loc[:t]
    df = pd.concat([stock.rename("y"), mkt.rename("x")], axis=1).dropna()
    df = df.iloc[-n_weeks:]
    if len(df) < 30:
        return float("nan")
    x = df["x"].values
    y = df["y"].values
    var = np.var(x, ddof=0)
    if var < 1e-12:
        return float("nan")
    cov = np.cov(x, y, ddof=0)[0, 1]
    return float(cov / var)


def market_weekly_returns(
    close: pd.DataFrame, proxy_col: str | None = "SPY"
) -> tuple[pd.Series, str]:
    """The market proxy's FULL W-FRI simple-return series + a label of what it is.

    Prefers ``px_spy_daily`` (present in the panel as the 'SPY' column when SPY is
    in the enabled universe, OR read separately -- see run_ic). If the proxy
    column is absent, falls back to the EQUAL-WEIGHT universe mean daily return
    resampled to weekly, and labels it so the report is honest about which proxy
    was used (this doubles as the 'in-house 52W beta' idea's own test)."""
    if proxy_col is not None and proxy_col in close.columns:
        wk = close[proxy_col].resample("W-FRI").last().dropna()
        return wk.pct_change().dropna(), f"{proxy_col} (px_spy_daily total-return)"
    # Equal-weight universe proxy: mean of daily simple returns across all names.
    daily_ret = close.pct_change()
    eq = daily_ret.mean(axis=1)
    wk_ret = (1.0 + eq).resample("W-FRI").prod() - 1.0
    return wk_ret.dropna(), "equal-weight universe mean (SPY proxy unavailable)"


def compute_price_signals(
    close: pd.DataFrame,
    t: pd.Timestamp,
    market_weekly_ret: pd.Series,
) -> pd.DataFrame:
    """All price signals for every ticker as of ``t`` -> DataFrame indexed by
    ticker with columns = PRICE_FACTORS (raw values; NaN where uncomputable).

    This is the per-slice cross-section the metrics consume. It is deliberately a
    straight per-column loop (503 x a handful of slices) -- clarity over a vec
    trick that would obscure the exact product indexing the parity test guards."""
    rows = {}
    for tk in close.columns:
        col = close[tk]
        rows[tk] = {
            "ret_12_1": ret_12_1(col, t),
            "ret_13w": ret_13w(col, t),
            "volatility_26w": volatility_26w(col, t),
            "beta_52w": beta_52w(col, t, market_weekly_ret),
        }
    return pd.DataFrame.from_dict(rows, orient="index")[list(PRICE_FACTORS)]


def to_score(
    raw: pd.Series, direction: int, sectors: pd.Series | None = None
) -> pd.Series:
    """Turn a raw signal into a 'higher = better' cross-sectional z, optionally
    sector-neutral -- using the PRODUCTION primitives (gaussian_rank +
    sector_neutralize). ``direction`` -1 negates a lower-is-better raw (risk)
    BEFORE the rank, exactly like scoring._signal.

    With ``sectors`` None -> raw gaussian-rank z (the 'raw' variant in the run).
    With ``sectors`` -> sector-neutralized z (the 'neutral' variant)."""
    x = -raw if direction < 0 else raw
    z = gaussian_rank(x)
    if sectors is None:
        return z
    return sector_neutralize(z, sectors.reindex(z.index))
