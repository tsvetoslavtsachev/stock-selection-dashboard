"""
IC and portfolio metrics.

Everything here consumes two aligned cross-sections per rebalance date: a signal
``score`` (higher = better) and a forward return. From the stack of monthly
cross-sections we compute:

  * Spearman IC per month  -- rank correlation of score vs forward return. (No
    scipy needed: Spearman == Pearson of the ranks, which is pandas
    rank().corr().) Summary: mean, std, plain t = mean/std*sqrt(n),
    Newey-West-adjusted t (lag 3), % positive months, n.
  * Block bootstrap 95% CI of the mean IC (block=3, 2000 resamples, numpy,
    fixed seed) -- honest CI under monthly autocorrelation of IC.
  * Quintile portfolios (EW, monthly rebalance): Q5-Q1 annualized return, vol,
    max drawdown, and average one-month turnover of the Q5 book.
  * Regime IC means over three named windows.
  * IC decay: 21d vs 63d mean IC.

STAT-POWER NOTE (reported): with n monthly ICs, the |mean IC| needed for a plain
t=2 is 2 / (sqrt(n) / (std/1)) ... = 2 * std / sqrt(n). The report prints this
using the realized std so the reader sees how much signal n can even resolve.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

# Fixed seed for every bootstrap so a re-run is bit-identical (determinism test).
BOOTSTRAP_SEED = 20260706


def spearman_ic(score: pd.Series, fwd: pd.Series) -> float:
    """Spearman rank IC of a single cross-section. NaN if < 3 aligned names or a
    degenerate (constant) side. Spearman == Pearson correlation of the ranks."""
    df = pd.concat([score.rename("s"), fwd.rename("f")], axis=1).dropna()
    if len(df) < 3:
        return float("nan")
    rs = df["s"].rank()
    rf = df["f"].rank()
    if rs.std(ddof=0) < 1e-12 or rf.std(ddof=0) < 1e-12:
        return float("nan")
    return float(rs.corr(rf))


def _newey_west_se(x: np.ndarray, lag: int) -> float:
    """Newey-West standard error of the MEAN of ``x`` with Bartlett weights up to
    ``lag``. Corrects the naive SE for serial correlation in the monthly IC
    series (adjacent months' ICs are not independent). Returns the SE of the mean
    (i.e. sqrt(long-run variance / n))."""
    x = np.asarray(x, dtype=float)
    x = x[~np.isnan(x)]
    n = len(x)
    if n < 2:
        return float("nan")
    xc = x - x.mean()
    gamma0 = np.dot(xc, xc) / n
    lrv = gamma0
    for k in range(1, min(lag, n - 1) + 1):
        w = 1.0 - k / (lag + 1.0)
        gamma_k = np.dot(xc[k:], xc[:-k]) / n
        lrv += 2.0 * w * gamma_k
    if lrv <= 0:
        return float("nan")
    return float(np.sqrt(lrv / n))


def ic_summary(ic: pd.Series, nw_lag: int = 3) -> dict:
    """Summary stats of a monthly IC series: n, mean, std, plain t, Newey-West t
    (lag ``nw_lag``), % positive, and the |mean IC| that would be needed for a
    plain t=2 at this n and std (statistical-power yardstick)."""
    x = ic.dropna().values.astype(float)
    n = len(x)
    if n == 0:
        return {"n": 0, "mean": float("nan"), "std": float("nan"),
                "t": float("nan"), "t_nw": float("nan"), "pct_pos": float("nan"),
                "ic_for_t2": float("nan")}
    mean = float(np.mean(x))
    std = float(np.std(x, ddof=1)) if n > 1 else float("nan")
    t = float(mean / (std / np.sqrt(n))) if (std and std > 0) else float("nan")
    nw_se = _newey_west_se(x, nw_lag)
    t_nw = float(mean / nw_se) if (nw_se and nw_se > 0) else float("nan")
    pct_pos = float(np.mean(x > 0) * 100.0)
    ic_for_t2 = float(2.0 * std / np.sqrt(n)) if (std and n) else float("nan")
    return {"n": n, "mean": mean, "std": std, "t": t, "t_nw": t_nw,
            "pct_pos": pct_pos, "ic_for_t2": ic_for_t2}


def block_bootstrap_ci(
    ic: pd.Series, block: int = 3, n_boot: int = 2000, seed: int = BOOTSTRAP_SEED,
    alpha: float = 0.05,
) -> tuple[float, float]:
    """95% CI of the MEAN IC by circular block bootstrap (block length ``block``).

    Resampling in blocks preserves the short-run autocorrelation of the monthly
    IC series (a plain i.i.d. bootstrap would understate the CI). Circular wrap
    keeps every block length equal. Deterministic given ``seed`` (the determinism
    test pins two runs to bit-equality)."""
    x = ic.dropna().values.astype(float)
    n = len(x)
    if n < block or n == 0:
        return (float("nan"), float("nan"))
    rng = np.random.default_rng(seed)
    n_blocks = int(np.ceil(n / block))
    means = np.empty(n_boot, dtype=float)
    starts_pool = np.arange(n)
    for b in range(n_boot):
        starts = rng.choice(starts_pool, size=n_blocks, replace=True)
        # circular blocks: indices (start + 0..block-1) mod n
        idx = (starts[:, None] + np.arange(block)[None, :]).ravel() % n
        idx = idx[: n_blocks * block][:n]  # trim to original length n
        means[b] = x[idx].mean()
    lo = float(np.percentile(means, 100 * (alpha / 2)))
    hi = float(np.percentile(means, 100 * (1 - alpha / 2)))
    return (lo, hi)


# --------------------------------------------------------------------------- #
# Quintile portfolios
# --------------------------------------------------------------------------- #
def _quintile_labels(score: pd.Series, q: int = 5) -> pd.Series:
    """Assign 1..q quintile labels by score rank (q = top). NaN scores -> NaN.
    Uses rank/qcut on ranks so ties spread evenly and equal scores don't collapse
    a bin."""
    s = score.dropna()
    if len(s) < q:
        return pd.Series(index=score.index, dtype=float)
    r = s.rank(method="first")
    try:
        lab = pd.qcut(r, q, labels=range(1, q + 1)).astype(int)
    except ValueError:
        return pd.Series(index=score.index, dtype=float)
    return lab.reindex(score.index)


def quintile_spread_series(
    scores_by_date: dict, fwd_by_date: dict, q: int = 5
) -> pd.DataFrame:
    """Per-rebalance EW quintile forward returns + the Q(top)-Q1 long-short.

    Returns a DataFrame indexed by rebalance date with columns q1..q{q} and
    'spread' (= q{top} - q1), each an equal-weight mean forward return over the
    holding month. Dates with too few names are skipped."""
    recs = {}
    for t, score in scores_by_date.items():
        fwd = fwd_by_date.get(t)
        if fwd is None:
            continue
        lab = _quintile_labels(score, q)
        df = pd.concat([lab.rename("q"), fwd.rename("f")], axis=1).dropna()
        if df["q"].nunique() < q:
            continue
        means = df.groupby("q")["f"].mean()
        row = {f"q{int(k)}": float(v) for k, v in means.items()}
        row["spread"] = float(means.get(q, np.nan) - means.get(1, np.nan))
        recs[t] = row
    if not recs:
        return pd.DataFrame()
    return pd.DataFrame.from_dict(recs, orient="index").sort_index()


def _annualize_from_monthly(returns: pd.Series, periods_per_year: float = 12.0) -> dict:
    """Annualized return / vol / max-drawdown of a monthly return series.

    'Monthly' here = one rebalance-to-rebalance holding period (the 21d spread
    series). Geometric annualized return; vol = std * sqrt(12); max drawdown on
    the compounded equity curve."""
    r = returns.dropna()
    if len(r) == 0:
        return {"ann_return": float("nan"), "ann_vol": float("nan"),
                "max_drawdown": float("nan"), "n": 0}
    geo = float((1.0 + r).prod() ** (periods_per_year / len(r)) - 1.0)
    vol = float(r.std(ddof=1) * np.sqrt(periods_per_year)) if len(r) > 1 else float("nan")
    equity = (1.0 + r).cumprod()
    dd = float((equity / equity.cummax() - 1.0).min())
    return {"ann_return": geo, "ann_vol": vol, "max_drawdown": dd, "n": len(r)}


def quintile_stats(spread_df: pd.DataFrame, q: int = 5) -> dict:
    """Annualized Q(top)-Q1 return / vol / max-drawdown from the spread series."""
    if spread_df.empty or "spread" not in spread_df.columns:
        return {"ann_return": float("nan"), "ann_vol": float("nan"),
                "max_drawdown": float("nan"), "n": 0}
    return _annualize_from_monthly(spread_df["spread"])


def top_quintile_turnover(scores_by_date: dict, q: int = 5) -> float:
    """Average one-rebalance turnover of the top quintile (Q{q}) book: the mean
    over consecutive rebalances of |names entering| / |book size|. 1.0 = fully
    replaced each month; ~0 = stable. Annualize by *12 outside if desired."""
    dates = sorted(scores_by_date.keys())
    prev = None
    turns = []
    for t in dates:
        lab = _quintile_labels(scores_by_date[t], q)
        book = set(lab[lab == q].index)
        if prev is not None and book:
            entered = len(book - prev)
            turns.append(entered / len(book))
        prev = book if book else prev
    return float(np.mean(turns)) if turns else float("nan")


# --------------------------------------------------------------------------- #
# Regime splits + IC decay
# --------------------------------------------------------------------------- #
REGIMES = {
    "late_bull_2021H2": ("2021-08-01", "2021-12-31"),
    "bear_rate_shock_2022": ("2022-01-01", "2022-12-31"),
    "ai_bull_2023_plus": ("2023-01-01", "2100-01-01"),
}


def regime_ic_means(ic: pd.Series) -> dict:
    """Mean IC within each named regime window (NaN if no slices fall in it)."""
    out = {}
    for name, (lo, hi) in REGIMES.items():
        mask = (ic.index >= pd.Timestamp(lo)) & (ic.index <= pd.Timestamp(hi))
        sub = ic[mask].dropna()
        out[name] = {"mean": float(sub.mean()) if len(sub) else float("nan"),
                     "n": int(len(sub))}
    return out
