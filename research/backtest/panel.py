"""
Price panel + rebalance calendar (READ-ONLY on the canonical archive).

The panel is a daily total-return Close matrix (DatetimeIndex x dotted-ticker)
for the full enabled universe, read base-first from the canonical price-archive
through ``collectors.price.consumer.load_ohlcv_base_first`` -- the SAME reader
the production pipeline uses (``src/jobs/fetch_prices.py``), so the research
prices are bar-for-bar the production prices: split-adjusted, drift-proof
total-return close (RIV-2 capstone).

Symbol form: the universe keys class shares with a DOT ("BRK.B", S&P/SEC form);
the archive catalog keys them with a DASH ("BRK-B"). We translate dot->dash at
the archive boundary (via the production ``_to_yahoo_symbol``) and map back to
the dotted form for the panel columns -- identical to fetch_prices, so a column
name here equals a ticker in universe.csv / ranks.csv.

RESEARCH DIFFERENCE FROM PRODUCTION: there is NO yfinance fallback. A symbol not
served from the base is LOGGED and DROPPED (reproducibility: a research panel
must be a deterministic function of the archive, never of a live Yahoo call).
The fetch_fallback injected below therefore always returns empty.

The rebalance calendar is the last trading day of each month (from the panel's
own trading index, so it lands on real bars only).
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)

# ROOT = stock-selection-dashboard/ (research/backtest/panel.py -> parents[2]).
_ROOT = Path(__file__).resolve().parents[2]

# The production reader lives in the sibling repos. Add them to sys.path here so
# the framework runs from a bare checkout of THIS repo without a global env edit
# (mirrors how the CI checks out the sibling repos next to this one). An explicit
# env PYTHONPATH still wins -- these are appended, not prepended.
_SIBLINGS = [Path("C:/Projects/collectors"), Path("C:/Projects/data-core")]
for _p in _SIBLINGS:
    sp = str(_p)
    if _p.exists() and sp not in sys.path:
        sys.path.append(sp)
# Repo root on the path so ``src.*`` imports resolve when run as -m from the root.
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

# DATACORE_ROOT points the reader at the price-archive checkout. Respect an
# explicit env; otherwise fall back to the conventional sibling location. This is
# a READ pointer only (datacore.archive.read is read-only -- no writes here).
if not os.environ.get("DATACORE_ROOT"):
    _default_archive = Path("C:/Projects/price-archive")
    if _default_archive.exists():
        os.environ["DATACORE_ROOT"] = str(_default_archive)


@dataclass(frozen=True)
class Panel:
    """The immutable research price panel + its derived calendar.

    close      : daily TR-Close, DatetimeIndex (sorted) x dotted-ticker columns.
    sectors    : ticker -> static GICS sector (from universe.csv).
    rebalances : DatetimeIndex of month-end trading days (subset of close.index).
    source_map : ticker -> provenance ('base' | dropped tickers absent).
    dropped    : tickers requested but not served from the base (logged, excluded).
    """

    close: pd.DataFrame
    sectors: pd.Series
    rebalances: pd.DatetimeIndex
    source_map: dict
    dropped: list


def _empty_fallback(missing, period=None):
    """Research fallback = NO fallback. A base miss stays missing (reproducibility).

    Signature matches what ``load_ohlcv_base_first`` calls: ``fetch(missing,
    period=period)`` returning a dict{field -> DataFrame}. We return an empty
    Close frame so every un-served ticker is dropped rather than back-filled from
    a live yfinance call (which would make the panel non-reproducible)."""
    return {"Close": pd.DataFrame()}


def month_end_trading_days(index: pd.DatetimeIndex) -> pd.DatetimeIndex:
    """Last actual trading day of each calendar month present in ``index``.

    Grouping the real trading index by year-month and taking the max lands the
    rebalance on a bar that exists (never a calendar month-end that fell on a
    weekend/holiday). Returned sorted and unique."""
    if len(index) == 0:
        return pd.DatetimeIndex([])
    s = pd.Series(index, index=index)
    keys = index.to_period("M")
    last = s.groupby(keys).max()
    return pd.DatetimeIndex(sorted(last.values))


def load_panel(period: str = "max") -> Panel:
    """Build the research price panel for the full enabled universe.

    Reads base-first from the canonical archive (READ-ONLY), drops any symbol not
    served from the base (logged), and derives the month-end rebalance calendar.

    ``period`` is passed through to the reader ("max" = full archive history; the
    panel is clipped only by what the archive holds, ~2020-06-29 -> today).
    """
    # Imports are deferred so this module imports even when the sibling repos are
    # absent (e.g. doc build) -- the failure then surfaces with a clear message
    # exactly where the archive is actually needed.
    try:
        from collectors.price.consumer import load_ohlcv_base_first  # noqa: PLC0415
    except ImportError as exc:  # pragma: no cover - environment guard
        raise ImportError(
            "collectors.price.consumer not importable. Put C:/Projects/collectors and "
            "C:/Projects/data-core on PYTHONPATH and set DATACORE_ROOT to the "
            "price-archive checkout. This framework is READ-ONLY on those repos."
        ) from exc
    from src.lib.io_utils import read_universe  # noqa: PLC0415
    from src.lib.yfinance_client import _to_yahoo_symbol  # noqa: PLC0415

    uni = read_universe(enabled_only=True)
    dot_tickers = [str(t).upper() for t in uni["ticker"]]
    sectors = pd.Series(
        uni["sector"].values, index=uni["ticker"].str.upper().values, name="sector"
    )

    # dot ("BRK.B") <-> dash ("BRK-B", the archive form), both directions.
    dash_of = {t: _to_yahoo_symbol(t) for t in dot_tickers}
    dot_of = {d: t for t, d in dash_of.items()}
    dash_tickers = [dash_of[t] for t in dot_tickers]

    ohlcv, source_map = load_ohlcv_base_first(
        dash_tickers,
        fetch_fallback=_empty_fallback,
        period=period,
        normalize_currency=False,  # S&P 500 is pure USD -> no-op
    )
    close_dash = ohlcv.get("Close", pd.DataFrame())

    # Map the dash columns back to the dotted universe form so a panel column ==
    # a universe ticker == a ranks.csv ticker (join key stays dotted everywhere).
    close = close_dash.rename(columns=dot_of).sort_index()
    close = close.reindex(sorted(close.columns), axis=1)

    served = list(close.columns)
    dropped = [t for t in dot_tickers if t not in served]
    if dropped:
        logger.warning(
            "Panel: %d/%d tickers NOT served from base -- dropped (no research "
            "fallback). First few: %s",
            len(dropped), len(dot_tickers), dropped[:10],
        )

    rebalances = month_end_trading_days(close.index)
    logger.info(
        "Panel: %d tickers x %d bars (%s -> %s); %d month-end rebalances",
        close.shape[1], close.shape[0],
        close.index.min().date() if len(close.index) else "n/a",
        close.index.max().date() if len(close.index) else "n/a",
        len(rebalances),
    )
    return Panel(
        close=close,
        sectors=sectors.reindex(served),
        rebalances=rebalances,
        source_map={dot_of.get(k, k): v for k, v in source_map.items()},
        dropped=dropped,
    )


def testable_rebalances(
    close: pd.DataFrame,
    rebalances: pd.DatetimeIndex,
    min_history_days: int = 253,
    min_forward_days: int = 21,
) -> pd.DatetimeIndex:
    """Rebalance dates usable as test slices: those with >= ``min_history_days`` of
    panel history BEFORE (inclusive) and >= ``min_forward_days`` of panel bars
    AFTER. Computed on the shared trading index (universe-level), not per stock --
    a stock with too little of its OWN history simply scores NaN at that slice and
    drops out of the cross-section there.
    """
    if len(close.index) == 0:
        return pd.DatetimeIndex([])
    idx = close.index
    pos = {ts: i for i, ts in enumerate(idx)}
    n = len(idx)
    out = []
    for ts in rebalances:
        i = pos.get(ts)
        if i is None:
            continue
        history = i + 1            # bars up to and including t
        forward = n - 1 - i        # bars strictly after t
        if history >= min_history_days and forward >= min_forward_days:
            out.append(ts)
    return pd.DatetimeIndex(out)
