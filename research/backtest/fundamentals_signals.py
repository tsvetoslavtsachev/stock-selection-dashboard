"""
Interface P CONSUMER -- turn the PIT fundamental panel into as-of-t signals.

The PIT panel and its accessor are built by a SEPARATE agent under
``research/fundamentals/`` (``build_panel.py`` -> ``research/data/edgar_pit_panel.csv.gz``;
``pit.py`` -> ``load_panel(path)`` + ``as_known_at(panel, as_of)``). This module is
ONLY the consumer: it calls their as-known-at accessor and assembles the
value/quality PIT signals the backtest scores.

CONTRACT (their pit.as_known_at output, verified against research/fundamentals/pit.py):
a wide DataFrame indexed by ticker with
  * STOCK columns: stockholders_equity, total_assets, current_liabilities,
    cash_and_equivalents, total_debt, shares_outstanding   (latest level known)
  * FLOW columns:  <concept>_ttm for revenues, gross_profit, operating_income,
    net_income, depreciation_amortization, operating_cash_flow, capex, buybacks,
    dividends_paid   (trailing-twelve-month sums)

Signals at t (mktcap[t] = shares_outstanding * close[t]; only filed<=t inputs):
    ep               = net_income_ttm / mktcap
    bp               = stockholders_equity / mktcap
    fcf_yield        = (operating_cash_flow_ttm - capex_ttm) / mktcap
    net_payout_yield = (dividends_paid_ttm + buybacks_ttm) / mktcap
    gpa              = gross_profit_ttm / total_assets
    roe              = net_income_ttm / stockholders_equity
    oper_margin      = operating_income_ttm / revenues_ttm
    ev_ebitda_yield  = (operating_income_ttm + depreciation_amortization_ttm) / EV
                       EV = mktcap + total_debt - cash_and_equivalents   (yield-form)

All are 'higher = better' already (yields / margins), so they go straight into
``to_score`` with direction +1. A missing input -> NaN for that signal (the
scoring reweights). If the panel is unavailable, ``available`` is False and the
caller skips fundamentals with a clear message.

The FUNDAMENTAL BUCKETS for the composite experiments:
    quality = {gpa, roe, oper_margin}
    value   = {ep, bp, fcf_yield, net_payout_yield, ev_ebitda_yield}
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

_PANEL_PATH = Path(__file__).resolve().parents[1] / "data" / "edgar_pit_panel.csv.gz"

FUNDAMENTAL_SIGNALS = (
    "ep", "bp", "fcf_yield", "net_payout_yield",
    "gpa", "roe", "oper_margin", "ev_ebitda_yield",
)
# Which fundamental signal feeds which bucket (all direction +1, higher = better).
FUNDAMENTAL_BUCKETS = {
    "quality": ("gpa", "roe", "oper_margin"),
    "value": ("ep", "bp", "fcf_yield", "net_payout_yield", "ev_ebitda_yield"),
}


class FundamentalsConsumer:
    """Wraps the Interface P accessor. ``available`` is False when the panel file
    is absent OR the accessor cannot be imported -- the caller then skips
    fundamentals. Kept import-light so a price-only run never touches EDGAR code."""

    def __init__(self, panel, accessor):
        # panel = their loaded DataFrame; accessor = their as_known_at callable.
        self._panel = panel
        self._as_known_at = accessor
        self.available = panel is not None and accessor is not None

    def signals_at(self, t: pd.Timestamp, close_t: pd.Series) -> pd.DataFrame:
        """PIT fundamental signals as of ``t`` -> DataFrame(index=ticker,
        columns=FUNDAMENTAL_SIGNALS), raw higher-is-better values. Empty when
        unavailable or nothing was filed by t."""
        if not self.available:
            return pd.DataFrame()
        wide = self._as_known_at(self._panel, t)
        if wide is None or len(wide) == 0:
            return pd.DataFrame()

        def col(name):
            return wide[name] if name in wide.columns else pd.Series(index=wide.index, dtype=float)

        equity = col("stockholders_equity")
        assets = col("total_assets")
        cash = col("cash_and_equivalents")
        debt = col("total_debt")
        shares = col("shares_outstanding")
        ni = col("net_income_ttm")
        rev = col("revenues_ttm")
        gp = col("gross_profit_ttm")
        oi = col("operating_income_ttm")
        da = col("depreciation_amortization_ttm")
        ocf = col("operating_cash_flow_ttm")
        capex = col("capex_ttm")
        div = col("dividends_paid_ttm")
        buyback = col("buybacks_ttm")

        idx = wide.index
        px = close_t.reindex(idx)
        mktcap = shares * px

        def ratio(numer, denom):
            d = denom.reindex(idx)
            return numer.reindex(idx) / d.where(d != 0)

        ev = mktcap + debt.reindex(idx).fillna(0.0) - cash.reindex(idx).fillna(0.0)
        ebitda = oi.reindex(idx) + da.reindex(idx).fillna(0.0)

        out = pd.DataFrame(index=idx)
        out["ep"] = ratio(ni, mktcap)
        out["bp"] = ratio(equity, mktcap)
        out["fcf_yield"] = ratio(ocf.reindex(idx) - capex.reindex(idx).fillna(0.0), mktcap)
        out["net_payout_yield"] = ratio(
            div.reindex(idx).fillna(0.0) + buyback.reindex(idx).fillna(0.0), mktcap)
        out["gpa"] = ratio(gp, assets)
        out["roe"] = ratio(ni, equity)
        out["oper_margin"] = ratio(oi, rev)
        out["ev_ebitda_yield"] = ebitda / ev.where(ev != 0)
        return out[list(FUNDAMENTAL_SIGNALS)]


def load_fundamentals(path: Path = _PANEL_PATH) -> FundamentalsConsumer:
    """Load the PIT panel via the Interface P accessor (research.fundamentals.pit).

    Absent panel OR un-importable accessor -> FundamentalsConsumer(None, None)
    (unavailable): the caller skips fundamental signals with a clear message. This
    is the ONLY coupling to the sibling module, and it is defensive -- the
    price-only run never imports it."""
    if not path.exists():
        logger.warning(
            "PIT fundamentals panel absent (%s) -- fundamental signals SKIPPED. "
            "The Interface P agent builds it via research.fundamentals.build_panel.",
            path,
        )
        return FundamentalsConsumer(None, None)
    try:
        from research.fundamentals import pit  # noqa: PLC0415
        panel = pit.load_panel(str(path))
        return FundamentalsConsumer(panel, pit.as_known_at)
    except Exception as exc:  # noqa: BLE001 - a bad panel/import must fail visible, not crash the run
        logger.warning("PIT panel/accessor unavailable (%s) -- fundamentals SKIPPED.", exc)
        return FundamentalsConsumer(None, None)
