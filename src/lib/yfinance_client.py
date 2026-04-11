"""
yfinance client — fetches price history and fundamental data for S&P 500.

Uses yfinance (Yahoo Finance) which has no daily request limit, unlike Alpha Vantage.
This module replaces alpha_vantage_client.py as the primary price/fundamentals source.

Alpha Vantage is kept as a fallback but is not used in the default pipeline.
"""

from __future__ import annotations

import logging
from typing import Any

import yfinance as yf
import pandas as pd

logger = logging.getLogger(__name__)


def get_price_history(symbol: str, period: str = "2y") -> pd.DataFrame | None:
    """
    Fetch weekly adjusted close prices for *symbol*.

    Parameters
    ----------
    symbol : str
        Stock ticker (e.g. "AAPL").
    period : str
        yfinance period string: "1y", "2y", "5y", "max".

    Returns
    -------
    pd.DataFrame with columns [Date, Close] sorted oldest→newest,
    or None on failure.
    """
    try:
        ticker = yf.Ticker(symbol)
        hist = ticker.history(period=period, interval="1wk", auto_adjust=True)
        if hist.empty:
            logger.warning("[%s] No price history returned", symbol)
            return None
        hist = hist[["Close"]].copy()
        hist.index.name = "Date"
        return hist.sort_index()
    except Exception as exc:
        logger.error("[%s] Price fetch failed: %s", symbol, exc)
        return None


def get_fundamentals(symbol: str) -> dict[str, Any]:
    """
    Fetch key fundamental metrics for *symbol* from Yahoo Finance.

    Returns a dict with all requested metrics.
    Missing values are set to None (handled downstream).

    Metrics returned:
        pe_ratio, pb_ratio, ev_ebitda, ev_ebit,
        roe, roic, debt_equity,
        eps_ttm, dividend_yield,
        revenue_growth_ttm, oper_margin_ttm, gross_margin_ttm,
        fcf_margin_ttm, market_cap, beta
    """
    result: dict[str, Any] = {
        "pe_ratio": None,
        "pb_ratio": None,
        "ev_ebitda": None,
        "ev_ebit": None,
        "roe": None,
        "roic": None,
        "debt_equity": None,
        "eps_ttm": None,
        "dividend_yield": None,
        "revenue_growth_ttm": None,
        "oper_margin_ttm": None,
        "gross_margin_ttm": None,
        "fcf_margin_ttm": None,
        "market_cap": None,
        "beta": None,
    }

    try:
        ticker = yf.Ticker(symbol)
        info = ticker.info or {}

        result["pe_ratio"] = info.get("trailingPE") or info.get("forwardPE")
        result["pb_ratio"] = info.get("priceToBook")
        result["ev_ebitda"] = info.get("enterpriseToEbitda")
        result["ev_ebit"] = _safe_divide(
            info.get("enterpriseValue"), info.get("ebitda")
        )
        result["roe"] = info.get("returnOnEquity")
        result["debt_equity"] = info.get("debtToEquity")
        if result["debt_equity"] is not None:
            result["debt_equity"] = result["debt_equity"] / 100.0  # yf returns as %

        result["eps_ttm"] = info.get("trailingEps")
        # yfinance returns dividendYield in percent (e.g. 2.5 means 2.5%)
        _dy = info.get("dividendYield")
        result["dividend_yield"] = _dy / 100.0 if _dy is not None else None
        result["revenue_growth_ttm"] = info.get("revenueGrowth")
        result["oper_margin_ttm"] = info.get("operatingMargins")
        result["gross_margin_ttm"] = info.get("grossMargins")
        result["market_cap"] = info.get("marketCap")
        result["beta"] = info.get("beta")

        # ROIC = EBIT / (Total Assets - Current Liabilities)
        # yfinance doesn't provide ROIC directly; approximate from financials
        result["roic"] = _calc_roic(ticker)

        # FCF margin
        result["fcf_margin_ttm"] = _calc_fcf_margin(ticker, info)

    except Exception as exc:
        logger.error("[%s] Fundamentals fetch failed: %s", symbol, exc)

    return result


def _safe_divide(a: float | None, b: float | None) -> float | None:
    if a is None or b is None or b == 0:
        return None
    return a / b


def _calc_roic(ticker: yf.Ticker) -> float | None:
    """Approximate ROIC from quarterly financials."""
    try:
        inc = ticker.quarterly_income_stmt
        bal = ticker.quarterly_balance_sheet
        if inc is None or bal is None or inc.empty or bal.empty:
            return None

        # EBIT (last 4 quarters)
        if "EBIT" in inc.index:
            ebit_ttm = inc.loc["EBIT"].head(4).sum()
        elif "Operating Income" in inc.index:
            ebit_ttm = inc.loc["Operating Income"].head(4).sum()
        else:
            return None

        # Invested capital (latest quarter)
        total_assets = bal.loc["Total Assets"].iloc[0] if "Total Assets" in bal.index else None
        current_liab = bal.loc["Current Liabilities"].iloc[0] if "Current Liabilities" in bal.index else None
        if total_assets is None or current_liab is None:
            return None

        invested = total_assets - current_liab
        if invested <= 0:
            return None
        return ebit_ttm / invested
    except Exception:
        return None


def _calc_fcf_margin(ticker: yf.Ticker, info: dict) -> float | None:
    """FCF Margin = Free Cash Flow / Revenue."""
    try:
        fcf = info.get("freeCashflow")
        revenue = info.get("totalRevenue")
        if fcf is not None and revenue and revenue > 0:
            return fcf / revenue

        cf = ticker.quarterly_cashflow
        if cf is None or cf.empty:
            return None

        ocf_key = "Operating Cash Flow" if "Operating Cash Flow" in cf.index else "Total Cash From Operating Activities"
        capex_key = "Capital Expenditure" if "Capital Expenditure" in cf.index else "Capital Expenditures"

        if ocf_key not in cf.index:
            return None

        ocf_ttm = cf.loc[ocf_key].head(4).sum()
        capex_ttm = abs(cf.loc[capex_key].head(4).sum()) if capex_key in cf.index else 0
        fcf_ttm = ocf_ttm - capex_ttm

        if revenue and revenue > 0:
            return fcf_ttm / revenue
        return None
    except Exception:
        return None
