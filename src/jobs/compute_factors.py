"""
Job: compute_factors
====================
Reads raw price CSVs and fetches fundamentals via yfinance,
computes all factor inputs, calls the scoring engine, and writes:

    data/processed/ranks.csv

Factor inputs:
    Price-based: ret_12_1, ret_13w, volatility_26w
    Fundamentals: pe_ratio, pb_ratio, ev_ebitda,
                  roe, roic, debt_equity, eps_ttm, dividend_yield,
                  revenue_growth_ttm, oper_margin_ttm, gross_margin_ttm,
                  fcf_margin_ttm, gpa, net_payout_yield, market_cap, beta

Usage
-----
    python -m src.jobs.compute_factors
"""

from __future__ import annotations

import logging
from typing import Any

import numpy as np
import pandas as pd

from src.lib.io_utils import DATA_RAW, DATA_PROCESSED, read_universe
from src.lib.yfinance_client import get_fundamentals
from src.lib.scoring import build_scores

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("compute_factors")

_PRICES_DIR = DATA_RAW / "prices"
_OUTPUT = DATA_PROCESSED / "ranks.csv"


# ─── Price features ──────────────────────────────────────────────────────────

def _load_prices(symbol: str) -> pd.Series | None:
    path = _PRICES_DIR / f"{symbol.upper()}.csv"
    if not path.exists():
        return None
    try:
        df = pd.read_csv(path, parse_dates=["Date"], index_col="Date")
        if "Close" not in df.columns or df.empty:
            return None
        return df["Close"].sort_index()
    except Exception as exc:
        logger.error("[%s] Error reading prices: %s", symbol, exc)
        return None


def _price_features(series: pd.Series) -> dict[str, float]:
    """
    Compute price-based features from a DAILY total-return close series (INIT-22
    P9 — sourced base-first from the price-archive). The series is resampled to
    weekly (W-FRI) here so point-to-point returns and volatility follow the house
    weekly convention (52 W-FRI returns, matching the beta convention used across
    the dashboards); Etap B's 12-1 skip-month momentum reads the daily series
    directly instead.

    Momentum is 12-1 (skip-month) on the DAILY series: the return from ~12 months
    ago (t-252) to ~1 month ago (t-21), skipping the most recent ~21 trading days
    where the short-term reversal lives (Jegadeesh-Titman). A 13-week return is
    retained on the weekly series for responsiveness; the old collinear 26w/52w
    point-to-point returns are dropped. Volatility is 26-week (W-FRI).

    A feature that cannot be computed from the available history returns NaN (NOT
    0.0) — a forced 0.0 return looks like a flat year, and a forced 0.0 volatility
    looks like the *calmest* stock in the universe, both of which silently distort
    the ranking. NaN instead flows into the NaN-aware scoring and is flagged as a
    data-quality issue.
    """
    wk = series.resample("W-FRI").last().dropna()
    nwk = len(wk)
    nd = len(series)

    # 12-1 momentum on daily bars: price[t-21] / price[t-252] - 1. The last ~21
    # trading days are excluded (no lookahead into the skipped window).
    if nd >= 253:
        past = series.iloc[-253]
        recent = series.iloc[-22]
        ret_12_1 = float("nan") if (past == 0 or np.isnan(past) or np.isnan(recent)) \
            else (recent / past) - 1.0
    else:
        ret_12_1 = float("nan")

    # 13-week point-to-point on the weekly (W-FRI) series — responsiveness input.
    if nwk >= 14:
        past13 = wk.iloc[-14]
        ret_13 = float("nan") if (past13 == 0 or np.isnan(past13)) else (wk.iloc[-1] / past13) - 1.0
    else:
        ret_13 = float("nan")

    if nwk >= 27:
        weekly_rets = np.log(wk.iloc[-26:].values / wk.iloc[-27:-1].values)
        vol = float(np.nanstd(weekly_rets) * np.sqrt(52))
    else:
        vol = float("nan")

    return {
        "ret_12_1": round(ret_12_1, 6),
        "ret_13w": round(ret_13, 6),
        "volatility_26w": round(vol, 6),
    }


def _apply_sector_guards(record: dict) -> None:
    """EV/EBITDA and GP/A are meaningless for banks/insurers (no operating EBITDA
    and no cost-of-goods gross profit in the non-financial sense; the GP/A
    fallback fills banks with a fictitious ~0 that would rank the whole group at
    the bottom of a metric that does not apply to them), so neither is scored for
    Financials -- set to NaN (sector comes from universe.csv). The value bucket
    then reweights onto E/P + net payout, the quality bucket onto ROE + margins."""
    if str(record.get("sector", "")) == "Financials":
        record["ev_ebitda"] = np.nan
        record["gpa"] = np.nan


# ─── Main ─────────────────────────────────────────────────────────────────────

def run() -> pd.DataFrame:
    universe = read_universe(enabled_only=True)
    rows: list[dict] = []
    total = len(universe)

    for i, (_, row) in enumerate(universe.iterrows()):
        symbol = row["ticker"]

        if (i + 1) % 25 == 0 or i == 0:
            logger.info("Processing %d/%d: %s", i + 1, total, symbol)

        record: dict[str, Any] = {
            "ticker": symbol,
            "name":   row.get("name", ""),
            "sector": row.get("sector", ""),
            "cik":    row.get("cik", ""),
        }

        # Price features. Missing prices = NaN (never 0.0) + a data_quality flag,
        # so a data outage cannot masquerade as a flat, ultra-low-volatility stock.
        prices = _load_prices(symbol)
        if prices is not None and len(prices) > 1:
            feats = _price_features(prices)
            record.update(feats)
            # Date of the newest price bar actually used — the real data-recency
            # signal (publish stamps its own run time, which is always "fresh"
            # even when the fetch silently returned nothing new).
            record["price_asof"] = prices.index[-1].strftime("%Y-%m-%d")
            n_missing = sum(1 for v in feats.values() if pd.isna(v))
            record["data_quality"] = "ok" if n_missing == 0 else "partial_prices"
        else:
            record.update(
                {"ret_12_1": np.nan, "ret_13w": np.nan, "volatility_26w": np.nan}
            )
            record["price_asof"] = None
            record["data_quality"] = "missing_prices"
            logger.warning("[%s] No usable price history — flagged missing_prices", symbol)

        # Fundamentals from yfinance
        fundamentals = get_fundamentals(symbol)
        record.update(fundamentals)

        _apply_sector_guards(record)

        rows.append(record)

    if not rows:
        logger.error("No data produced")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    scored = build_scores(df)

    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    scored.to_csv(_OUTPUT, index=False)
    logger.info("Saved ranks.csv: %d rows → %s", len(scored), _OUTPUT)

    return scored


def main() -> None:
    run()


if __name__ == "__main__":
    main()
