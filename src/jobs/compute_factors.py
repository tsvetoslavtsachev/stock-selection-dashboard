"""
Job: compute_factors
====================
Reads raw price CSVs and fetches fundamentals via yfinance,
computes all factor inputs, calls the scoring engine, and writes:

    data/processed/ranks.csv

Factor inputs:
    Price-based: ret_13w, ret_26w, ret_52w, volatility_26w
    Fundamentals: pe_ratio, pb_ratio, ev_ebitda, ev_ebit,
                  roe, roic, debt_equity, eps_ttm, dividend_yield,
                  revenue_growth_ttm, oper_margin_ttm, gross_margin_ttm,
                  fcf_margin_ttm, market_cap, beta

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
    n = len(series)

    def safe_ret(weeks: int) -> float:
        if n < weeks + 1:
            return 0.0
        past = series.iloc[-(weeks + 1)]
        current = series.iloc[-1]
        if past == 0 or np.isnan(past):
            return 0.0
        return (current / past) - 1.0

    ret_13 = safe_ret(13)
    ret_26 = safe_ret(26)
    ret_52 = safe_ret(52)

    if n >= 27:
        weekly_rets = np.log(series.iloc[-26:].values / series.iloc[-27:-1].values)
        vol = float(np.nanstd(weekly_rets) * np.sqrt(52))
    else:
        vol = 0.0

    return {
        "ret_13w": round(ret_13, 6),
        "ret_26w": round(ret_26, 6),
        "ret_52w": round(ret_52, 6),
        "volatility_26w": round(vol, 6),
    }


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

        # Price features
        prices = _load_prices(symbol)
        if prices is not None and len(prices) > 1:
            record.update(_price_features(prices))
        else:
            record.update({"ret_13w": 0.0, "ret_26w": 0.0, "ret_52w": 0.0, "volatility_26w": 0.0})

        # Fundamentals from yfinance
        fundamentals = get_fundamentals(symbol)
        record.update(fundamentals)

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
