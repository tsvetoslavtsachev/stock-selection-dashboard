"""
Job: publish_site_data
======================
Reads data/processed/ranks.csv and publishes JSON files for the frontend:

    app/data/ranked_stocks.json   — full S&P 500 with all scores + fundamentals
    app/data/market_summary.json  — high-level stats
    app/data/leaders.json         — top 10

Usage
-----
    python -m src.jobs.publish_site_data
"""

from __future__ import annotations

import datetime
import logging
import math

import pandas as pd

from src.lib.io_utils import APP_DATA, DATA_PROCESSED, write_json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("publish_site_data")

_RANKS_CSV = DATA_PROCESSED / "ranks.csv"
_N_LEADERS = 10

# All columns to include in JSON output
_META_COLS = ["ticker", "name", "sector"]

_SCORE_COLS = [
    "composite_score", "trend_score", "quality_score",
    "value_score", "risk_score",
]

_PRICE_COLS = [
    "ret_13w", "ret_26w", "ret_52w", "volatility_26w",
]

_FUNDAMENTAL_COLS = [
    "pe_ratio", "pb_ratio", "ev_ebitda", "ev_ebit",
    "roe", "roic", "debt_equity",
    "eps_ttm", "dividend_yield",
    "revenue_growth_ttm", "oper_margin_ttm", "gross_margin_ttm",
    "fcf_margin_ttm", "market_cap", "beta",
]


def _clean(v) -> float | int | str | None:
    """Replace NaN/Inf with None, round floats."""
    if v is None:
        return None
    try:
        if isinstance(v, float):
            if math.isnan(v) or math.isinf(v):
                return None
            return round(v, 4)
        if isinstance(v, (int,)):
            return v
    except (TypeError, ValueError):
        pass
    return v


def _row_to_dict(row: pd.Series, rank: int) -> dict:
    d: dict = {"rank": rank}
    for col in _META_COLS:
        d[col] = str(row.get(col, "")) if col in row.index else ""
    for col in _SCORE_COLS + _PRICE_COLS + _FUNDAMENTAL_COLS:
        raw = row.get(col, None)
        d[col] = _clean(raw)
    return d


def run() -> None:
    if not _RANKS_CSV.exists():
        raise FileNotFoundError(f"ranks.csv not found at {_RANKS_CSV}. Run compute_factors first.")

    df = pd.read_csv(_RANKS_CSV)
    logger.info("Loaded ranks.csv: %d rows", len(df))

    if df.empty:
        logger.error("ranks.csv is empty — nothing to publish")
        return

    if "composite_score" in df.columns:
        df = df.sort_values("composite_score", ascending=False).reset_index(drop=True)

    as_of = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    # ── ranked_stocks.json ──
    ranked = [_row_to_dict(row, rank=i + 1) for i, (_, row) in enumerate(df.iterrows())]
    write_json(ranked, APP_DATA / "ranked_stocks.json", indent=None)  # compact for 500 rows
    logger.info("Wrote ranked_stocks.json (%d entries, %.0f KB)",
                len(ranked), (APP_DATA / "ranked_stocks.json").stat().st_size / 1024)

    # ── market_summary.json ──
    top = df.iloc[0]
    bottom = df.iloc[-1]

    # Sector breakdown
    sector_counts = df["sector"].value_counts().to_dict() if "sector" in df.columns else {}

    summary = {
        "universe_size": len(df),
        "top_symbol": str(top.get("ticker", "")),
        "top_score": _clean(top.get("composite_score")),
        "top_sector": str(top.get("sector", "")),
        "bottom_symbol": str(bottom.get("ticker", "")),
        "bottom_score": _clean(bottom.get("composite_score")),
        "median_composite": _clean(df["composite_score"].median()) if "composite_score" in df.columns else None,
        "avg_composite": _clean(df["composite_score"].mean()) if "composite_score" in df.columns else None,
        "avg_pe": _clean(df["pe_ratio"].median()) if "pe_ratio" in df.columns else None,
        "avg_roe": _clean(df["roe"].median()) if "roe" in df.columns else None,
        "sector_counts": sector_counts,
        "as_of": as_of,
    }
    write_json(summary, APP_DATA / "market_summary.json")
    logger.info("Wrote market_summary.json (top=%s)", summary["top_symbol"])

    # ── leaders.json ──
    leaders = ranked[:_N_LEADERS]
    write_json(leaders, APP_DATA / "leaders.json")
    logger.info("Wrote leaders.json (%d entries)", len(leaders))


def main() -> None:
    run()


if __name__ == "__main__":
    main()
