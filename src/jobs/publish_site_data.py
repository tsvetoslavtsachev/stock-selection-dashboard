"""
Job: publish_site_data
======================
Reads data/processed/ranks.csv and publishes three static JSON files
consumed by the frontend dashboard:

    app/data/ranked_stocks.json   — full universe with all scores
    app/data/market_summary.json  — high-level metadata snapshot
    app/data/leaders.json         — top 10 by composite_score

JSON schema
-----------
ranked_stocks.json:
    List of objects sorted by composite_score descending.
    Each object: {ticker, name, sector, rank, composite_score,
                  trend_score, quality_score, value_score, risk_score,
                  ret_13w, ret_26w, ret_52w, volatility_26w,
                  revenue_growth_ttm, oper_margin_ttm, fcf_margin_ttm}

market_summary.json:
    {universe_size, top_symbol, top_score, bottom_symbol, bottom_score,
     median_composite, as_of}

leaders.json:
    Same schema as ranked_stocks entries, top 10 only.

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

# Columns to include in the published JSON (keeps payload lean)
_SCORE_COLS = [
    "composite_score", "trend_score", "quality_score",
    "value_score", "risk_score",
]
_FACTOR_COLS = [
    "ret_13w", "ret_26w", "ret_52w", "volatility_26w",
    "revenue_growth_ttm", "oper_margin_ttm", "fcf_margin_ttm",
]
_META_COLS = ["ticker", "name", "sector"]


def _clean_float(v: float) -> float | None:
    """Replace NaN / Inf with None so JSON serialisation is clean."""
    if v is None:
        return None
    try:
        if math.isnan(v) or math.isinf(v):
            return None
    except TypeError:
        pass
    return round(float(v), 4)


def _row_to_dict(row: pd.Series, rank: int) -> dict:
    """Convert a DataFrame row to a clean dict ready for JSON output."""
    d: dict = {"rank": rank}
    for col in _META_COLS:
        d[col] = str(row.get(col, "")) if col in row.index else ""
    for col in _SCORE_COLS + _FACTOR_COLS:
        raw = row.get(col, None)
        d[col] = _clean_float(raw) if raw is not None else None
    return d


def run() -> None:
    """
    Main entry point. Reads ranks.csv, builds and writes the three JSON files.
    """
    if not _RANKS_CSV.exists():
        raise FileNotFoundError(
            f"ranks.csv not found at {_RANKS_CSV}. "
            "Run compute_factors first."
        )

    df = pd.read_csv(_RANKS_CSV)
    logger.info("Loaded ranks.csv: %d rows", len(df))

    if df.empty:
        logger.error("ranks.csv is empty — nothing to publish")
        return

    # Ensure sorted by composite_score descending
    if "composite_score" in df.columns:
        df = df.sort_values("composite_score", ascending=False).reset_index(drop=True)

    as_of = datetime.datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    # ── ranked_stocks.json ──────────────────────────────────────────────────
    ranked_stocks = [
        _row_to_dict(row, rank=i + 1)
        for i, (_, row) in enumerate(df.iterrows())
    ]

    write_json(ranked_stocks, APP_DATA / "ranked_stocks.json", indent=2)
    logger.info("Wrote ranked_stocks.json (%d entries)", len(ranked_stocks))

    # ── market_summary.json ─────────────────────────────────────────────────
    top_row = df.iloc[0]
    bottom_row = df.iloc[-1]
    median_composite = _clean_float(df["composite_score"].median()) if "composite_score" in df.columns else None

    market_summary = {
        "universe_size": len(df),
        "top_symbol": str(top_row.get("ticker", "")),
        "top_score": _clean_float(top_row.get("composite_score", None)),
        "top_sector": str(top_row.get("sector", "")),
        "bottom_symbol": str(bottom_row.get("ticker", "")),
        "bottom_score": _clean_float(bottom_row.get("composite_score", None)),
        "median_composite": median_composite,
        "as_of": as_of,
    }

    write_json(market_summary, APP_DATA / "market_summary.json", indent=2)
    logger.info("Wrote market_summary.json (top=%s)", market_summary["top_symbol"])

    # ── leaders.json ────────────────────────────────────────────────────────
    leaders = ranked_stocks[:_N_LEADERS]

    write_json(leaders, APP_DATA / "leaders.json", indent=2)
    logger.info("Wrote leaders.json (%d entries)", len(leaders))


def main() -> None:
    run()


if __name__ == "__main__":
    main()
