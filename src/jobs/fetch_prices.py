"""
Job: fetch_prices
=================
For every enabled ticker in universe.csv, downloads 2 years of weekly price
history via yfinance and saves to:

    data/raw/prices/{symbol}.csv

yfinance has no daily request limit — the full S&P 500 can be fetched in one run.

Usage
-----
    python -m src.jobs.fetch_prices
    python -m src.jobs.fetch_prices --force
"""

from __future__ import annotations

import argparse
import datetime
import logging
import sys
from pathlib import Path

from src.lib.io_utils import DATA_RAW, read_universe
from src.lib.yfinance_client import get_price_history

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("fetch_prices")

_PRICES_DIR = DATA_RAW / "prices"


def _target_path(symbol: str) -> Path:
    return _PRICES_DIR / f"{symbol.upper()}.csv"


def _is_stale(path: Path, max_age_days: int) -> bool:
    if not path.exists():
        return True
    age = datetime.datetime.now() - datetime.datetime.fromtimestamp(path.stat().st_mtime)
    return age.days >= max_age_days


def run(force: bool = False, max_age_days: int = 1) -> dict[str, int]:
    universe = read_universe(enabled_only=True)
    stats = {"fetched": 0, "skipped": 0, "errors": 0}

    _PRICES_DIR.mkdir(parents=True, exist_ok=True)

    for _, row in universe.iterrows():
        symbol = row["ticker"]
        path = _target_path(symbol)

        if not force and not _is_stale(path, max_age_days):
            stats["skipped"] += 1
            continue

        df = get_price_history(symbol, period="2y")
        if df is None or df.empty:
            logger.warning("[%s] No price data", symbol)
            stats["errors"] += 1
            continue

        df.to_csv(path)
        stats["fetched"] += 1

        if stats["fetched"] % 50 == 0:
            logger.info("Progress: %d fetched, %d errors", stats["fetched"], stats["errors"])

    logger.info(
        "fetch_prices done — fetched: %d | skipped: %d | errors: %d",
        stats["fetched"], stats["skipped"], stats["errors"],
    )
    return stats


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--max-age-days", type=int, default=1)
    args = parser.parse_args()
    stats = run(force=args.force, max_age_days=args.max_age_days)
    if stats["errors"] > stats["fetched"] and stats["fetched"] == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
