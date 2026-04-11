"""
Job: fetch_prices
=================
For every enabled ticker in universe.csv, downloads weekly adjusted price
data from Alpha Vantage and saves it as:

    data/raw/prices/{symbol}.json

Skips tickers whose file is fresher than --max-age-days (default 1) unless
--force is set. This avoids burning API quota on re-runs triggered the same day.

Usage
-----
    python -m src.jobs.fetch_prices
    python -m src.jobs.fetch_prices --force
    python -m src.jobs.fetch_prices --max-age-days 2
"""

from __future__ import annotations

import argparse
import datetime
import logging
import sys

from pathlib import Path

from src.lib.alpha_vantage_client import AlphaVantageClient
from src.lib.io_utils import DATA_RAW, read_universe, write_json

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("fetch_prices")

_PRICES_DIR = DATA_RAW / "prices"


def _target_path(symbol: str) -> Path:
    return _PRICES_DIR / f"{symbol.upper()}.json"


def _is_stale(path: Path, max_age_days: int) -> bool:
    """Return True if *path* does not exist or is older than *max_age_days*."""
    if not path.exists():
        return True
    age = datetime.datetime.now() - datetime.datetime.fromtimestamp(path.stat().st_mtime)
    return age.days >= max_age_days


def run(force: bool = False, max_age_days: int = 1) -> dict[str, int]:
    """
    Main entry point.

    Parameters
    ----------
    force : bool
        Re-download even if a fresh file exists.
    max_age_days : int
        Consider a cached file 'stale' after this many calendar days.

    Returns
    -------
    dict with keys 'fetched', 'skipped', 'errors'
    """
    universe = read_universe(enabled_only=True)
    client = AlphaVantageClient()  # reads ALPHA_VANTAGE_API_KEY from env

    stats = {"fetched": 0, "skipped": 0, "errors": 0}

    for _, row in universe.iterrows():
        symbol = row["ticker"]
        path = _target_path(symbol)

        if not force and not _is_stale(path, max_age_days):
            logger.debug("[%s] Fresh file exists — skipping", symbol)
            stats["skipped"] += 1
            continue

        logger.info("[%s] Fetching weekly adjusted prices", symbol)

        try:
            data = client.weekly_adjusted(symbol)

            if data is None:
                logger.warning("[%s] No data returned from Alpha Vantage", symbol)
                stats["errors"] += 1
                continue

            if "Weekly Adjusted Time Series" not in data:
                logger.warning(
                    "[%s] Unexpected response structure: %s",
                    symbol, list(data.keys()),
                )
                stats["errors"] += 1
                continue

            write_json(data, path)
            n_weeks = len(data["Weekly Adjusted Time Series"])
            logger.info("[%s] Saved %d weekly bars → %s", symbol, n_weeks, path.name)
            stats["fetched"] += 1

        except Exception as exc:
            logger.error("[%s] Price fetch failed: %s", symbol, exc)
            stats["errors"] += 1

    logger.info(
        "fetch_prices complete — fetched: %d | skipped: %d | errors: %d",
        stats["fetched"], stats["skipped"], stats["errors"],
    )
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch weekly adjusted prices from Alpha Vantage.")
    parser.add_argument("--force", action="store_true", help="Re-download even if a fresh file exists.")
    parser.add_argument("--max-age-days", type=int, default=1, help="Days before a cached file is considered stale.")
    args = parser.parse_args()

    stats = run(force=args.force, max_age_days=args.max_age_days)
    if stats["errors"] > stats["fetched"] and stats["fetched"] == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
