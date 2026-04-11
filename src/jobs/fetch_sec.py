"""
Job: fetch_sec
==============
For every enabled ticker in universe.csv, downloads:
  - companyfacts  → data/raw/sec/{symbol}/companyfacts.json
  - submissions   → data/raw/sec/{symbol}/submissions.json

Skips a ticker if both files already exist AND --force flag is not set,
so re-runs are cheap (only fetches what is missing).

Usage
-----
    python -m src.jobs.fetch_sec
    python -m src.jobs.fetch_sec --force   # re-download everything
"""

from __future__ import annotations

import argparse
import logging
import sys

from pathlib import Path

from src.lib.io_utils import DATA_RAW, read_universe, write_json
from src.lib.sec_client import SECClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("fetch_sec")


def _target_paths(symbol: str) -> tuple[Path, Path]:
    """Return (companyfacts_path, submissions_path) for a given symbol."""
    base = DATA_RAW / "sec" / symbol.upper()
    return base / "companyfacts.json", base / "submissions.json"


def run(force: bool = False) -> dict[str, int]:
    """
    Main entry point.

    Parameters
    ----------
    force : bool
        Re-download even if files already exist.

    Returns
    -------
    dict with keys 'fetched', 'skipped', 'errors'
    """
    universe = read_universe(enabled_only=True)
    client = SECClient()

    stats = {"fetched": 0, "skipped": 0, "errors": 0}

    for _, row in universe.iterrows():
        symbol = row["ticker"]
        cik = row["cik"]

        cf_path, sub_path = _target_paths(symbol)

        if not force and cf_path.exists() and sub_path.exists():
            logger.debug("[%s] Both files exist — skipping", symbol)
            stats["skipped"] += 1
            continue

        logger.info("[%s] Fetching SEC data (CIK %s)", symbol, cik)

        # --- companyfacts ---
        try:
            facts = client.companyfacts(cik)
            if facts is None:
                logger.warning("[%s] companyfacts returned None (CIK %s)", symbol, cik)
                stats["errors"] += 1
            else:
                write_json(facts, cf_path)
                logger.info("[%s] companyfacts saved (%d bytes)", symbol, cf_path.stat().st_size)
        except Exception as exc:
            logger.error("[%s] companyfacts fetch failed: %s", symbol, exc)
            stats["errors"] += 1

        # --- submissions ---
        try:
            subs = client.submissions(cik)
            if subs is None:
                logger.warning("[%s] submissions returned None (CIK %s)", symbol, cik)
                stats["errors"] += 1
            else:
                write_json(subs, sub_path)
                logger.info("[%s] submissions saved (%d bytes)", symbol, sub_path.stat().st_size)
        except Exception as exc:
            logger.error("[%s] submissions fetch failed: %s", symbol, exc)
            stats["errors"] += 1

        stats["fetched"] += 1

    logger.info(
        "fetch_sec complete — fetched: %d | skipped: %d | errors: %d",
        stats["fetched"], stats["skipped"], stats["errors"],
    )
    return stats


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch SEC EDGAR data for universe tickers.")
    parser.add_argument(
        "--force", action="store_true",
        help="Re-download files even if they already exist.",
    )
    args = parser.parse_args()

    stats = run(force=args.force)
    if stats["errors"] > 0:
        sys.exit(1)  # Non-zero exit signals partial failure to GitHub Actions


if __name__ == "__main__":
    main()
