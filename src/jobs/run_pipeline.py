"""
Job: run_pipeline
=================
Orchestrator — runs the full data pipeline:

    1. fetch_prices     — download weekly prices via yfinance (no API limit)
    2. compute_factors  — compute factor inputs + score all tickers
    3. publish_site_data — write JSON snapshots for frontend

SEC EDGAR fetching is optional (used for deep XBRL analysis).
yfinance provides both prices AND fundamentals for the S&P 500.

Each step runs in try/except — failure in one step does not stop the pipeline.

Usage
-----
    python -m src.jobs.run_pipeline
    python -m src.jobs.run_pipeline --force
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from typing import Callable

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("run_pipeline")


def _step_fetch_prices(force: bool) -> None:
    from src.jobs.fetch_prices import run as _run
    stats = _run(force=force)
    if stats["errors"] > 0:
        logger.warning("fetch_prices: %d errors (%d fetched, %d skipped)",
                        stats["errors"], stats["fetched"], stats["skipped"])


def _step_compute_factors(_force: bool) -> None:
    from src.jobs.compute_factors import run as _run
    df = _run()
    if df.empty:
        raise RuntimeError("compute_factors produced empty DataFrame")
    logger.info("compute_factors: %d scored rows", len(df))


def _step_publish(_force: bool) -> None:
    from src.jobs.publish_site_data import run as _run
    _run()


_ALL_STEPS: list[tuple[str, Callable[[bool], None]]] = [
    ("fetch_prices",      _step_fetch_prices),
    ("compute_factors",   _step_compute_factors),
    ("publish_site_data", _step_publish),
]
_STEP_NAMES = [name for name, _ in _ALL_STEPS]


def run(force: bool = False, steps: list[str] | None = None) -> bool:
    selected = [
        (name, fn) for name, fn in _ALL_STEPS
        if steps is None or name in steps
    ]
    if not selected:
        logger.error("No valid steps. Available: %s", _STEP_NAMES)
        return False

    results: dict[str, bool] = {}
    t0 = time.monotonic()

    logger.info("=" * 60)
    logger.info("Pipeline start | steps=%s | force=%s", [n for n, _ in selected], force)
    logger.info("=" * 60)

    for name, fn in selected:
        logger.info("─── %s ───", name)
        st = time.monotonic()
        try:
            fn(force)
            logger.info("✓ %s (%.1f s)", name, time.monotonic() - st)
            results[name] = True
        except Exception as exc:
            logger.warning("✗ %s FAILED (%.1f s): %s", name, time.monotonic() - st, exc)
            results[name] = False

    ok = sum(v for v in results.values())
    fail = len(results) - ok
    logger.info("=" * 60)
    logger.info("Pipeline done in %.1f s | %d/%d OK", time.monotonic() - t0, ok, len(results))
    for n, v in results.items():
        logger.info("  [%s] %s", "OK" if v else "FAIL", n)
    logger.info("=" * 60)

    return fail == 0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--steps", nargs="+", choices=_STEP_NAMES, default=None)
    args = parser.parse_args()
    run(force=args.force, steps=args.steps)


if __name__ == "__main__":
    main()
