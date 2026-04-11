"""
Job: run_pipeline
=================
Orchestrator that runs the full data pipeline sequentially:

    1. fetch_sec        — download SEC EDGAR companyfacts + submissions
    2. fetch_prices     — download Alpha Vantage weekly adjusted prices
    3. compute_factors  — compute factor inputs and score all tickers
    4. publish_site_data — write JSON snapshots for the frontend

Each step runs inside a try/except block.  A failure in one step emits a
WARNING and allows the pipeline to continue, ensuring that partial data
(e.g. previously cached files) can still produce a publishable dashboard.

Exit code 0 if all steps succeed or produce partial results.
Exit code 1 only if EVERY step fails (total pipeline failure).

Usage
-----
    python -m src.jobs.run_pipeline
    python -m src.jobs.run_pipeline --force    # force re-download of all raw data
    python -m src.jobs.run_pipeline --steps fetch_sec fetch_prices  # run subset
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


# ─── Step definitions ─────────────────────────────────────────────────────────

def _step_fetch_sec(force: bool) -> None:
    from src.jobs.fetch_sec import run as _run
    stats = _run(force=force)
    if stats["errors"] > 0:
        logger.warning(
            "fetch_sec completed with %d errors (%d fetched, %d skipped)",
            stats["errors"], stats["fetched"], stats["skipped"],
        )


def _step_fetch_prices(force: bool) -> None:
    from src.jobs.fetch_prices import run as _run
    stats = _run(force=force)
    if stats["errors"] > 0:
        logger.warning(
            "fetch_prices completed with %d errors (%d fetched, %d skipped)",
            stats["errors"], stats["fetched"], stats["skipped"],
        )


def _step_compute_factors(_force: bool) -> None:
    from src.jobs.compute_factors import run as _run
    df = _run()
    if df.empty:
        raise RuntimeError("compute_factors produced an empty DataFrame — nothing to score")
    logger.info("compute_factors produced %d scored rows", len(df))


def _step_publish(_force: bool) -> None:
    from src.jobs.publish_site_data import run as _run
    _run()


# Ordered step registry: (step_name, callable)
_ALL_STEPS: list[tuple[str, Callable[[bool], None]]] = [
    ("fetch_sec",         _step_fetch_sec),
    ("fetch_prices",      _step_fetch_prices),
    ("compute_factors",   _step_compute_factors),
    ("publish_site_data", _step_publish),
]

_STEP_NAMES = [name for name, _ in _ALL_STEPS]


# ─── Orchestrator ─────────────────────────────────────────────────────────────

def run(force: bool = False, steps: list[str] | None = None) -> bool:
    """
    Execute pipeline steps sequentially.

    Parameters
    ----------
    force : bool
        Pass force=True to fetch_sec and fetch_prices.
    steps : list[str] | None
        Subset of step names to run (in order). Runs all if None.

    Returns
    -------
    bool
        True if every attempted step succeeded, False otherwise.
    """
    selected = [
        (name, fn) for name, fn in _ALL_STEPS
        if steps is None or name in steps
    ]

    if not selected:
        logger.error("No valid steps selected. Available: %s", _STEP_NAMES)
        return False

    results: dict[str, bool] = {}
    pipeline_start = time.monotonic()

    logger.info("=" * 60)
    logger.info("Pipeline start | steps: %s | force=%s", [n for n, _ in selected], force)
    logger.info("=" * 60)

    for step_name, step_fn in selected:
        logger.info("─── Step: %s ───", step_name)
        step_start = time.monotonic()

        try:
            step_fn(force)
            elapsed = time.monotonic() - step_start
            logger.info("✓ %s completed in %.1f s", step_name, elapsed)
            results[step_name] = True

        except Exception as exc:
            elapsed = time.monotonic() - step_start
            logger.warning(
                "✗ %s FAILED after %.1f s: %s — continuing pipeline",
                step_name, elapsed, exc,
            )
            results[step_name] = False

    # ── Summary ──
    total_elapsed = time.monotonic() - pipeline_start
    successes = sum(1 for ok in results.values() if ok)
    failures  = sum(1 for ok in results.values() if not ok)

    logger.info("=" * 60)
    logger.info(
        "Pipeline finished in %.1f s | %d/%d steps succeeded",
        total_elapsed, successes, len(results),
    )

    for name, ok in results.items():
        status = "OK " if ok else "FAIL"
        logger.info("  [%s] %s", status, name)

    logger.info("=" * 60)

    all_ok = failures == 0
    return all_ok


# ─── CLI ──────────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run the stock-selection dashboard data pipeline."
    )
    parser.add_argument(
        "--force", action="store_true",
        help="Force re-download of raw data (passed to fetch_sec and fetch_prices).",
    )
    parser.add_argument(
        "--steps", nargs="+", choices=_STEP_NAMES, default=None,
        metavar="STEP",
        help=(
            "Run only specific steps (space-separated). "
            f"Available: {', '.join(_STEP_NAMES)}. "
            "Default: run all steps."
        ),
    )
    args = parser.parse_args()

    all_ok = run(force=args.force, steps=args.steps)

    if not all_ok:
        logger.warning(
            "Pipeline completed with failures. "
            "Check warnings above. Dashboard may reflect stale data."
        )
        # Do NOT exit(1) by default — partial dashboard is better than no publish.
        # Change to sys.exit(1) if you want GitHub Actions to mark the run as failed.


if __name__ == "__main__":
    main()
