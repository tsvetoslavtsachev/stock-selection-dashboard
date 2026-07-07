"""
SEC EDGAR point-in-time fundamental collector.

For every CIK in config/universe.csv this downloads the full XBRL companyfacts
JSON (a single document that carries the entire filing history, 2009+) and caches
it at research/cache/edgar/CIK{cik10}.json.

Design notes
------------
* Resumable   — a CIK whose cache file already exists (and parses) is skipped, so
                a re-run only fetches what is missing. The multi-tens-of-minutes
                first run can be interrupted and resumed at no cost.
* Rate limit  — the fetch goes through src.lib.sec_client.SECClient, which already
                sleeps ~0.12 s between requests (~8 req/s), inside SEC's 10 req/s
                cap. We do NOT re-implement throttling.
* Robust      — a CIK that fails (network / 500 / malformed) is retried twice more
                (on top of SECClient's own per-request retry); persistent failures
                are written to research/data/edgar_failures.csv and the run keeps
                going. Three bad names never abort the batch.
* Progress    — a log line every PROGRESS_EVERY CIKs.

CLI
---
    python -m research.fundamentals.collect_edgar
    python -m research.fundamentals.collect_edgar --limit 10   # smoke test
    python -m research.fundamentals.collect_edgar --force      # ignore cache

Run from the repo root so ``src`` and ``config`` resolve.
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import time
from pathlib import Path

# Repo root = two levels up from this file (research/fundamentals/ -> repo/).
_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.lib.sec_client import SECClient  # noqa: E402

logger = logging.getLogger("collect_edgar")

_UNIVERSE_PATH = _REPO_ROOT / "config" / "universe.csv"
_CACHE_DIR = _REPO_ROOT / "research" / "cache" / "edgar"
_FAILURES_PATH = _REPO_ROOT / "research" / "data" / "edgar_failures.csv"

PROGRESS_EVERY = 25
RETRY_ATTEMPTS = 2        # extra attempts on top of SECClient's internal retry
RETRY_BACKOFF = 3.0       # seconds between our own retries


def _pad10(cik: str | int) -> str:
    """Zero-pad a CIK to the 10-digit form used in the cache filename."""
    return str(int(cik)).zfill(10)


def load_universe(path: Path = _UNIVERSE_PATH) -> list[dict]:
    """Read universe.csv -> list of {symbol, cik, ...}. Only enabled==1 rows with a
    non-empty CIK are kept (the whole universe is enabled today, but the guard
    keeps the collector honest if that ever changes)."""
    rows: list[dict] = []
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("enabled", "1").strip() not in ("1", ""):
                continue
            cik = (row.get("cik") or "").strip()
            if not cik:
                continue
            rows.append(row)
    return rows


def _cache_path(cik: str | int) -> Path:
    return _CACHE_DIR / f"CIK{_pad10(cik)}.json"


def _is_cached(cik: str | int) -> bool:
    """A CIK counts as cached only if the file exists AND parses as JSON — a
    truncated file from a killed run is treated as missing and re-fetched."""
    path = _cache_path(cik)
    if not path.exists() or path.stat().st_size == 0:
        return False
    try:
        with open(path, encoding="utf-8") as f:
            json.load(f)
        return True
    except (json.JSONDecodeError, OSError):
        return False


def _fetch_one(client: SECClient, cik: str) -> dict | None:
    """Fetch companyfacts for one CIK with our own retry loop layered on top of
    SECClient. Returns the JSON dict, or None if the CIK genuinely has no
    companyfacts (404 -> SECClient returns None; not an error)."""
    last_exc: Exception | None = None
    for attempt in range(1, RETRY_ATTEMPTS + 2):  # 1 initial + RETRY_ATTEMPTS
        try:
            return client.companyfacts(cik)
        except Exception as exc:  # noqa: BLE001 - want to survive anything transient
            last_exc = exc
            logger.warning("CIK %s fetch attempt %d failed: %s", cik, attempt, exc)
            if attempt <= RETRY_ATTEMPTS:
                time.sleep(RETRY_BACKOFF * attempt)
    raise last_exc  # type: ignore[misc]


def collect(limit: int | None = None, force: bool = False) -> dict:
    """Run the collector over the universe. Returns a summary dict with counts."""
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    _FAILURES_PATH.parent.mkdir(parents=True, exist_ok=True)

    universe = load_universe()
    if limit is not None:
        universe = universe[:limit]
    total = len(universe)

    client = SECClient()
    n_fetched = 0
    n_skipped = 0
    n_nofacts = 0
    failures: list[dict] = []

    logger.info("Collecting companyfacts for %d CIKs -> %s", total, _CACHE_DIR)
    t0 = time.time()

    for i, row in enumerate(universe, start=1):
        symbol = row.get("symbol", "?")
        cik = row["cik"].strip()

        if not force and _is_cached(cik):
            n_skipped += 1
        else:
            try:
                data = _fetch_one(client, cik)
            except Exception as exc:  # noqa: BLE001
                logger.error("CIK %s (%s) permanently failed: %s", cik, symbol, exc)
                failures.append({"symbol": symbol, "cik": cik, "error": str(exc)[:300]})
                if i % PROGRESS_EVERY == 0 or i == total:
                    _log_progress(i, total, n_fetched, n_skipped, len(failures), t0)
                continue

            if data is None:
                # 404 / no XBRL facts filed — not an error, but recorded so the
                # coverage report can explain a missing name.
                logger.warning("CIK %s (%s) has no companyfacts (404).", cik, symbol)
                failures.append({"symbol": symbol, "cik": cik, "error": "no_companyfacts_404"})
                n_nofacts += 1
            else:
                tmp = _cache_path(cik).with_suffix(".json.tmp")
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(data, f)
                tmp.replace(_cache_path(cik))  # atomic; no half-written cache files
                n_fetched += 1

        if i % PROGRESS_EVERY == 0 or i == total:
            _log_progress(i, total, n_fetched, n_skipped, len(failures), t0)

    _write_failures(failures)

    elapsed = time.time() - t0
    summary = {
        "total": total,
        "fetched": n_fetched,
        "skipped_cached": n_skipped,
        "no_companyfacts": n_nofacts,
        "failed": len(failures),
        "cached_now": sum(1 for r in universe if _is_cached(r["cik"].strip())),
        "elapsed_sec": round(elapsed, 1),
    }
    logger.info("DONE: %s", summary)
    return summary


def _log_progress(i, total, fetched, skipped, failed, t0):
    rate = i / max(time.time() - t0, 1e-9)
    logger.info(
        "[%d/%d] fetched=%d skipped=%d failed=%d (%.1f CIK/s)",
        i, total, fetched, skipped, failed, rate,
    )


def _write_failures(failures: list[dict]) -> None:
    """Always (re)write the failures file so a clean re-run empties it."""
    _FAILURES_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(_FAILURES_PATH, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["symbol", "cik", "error"])
        w.writeheader()
        for row in failures:
            w.writerow(row)
    if failures:
        logger.warning("%d CIK(s) failed -> %s", len(failures), _FAILURES_PATH)


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Collect SEC EDGAR companyfacts for the universe.")
    p.add_argument("--limit", type=int, default=None, help="only the first N CIKs (smoke test)")
    p.add_argument("--force", action="store_true", help="re-fetch even if cached")
    p.add_argument("--quiet", action="store_true", help="WARNING-level logging only")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    summary = collect(limit=args.limit, force=args.force)
    # Non-zero exit only if EVERYTHING failed (a total outage), not for a few names.
    if summary["total"] > 0 and summary["fetched"] == 0 and summary["skipped_cached"] == 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
