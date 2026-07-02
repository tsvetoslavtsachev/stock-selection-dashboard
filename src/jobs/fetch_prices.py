"""
Job: fetch_prices
=================
For every enabled ticker in universe.csv, materialise a DAILY total-return close
series to:

    data/raw/prices/{symbol}.csv

INIT-22 P9 strangler — the prices come base-first FROM the canonical price-archive
(``collectors.price.consumer.load_ohlcv_base_first``): split-adjusted, drift-proof
total-return close (== yfinance auto_adjust=True to ~1e-6, RIV-2 capstone), shared
by every price consumer. The OLD per-ticker yfinance pull is kept as a CLOSED
fallback (now DAILY, unit-consistent with the archive) so production never stops
when the archive is not checked out or a symbol is missing from it.

Symbol form: universe.csv keys class shares with a DOT ("BRK.B", matching S&P/SEC),
but the archive catalog (and Yahoo) key them with a DASH ("BRK-B"). The consumer's
``symbol_to_series`` maps only the dash form, so a raw dotted symbol resolves to
SRC_UNMAPPED and silently drops to fallback. We translate dot->dash at the archive
boundary and map back to the dotted form for the CSV filename / record key.

Provenance (base / fetch / missing) per symbol is written to price_source.json for
the ``assert_base_sourced`` CI guard, which fails RED on a silent mass-fallback.

Usage
-----
    python -m src.jobs.fetch_prices
    python -m src.jobs.fetch_prices --force
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import pandas as pd

from src.lib.io_utils import DATA_RAW, ROOT, read_universe
from src.lib.yfinance_client import _to_yahoo_symbol, get_price_history

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("fetch_prices")

_PRICES_DIR = DATA_RAW / "prices"
_PRICE_SOURCE = ROOT / "price_source.json"

# ~3y of daily bars: comfortably covers 12-1 momentum (needs ~252 trading days),
# 52-week returns and 26-week volatility, while keeping the base read light.
_PERIOD = "3y"

# INIT-22 P9: base-first canonical prices; degrade to the OLD (now daily) yfinance
# fetch when the archive reader is not importable (a bare local run / CI without
# the archive checkout). The consumer (collectors.price.consumer) lives in the
# PUBLIC collectors repo and reads the PRIVATE price-archive via DATACORE_ROOT.
try:
    from collectors.price.consumer import load_ohlcv_base_first
    _HAVE_BASE = True
except ImportError:
    _HAVE_BASE = False


def _target_path(symbol: str) -> Path:
    return _PRICES_DIR / f"{symbol.upper()}.csv"


def _daily_yf_fallback(missing: list[str], period: str | None = None) -> dict[str, pd.DataFrame]:
    """CLOSED fallback: DAILY yfinance close for the (Yahoo/dash-form) *missing*
    tickers, shaped as the consumer expects — dict{field -> flat DataFrame}. Only
    Close is produced (the ranker uses close only); other fields are left to the
    base read via the consumer's per-field merge."""
    close: dict[str, pd.Series] = {}
    for sym in missing:
        try:
            df = get_price_history(sym, period=period or _PERIOD, interval="1d")
            if df is None or df.empty:
                continue
            s = df["Close"]
            # yfinance returns a tz-aware index; the archive base is tz-naive. Strip
            # the tz so the consumer's base+fallback merge does not raise on a
            # naive/aware comparison (mirrors the sibling's _naive discipline).
            if getattr(s.index, "tz", None) is not None:
                s = s.copy()
                s.index = s.index.tz_localize(None)
            # Drop duplicate timestamps: a duplicated index makes pd.DataFrame(dict)
            # raise ("cannot reindex on an axis with duplicate labels"), which would
            # abort the whole (already-degraded) fallback run.
            s = s[~s.index.duplicated(keep="last")]
            close[sym] = s
        except Exception as exc:  # noqa: BLE001 — one bad symbol must not kill the fallback
            logger.warning("[%s] daily fallback failed: %s", sym, exc)
    return {"Close": pd.DataFrame(close)}


def _base_first_daily_close(dash_tickers: list[str], source_acc: dict[str, str]) -> pd.DataFrame:
    """Daily total-return Close (DatetimeIndex x dash-tickers), base-first with the
    CLOSED yfinance fallback. Fills ``source_acc`` with base/fetch/missing per
    symbol (P9 provenance). ANY base-read failure degrades to the pure fallback —
    the strangler never hard-stops."""
    def _pure_fetch() -> pd.DataFrame:
        close = _daily_yf_fallback(dash_tickers).get("Close", pd.DataFrame())
        for s in dash_tickers:
            source_acc[s] = "fetch" if s in close.columns else "missing"
        return close

    if not _HAVE_BASE:
        logger.warning("collectors.price.consumer not importable — pure yfinance fallback (no base)")
        return _pure_fetch()

    try:
        ohlcv, source_map = load_ohlcv_base_first(
            dash_tickers, fetch_fallback=_daily_yf_fallback,
            period=_PERIOD, normalize_currency=False,  # S&P 500 is pure USD -> no-op
        )
    except Exception as exc:  # noqa: BLE001 — strangler: ANY base failure degrades
        logger.warning("base read raised (%r); degrading to yfinance (strangler)", exc)
        return _pure_fetch()

    source_acc.update(source_map)
    for s in dash_tickers:
        source_acc.setdefault(s, "missing")  # requested but neither base nor fetch served it
    return ohlcv.get("Close", pd.DataFrame())


def _write_price_source(source_acc: dict[str, str], expected: int) -> None:
    """Per-symbol price provenance (base vs fetch) for assert_base_sourced.py."""
    n_base = sum(1 for v in source_acc.values() if v == "base")
    payload = {
        "by_symbol": dict(sorted(source_acc.items())),
        "summary": {"expected": expected, "covered": len(source_acc),
                    "base": n_base, "fetch": len(source_acc) - n_base},
    }
    _PRICE_SOURCE.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
                             encoding="utf-8")


def run(force: bool = False, max_age_days: int = 1) -> dict[str, int]:
    """Rebuild all daily price CSVs from the archive base (force/max_age_days kept
    for CLI/orchestrator parity — the base read is cheap, so we always rebuild)."""
    universe = read_universe(enabled_only=True)
    dot_tickers = [str(t).upper() for t in universe["ticker"]]
    # dot ("BRK.B") <-> dash ("BRK-B", the archive/Yahoo form) both directions.
    dash_of = {t: _to_yahoo_symbol(t) for t in dot_tickers}
    # The dot->dash map must be 1:1; otherwise a dotted class share and a literal
    # dash ticker collapse to one archive/Yahoo key and one constituent silently
    # loses its CSV + provenance on the map-back.
    if len(set(dash_of.values())) != len(dash_of):
        seen: set[str] = set()
        dups = sorted({d for d in dash_of.values() if d in seen or seen.add(d)})
        raise ValueError(f"dot->dash symbol collision (would drop a constituent): {dups}")
    dot_of = {d: t for t, d in dash_of.items()}
    dash_tickers = [dash_of[t] for t in dot_tickers]

    _PRICES_DIR.mkdir(parents=True, exist_ok=True)

    source_acc: dict[str, str] = {}
    close_df = _base_first_daily_close(dash_tickers, source_acc)

    stats = {"fetched": 0, "skipped": 0, "errors": 0}
    for dash in close_df.columns:
        dot = dot_of.get(dash, dash)
        s = close_df[dash].dropna().astype(float)
        if s.empty:
            logger.warning("[%s] Empty close series", dot)
            stats["errors"] += 1
            continue
        out = s.rename("Close").to_frame()
        out.index.name = "Date"
        out.to_csv(_target_path(dot))
        stats["fetched"] += 1

    # Any requested ticker with no column at all is a genuine miss.
    stats["errors"] += sum(1 for d in dash_tickers if d not in close_df.columns)

    # Provenance keyed back to the dotted (universe) form for the guard/report.
    prov = {dot_of.get(d, d): v for d, v in source_acc.items()}
    _write_price_source(prov, expected=len(dot_tickers))

    n_base = sum(1 for v in prov.values() if v == "base")
    n_fetch = sum(1 for v in prov.values() if v == "fetch")
    logger.info(
        "fetch_prices done — wrote: %d | errors: %d | source: %d base / %d fetch",
        stats["fetched"], stats["errors"], n_base, n_fetch,
    )
    return stats


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--max-age-days", type=int, default=1)
    args = parser.parse_args()
    stats = run(force=args.force, max_age_days=args.max_age_days)
    if stats["fetched"] == 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
