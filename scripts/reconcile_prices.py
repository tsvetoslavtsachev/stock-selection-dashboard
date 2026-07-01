#!/usr/bin/env python3
"""A-gate (INIT-22 P9): reconcile the price-archive swap for stock-selection.

Read-only verification, run locally against a real price-archive checkout
(DATACORE_ROOT + collectors/data-core on PYTHONPATH). Three checks, none of which
touches the archive:

  1. MAPPING DIFF   — every enabled universe ticker (dotted, "BRK.B") translated
     to the archive/Yahoo dash form and looked up in symbol_to_series(); reports
     the UNMAPPED set (would silently fall to yfinance).
  2. CLOSE IDENTITY — for a sample, the archive's drift-proof total-return Close
     vs a live yfinance daily auto_adjust=True Close over the overlapping dates;
     max abs pct diff (expected ~1e-6..1e-4, RIV-2 == auto_adjust).
  3. COVERAGE       — base/fetch/missing from price_source.json if present.

Usage:
    DATACORE_ROOT=/path/to/price-archive \
    PYTHONPATH=/path/to/data-core:/path/to/collectors \
    python scripts/reconcile_prices.py [N_SAMPLE]
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.lib.io_utils import read_universe  # noqa: E402
from src.lib.yfinance_client import _to_yahoo_symbol  # noqa: E402


def main(n_sample: int = 15) -> int:
    try:
        from collectors.price.consumer import read_base_close, symbol_to_series
    except ImportError as exc:
        print(f"reconcile: consumer not importable ({exc!r}); set PYTHONPATH", file=sys.stderr)
        return 2
    import yfinance as yf

    uni = read_universe(enabled_only=True)
    dot = [str(t).upper() for t in uni["ticker"]]
    dash_of = {t: _to_yahoo_symbol(t) for t in dot}
    sym_map = symbol_to_series()

    # ---- 1. Mapping diff --------------------------------------------------------
    unmapped = [t for t in dot if sym_map.get(dash_of[t]) is None]
    print(f"[1] MAPPING: {len(dot) - len(unmapped)}/{len(dot)} mapped; "
          f"{len(unmapped)} unmapped")
    if unmapped:
        print("    unmapped (-> yfinance fallback):", ", ".join(unmapped[:25])
              + (" ..." if len(unmapped) > 25 else ""))

    # ---- 2. Close identity (sample) --------------------------------------------
    mapped = [t for t in dot if t not in unmapped]
    step = max(1, len(mapped) // n_sample)
    sample = mapped[::step][:n_sample]
    dash_sample = [dash_of[t] for t in sample]

    base_close, _ = read_base_close(dash_sample, period="1y")
    print(f"[2] IDENTITY: archive vs yfinance daily auto_adjust, {len(sample)} names")
    worst = 0.0
    worst_sym = None
    for t, d in zip(sample, dash_sample):
        if d not in base_close.columns:
            print(f"    {t:6s} archive: EMPTY (fallback candidate)")
            continue
        a = base_close[d].dropna()
        try:
            y = yf.Ticker(d).history(period="1y", interval="1d", auto_adjust=True)["Close"]
        except Exception as exc:  # noqa: BLE001
            print(f"    {t:6s} yfinance error: {exc!r}")
            continue
        y.index = y.index.tz_localize(None)
        idx = a.index.intersection(y.index)
        if len(idx) < 20:
            print(f"    {t:6s} <20 overlapping bars ({len(idx)}) — skip")
            continue
        diff = (a.reindex(idx) / y.reindex(idx) - 1.0).abs()
        mx = float(diff.max())
        if mx > worst:
            worst, worst_sym = mx, t
        print(f"    {t:6s} n={len(idx):4d}  max|diff|={mx:.6%}")
    print(f"    -> worst {worst:.6%} ({worst_sym})")

    # ---- 3. Coverage ------------------------------------------------------------
    ps = ROOT / "price_source.json"
    if ps.exists():
        summ = json.loads(ps.read_text(encoding="utf-8")).get("summary", {})
        print(f"[3] COVERAGE (price_source.json): {summ}")
    else:
        print("[3] COVERAGE: price_source.json not present (run fetch_prices first)")

    return 0


if __name__ == "__main__":
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 15
    raise SystemExit(main(n))
