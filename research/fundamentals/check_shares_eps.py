"""
Share-count check against the company's own EPS (ПНЛ1 гейт Г3, 12.10.2026).

For each ticker: our synthetic EPS = net_income_ttm / shares_outstanding, both as
known at --as-of from the PIT panel (the exact pair the valuation-organ screen
divides), against the company's reported EPS for the same trailing twelve months
(FY + YTD - prior-year YTD, from companyfacts; for issuers that tag EPS only per
class, from the per-class cache on the unit-class member of share_classes.csv).

A diluted ratio synthetic/reported outside 1 +/- --tol is printed as a breach, with
the basic ratio next to it. Expected, explainable gaps: diluted vs basic count
(options, converts, Up-C units exchanged in the diluted EPS), buybacks between the
weighted twelve months and the cover date, a different net-income line (NCI).
A split inside the window is handled: the fiscal-year EPS filed before the split is
rescaled by the restatement of the prior-year YTD comparative (CVNA 5:1, 2026).

    python -m research.fundamentals.check_shares_eps --tickers META V BRK.B
    python -m research.fundamentals.check_shares_eps --table      # every table ticker
"""

from __future__ import annotations

import argparse
import csv
import json
import sys
from datetime import date, timedelta
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from research.fundamentals import build_panel as bp  # noqa: E402
from research.fundamentals.pit import as_known_at, load_panel  # noqa: E402

_D = date.fromisoformat

_EPS_TAGS = {
    "dil": ("EarningsPerShareDiluted", "IncomeLossFromContinuingOperationsPerDilutedShare"),
    "bas": ("EarningsPerShareBasic", "IncomeLossFromContinuingOperationsPerBasicShare"),
}


def _dur(s: str, e: str) -> int:
    return (_D(e) - _D(s)).days


def ttm_eps(facts: list[tuple]) -> tuple[float, str] | None:
    """Trailing-twelve-month EPS from duration facts
    (start, end, latest_val[, first_val, first_filed, latest_filed])."""
    facts = [tuple(f) if len(f) == 6 else (*f, None, "", "") for f in facts]
    facts = [f for f in facts if f[0] and f[1] and _dur(f[0], f[1]) >= 80]
    if not facts:
        return None
    end = max(f[1] for f in facts)
    at_end = [f for f in facts if f[1] == end]
    annual = [f for f in at_end if 350 <= _dur(f[0], f[1]) <= 380]
    if annual:
        return annual[0][2], end
    ytd = min(at_end, key=lambda f: f[0])  # longest period ending at `end`
    s = _D(ytd[0])

    def near(a: str, b: date, days: int = 10) -> bool:
        return abs((_D(a) - b).days) <= days

    prior = [f for f in facts if near(f[0], s - timedelta(days=365)) and near(f[1], _D(end) - timedelta(days=365))
             and abs(_dur(f[0], f[1]) - _dur(ytd[0], ytd[1])) <= 10]
    fy = [f for f in facts if near(f[1], s - timedelta(days=1)) and 350 <= _dur(f[0], f[1]) <= 380]
    if not prior or not fy:
        return None
    p, f = prior[0], fy[0]
    fy_val = f[2]
    # a split inside the window: a later filing restated the prior-year YTD, the
    # fiscal-year figure (last filed before that restatement) was not
    if p[3] and p[2] and abs(p[3] / p[2] - 1) > 0.2 and f[5] and f[5] < p[5]:
        fy_val = f[2] * p[2] / p[3]
    return fy_val + ytd[2] - p[2], end


def _first_last(items) -> list[tuple]:
    """(start, end, val, filed) items -> one tuple per period:
    (start, end, latest val, first val, first filed, latest filed)."""
    by: dict[tuple, list] = {}
    for s, e, v, filed in items:
        by.setdefault((s, e), []).append((filed, v))
    out = []
    for (s, e), xs in by.items():
        xs.sort()
        out.append((s, e, xs[-1][1], xs[0][1], xs[0][0], xs[-1][0]))
    return out


def _companyfacts_eps(cik10: str, kind: str) -> list[tuple]:
    p = bp._CACHE_DIR / f"CIK{cik10}.json"
    f = json.loads(p.read_text(encoding="utf-8"))["facts"].get("us-gaap", {})
    for tag in _EPS_TAGS[kind]:
        lst = f.get(tag, {}).get("units", {}).get("USD/shares", [])
        if lst:
            return _first_last((x.get("start", ""), x["end"], x["val"], x["filed"]) for x in lst)
    return []


def _class_eps(cik10: str, members: list[str], kind: str) -> list[tuple]:
    p = bp._CLASS_CACHE_DIR / f"CIK{cik10}.json"
    if not p.exists():
        return []
    cache = json.loads(p.read_text(encoding="utf-8"))
    k = "eps" if kind == "dil" else "epsb"
    return _first_last((s, en, v, e["filed"]) for e in cache.values()
                       for m, s, en, v in e.get(k, []) if m in members)


def _reported(cik10: str, spec: dict | None, kind: str) -> tuple | None:
    """(ttm, end, source): companyfacts, or the per-class cache when it is fresher."""
    rep = ttm_eps(_companyfacts_eps(cik10, kind))
    out = (rep[0], rep[1], "companyfacts") if rep else None
    if spec is not None:
        members = [m for m in (spec["eps"], spec["unit"]) if m]
        cl = ttm_eps(_class_eps(cik10, members, kind))
        if cl and (out is None or cl[1] > out[1]):
            out = (cl[0], cl[1], "per-class")
    return out


def check(tickers: list[str], as_of: str, tol: float) -> list[dict]:
    panel = load_panel(bp._PANEL_PATH)
    panel = panel[panel["ticker"].isin(tickers)]
    wide = as_known_at(panel, as_of)
    cik = dict(bp.load_universe())
    table = bp.load_class_table()
    out = []
    for t in tickers:
        row = wide.loc[t].to_dict() if t in wide.index else {}
        sh, ni = row.get("shares_outstanding"), row.get("net_income_ttm")
        spec = table.get(t)
        dil, bas = _reported(cik[t], spec, "dil"), _reported(cik[t], spec, "bas")
        rep = dil or bas
        syn = ni / sh if sh and ni == ni and sh == sh else None
        r = syn / rep[0] if syn and rep and rep[0] else None
        rb = syn / bas[0] if syn and bas and bas[0] else None
        out.append({"ticker": t, "shares": sh, "ni_ttm": ni, "syn_eps": syn,
                    "rep_eps": rep[0] if rep else None, "rep_end": rep[1] if rep else "",
                    "src": rep[2] if rep else "", "ratio": r,
                    "rep_eps_basic": bas[0] if bas else None, "ratio_basic": rb,
                    "ok": r is not None and abs(r - 1) <= tol})
    return out


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickers", nargs="*")
    ap.add_argument("--table", action="store_true", help="every ticker in share_classes.csv")
    ap.add_argument("--as-of", default=date.today().isoformat())
    ap.add_argument("--tol", type=float, default=0.05)
    ap.add_argument("--csv", help="write the full result here")
    a = ap.parse_args(argv)
    tickers = sorted(set((a.tickers or []) + (list(bp.load_class_table()) if a.table else [])))
    res = check(tickers, a.as_of, a.tol)
    fmt = lambda v, n=3: "-" if v is None else f"{v:.{n}f}"  # noqa: E731
    for r in res:
        if not r["ok"]:
            print(f"BREACH {r['ticker']:6} shares={fmt(r['shares'], 0)} syn={fmt(r['syn_eps'])} "
                  f"rep={fmt(r['rep_eps'])}@{r['rep_end']} ({r['src']}) ratio={fmt(r['ratio'])} "
                  f"basic={fmt(r['rep_eps_basic'])} ratio_basic={fmt(r['ratio_basic'])}")
    print(f"{sum(r['ok'] for r in res)}/{len(res)} within {a.tol:.0%}")
    if a.csv:
        with open(a.csv, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(res[0]))
            w.writeheader()
            w.writerows(res)
    return 0 if all(r["ok"] for r in res) else 1


if __name__ == "__main__":
    raise SystemExit(main())
