"""
Validation gates G2 and G3 for the EDGAR PIT panel (G1/G4/G5 live elsewhere: G1 is
the collector's own summary; G4/G5 are pytest tests).

G2 — coverage_report.csv
    * per concept: the % of the 503-name universe with >= 1 value dated in 2024+;
    * per year 2009-2026: the number of companies with any ``revenues`` fact.
    Thresholds: net_income, stockholders_equity, total_assets, revenues must clear
    90% for 2024+. Failures are printed loudly.

G3 — megacap ROE spot-check
    * for AAPL, MSFT, JPM, XOM, JNJ: as_known_at(today) ROE = net_income_ttm /
      stockholders_equity, compared to the ``roe`` column in data/processed/ranks.csv
      (yfinance). yfinance uses different definitions, so +/- 30% is fine; a 5x gap
      is a red flag. Prints a table.

G6 — staleness ratchet (12.10.2026)
    * for every (ticker, concept): how far the concept's latest period_end trails the
      same ticker's latest ``revenues`` period_end. More than _STALE_DAYS behind, or
      absent altogether, is STALE: a tag chain stopped following the filer (ORCL debt
      stuck at 2022-05-31, AVGO net income at 2024-11-03, before the chains widened).
    * research/data/staleness_baseline.csv lists the KNOWN gaps (legit ones such as a
      buyback program that ended, and not-yet-fixed tag gaps) with the last period_end
      each had when frozen. G6 FAILS (exit 1) on a stale pair missing from the baseline
      (NEW) or one whose last period_end moved backwards (REGRESSED); baseline pairs
      that caught up are reported as HEALED so the file only shrinks.
    * every baseline pair carries a ``reason`` (ПНЛ4, 13.10.2026): its class and the
      evidence (the tag and date of the last fact, the sector, the proof that failed).
      A still-stale pair with an empty reason FAILS G6 (UNEXPLAINED), and
      --write-baseline keeps the reasons of the pairs it carries over, so a newly
      frozen gap stays red until someone writes down why it is there.
    * G6d (ПНЛ2, 13.10.2026): a name declared debt-free (build_panel._DEBT_FREE) must
      carry total_debt 0 at its latest balance sheet, and a not-applicable name
      (debt.DEBT_NA: banks, broker-dealers, insurers) must carry no total_debt at all.
    * G6a (ПНЛ4б, 13.10.2026): depreciation_amortization is the full line; no row may
      be a bare `Depreciation` fact (PP&E only), and a build_panel._DA_NA name carries
      none. Reads the EDGAR cache; skipped where it is absent (CI).

CLI
    python -m research.fundamentals.validate_gates
    python -m research.fundamentals.validate_gates --panel OTHER.csv.gz   # G6+G6d only
    python -m research.fundamentals.validate_gates --panel P --no-stamp   # CI: never touches the stamp
    python -m research.fundamentals.validate_gates --write-baseline       # refreeze G6
"""

from __future__ import annotations

import argparse
import csv
import logging
import sys
from datetime import date
from pathlib import Path

import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from research.fundamentals import debt, g6_stamp, pit  # noqa: E402

logger = logging.getLogger("validate_gates")

_UNIVERSE_PATH = _REPO_ROOT / "config" / "universe.csv"
_PANEL_PATH = _REPO_ROOT / "research" / "data" / "edgar_pit_panel.csv.gz"
_COVERAGE_PATH = _REPO_ROOT / "research" / "data" / "coverage_report.csv"
_RANKS_PATH = _REPO_ROOT / "data" / "processed" / "ranks.csv"
_STALENESS_BASELINE_PATH = _REPO_ROOT / "research" / "data" / "staleness_baseline.csv"

_UNIVERSE_N = 503
_RECENT_YEAR = 2024
_G2_THRESHOLD = 0.90
_G2_REQUIRED = ["net_income", "stockholders_equity", "total_assets", "revenues"]
_MEGACAPS = ["AAPL", "MSFT", "JPM", "XOM", "JNJ"]
_STALE_DAYS = 100   # same threshold as valuation-organ scripts/build_cash_table.STALE_DAYS
_BASELINE_COLUMNS = ["ticker", "concept", "last_period_end", "revenues_period_end", "lag_days", "reason"]

_ALL_CONCEPTS = sorted(pit.STOCK_CONCEPTS | pit.FLOW_CONCEPTS)


def _universe_size() -> int:
    with open(_UNIVERSE_PATH, encoding="utf-8") as f:
        return sum(1 for r in csv.DictReader(f)
                   if (r.get("enabled", "1").strip() in ("1", "")) and (r.get("cik") or "").strip())


def _canon_symbols() -> set[str]:
    """The S&P 500 canon: enabled==1 rows only. A GUEST (enabled=2, outside the index, e.g. LYFT
    from УБР3) sits in the panel for the organ, but must not count in the S&P coverage numerator
    while universe_n is the canon size (it showed 504/503 = 100.2%)."""
    with open(_UNIVERSE_PATH, encoding="utf-8") as f:
        return {r["symbol"].strip() for r in csv.DictReader(f)
                if r.get("enabled", "1").strip() in ("1", "") and (r.get("cik") or "").strip()}


# ---------------------------------------------------------------------------
# G2 — coverage report
# ---------------------------------------------------------------------------
def g2_coverage(panel: pd.DataFrame, universe_n: int) -> tuple[pd.DataFrame, pd.DataFrame, list[str]]:
    """Return (per_concept_df, per_year_revenue_df, failures)."""
    panel = panel.copy()
    panel["end_year"] = panel["period_end"].dt.year

    # Per-concept coverage in RECENT_YEAR+ : distinct tickers with >=1 value.
    recent = panel[panel["end_year"] >= _RECENT_YEAR]
    concept_rows = []
    failures: list[str] = []
    for concept in _ALL_CONCEPTS:
        n = recent[recent["concept"] == concept]["ticker"].nunique()
        pct = n / universe_n
        concept_rows.append({"concept": concept, "n_with_value_2024plus": n,
                             "pct_of_universe": round(pct, 4)})
        if concept in _G2_REQUIRED and pct < _G2_THRESHOLD:
            failures.append(f"{concept}: {pct:.1%} < {_G2_THRESHOLD:.0%}")
    concept_df = pd.DataFrame(concept_rows).sort_values("pct_of_universe", ascending=False)

    # Per-year revenue coverage 2009-2026.
    rev = panel[panel["concept"] == "revenues"]
    year_rows = []
    for yr in range(2009, 2027):
        n = rev[rev["end_year"] == yr]["ticker"].nunique()
        year_rows.append({"year": yr, "companies_with_revenues": n})
    year_df = pd.DataFrame(year_rows)

    return concept_df, year_df, failures


def write_coverage_report(concept_df: pd.DataFrame, year_df: pd.DataFrame) -> None:
    """Write coverage_report.csv: concept block, blank line, then per-year block."""
    with open(_COVERAGE_PATH, "w", encoding="utf-8", newline="") as f:
        f.write("# G2 coverage report\n")
        f.write("# section: per-concept coverage (>=1 value with period_end in 2024+)\n")
        concept_df.to_csv(f, index=False)
        f.write("\n")
        f.write("# section: per-year company count with a revenues fact\n")
        year_df.to_csv(f, index=False)


# ---------------------------------------------------------------------------
# G3 — megacap ROE spot-check
# ---------------------------------------------------------------------------
def g3_roe_spotcheck(panel: pd.DataFrame) -> pd.DataFrame:
    """Compute as_known_at(today) ROE for the megacaps and compare to ranks.csv."""
    today = date.today()
    sub = panel[panel["ticker"].isin(_MEGACAPS)]
    wide = pit.as_known_at(sub, today)

    # yfinance ROE reference.
    yf_roe: dict[str, float] = {}
    if _RANKS_PATH.exists():
        ranks = pd.read_csv(_RANKS_PATH)
        for _, r in ranks.iterrows():
            if r["ticker"] in _MEGACAPS:
                try:
                    yf_roe[r["ticker"]] = float(r["roe"])
                except (TypeError, ValueError):
                    pass

    rows = []
    for t in _MEGACAPS:
        ni = wide.loc[t, "net_income_ttm"] if t in wide.index else float("nan")
        eq = wide.loc[t, "stockholders_equity"] if t in wide.index else float("nan")
        pit_roe = (ni / eq) if (pd.notna(ni) and pd.notna(eq) and eq != 0) else float("nan")
        ref = yf_roe.get(t, float("nan"))
        ratio = (pit_roe / ref) if (pd.notna(pit_roe) and pd.notna(ref) and ref != 0) else float("nan")
        flag = ""
        if pd.notna(ratio):
            if ratio > 5 or ratio < 0.2:
                flag = "RED (>5x gap)"
            elif ratio > 1.3 or ratio < 0.77:
                flag = "WARN (>30%)"
            else:
                flag = "OK"
        rows.append({
            "ticker": t,
            "net_income_ttm": None if pd.isna(ni) else round(ni / 1e9, 3),  # $B
            "equity": None if pd.isna(eq) else round(eq / 1e9, 3),          # $B
            "pit_roe": None if pd.isna(pit_roe) else round(pit_roe, 4),
            "yfinance_roe": None if pd.isna(ref) else round(ref, 4),
            "ratio": None if pd.isna(ratio) else round(ratio, 3),
            "verdict": flag,
        })
    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# G6 — staleness ratchet
# ---------------------------------------------------------------------------
def staleness_table(panel: pd.DataFrame) -> pd.DataFrame:
    """Every STALE (ticker, concept): latest period_end more than _STALE_DAYS behind
    the ticker's latest revenues period_end, or no rows at all (last_period_end empty).
    Tickers with no revenues have no yardstick and are skipped."""
    last = panel.groupby(["ticker", "concept"])["period_end"].max().unstack()
    if "revenues" not in last.columns:
        return pd.DataFrame(columns=_BASELINE_COLUMNS)
    last = last[last["revenues"].notna()]
    concepts = [c for c in _ALL_CONCEPTS if c != "revenues"]
    last = last.reindex(columns=["revenues"] + concepts)
    rows = []
    for ticker, r in last.iterrows():
        rev = r["revenues"]
        for c in concepts:
            end = r[c]
            lag = None if pd.isna(end) else (rev - end).days
            if lag is None or lag > _STALE_DAYS:
                rows.append({"ticker": ticker, "concept": c,
                             "last_period_end": None if pd.isna(end) else end.date().isoformat(),
                             "revenues_period_end": rev.date().isoformat(),
                             "lag_days": lag, "reason": ""})
    return pd.DataFrame(rows, columns=_BASELINE_COLUMNS)


def load_staleness_baseline(path: Path = _STALENESS_BASELINE_PATH) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=_BASELINE_COLUMNS)
    base = pd.read_csv(path, dtype={"ticker": str, "concept": str, "last_period_end": str,
                                    "reason": str})
    if "reason" not in base.columns:
        base["reason"] = ""
    base["reason"] = base["reason"].fillna("")
    return base


def g6_staleness(panel: pd.DataFrame, baseline: pd.DataFrame) -> dict:
    """Compare the panel's stale pairs with the frozen baseline.
    Returns {"stale", "new", "regressed", "unexplained", "healed", "failures"}."""
    stale = staleness_table(panel)
    base = {(r.ticker, r.concept): (None if pd.isna(r.last_period_end) else r.last_period_end)
            for r in baseline.itertuples()}
    stale = stale.astype(object).where(stale.notna(), None)   # absent -> None, not NaN
    reasons = {(r.ticker, r.concept): str(getattr(r, "reason", "") or "").strip()
               for r in baseline.itertuples()}
    new, regressed, unexplained = [], [], []
    for r in stale.itertuples():
        k = (r.ticker, r.concept)
        if k not in base:
            new.append(r)
            continue
        was = base[k]
        if was is not None and (r.last_period_end is None or r.last_period_end < was):
            regressed.append(r)
        if not reasons.get(k):
            unexplained.append(r)
    now = {(r.ticker, r.concept) for r in stale.itertuples()}
    healed = sorted(k for k in base if k not in now)

    def _last(v):
        return v if v else "absent"

    failures = [
        f"NEW {r.ticker}.{r.concept}: last {_last(r.last_period_end)}, revenues {r.revenues_period_end}"
        for r in new
    ] + [
        f"REGRESSED {r.ticker}.{r.concept}: last {_last(r.last_period_end)} < baseline {base[(r.ticker, r.concept)]}"
        for r in regressed
    ] + [
        f"UNEXPLAINED {r.ticker}.{r.concept}: no reason in the baseline"
        for r in unexplained
    ]
    return {"stale": stale, "new": new, "regressed": regressed, "unexplained": unexplained,
            "healed": healed, "failures": failures}


def g6d_debt_declarations(panel: pd.DataFrame, debt_free: dict) -> list[str]:
    """Failures of the debt declarations: a _DEBT_FREE name whose latest total_debt is
    nonzero, a debt.DEBT_NA name with any total_debt row."""
    d = panel[panel["concept"] == "total_debt"]
    fails = []
    for t, g in d[d["ticker"].isin(list(debt_free))].groupby("ticker"):
        last = g.sort_values(["period_end", "filed"]).iloc[-1]
        if last["value"] != 0:
            fails.append(f"DEBT_FREE {t}: total_debt {last['value']:,.0f} at {last['period_end']}")
    for t in sorted(set(d["ticker"]) & set(debt.DEBT_NA)):
        fails.append(f"DEBT_NA {t}: has total_debt rows")
    return fails


def g6a_da_full_line(panel: pd.DataFrame, cache_dir: Path | None = None) -> list[str]:
    """Failures of the D&A definition (ПНЛ4б): a depreciation_amortization row that is a
    bare `Depreciation` fact (same key and value) found in no admissible source (the
    chain, _FALLBACK_TAGS, _FALLBACK_SUMS), any row for a _DA_NA name, or (ПНЛ4д) a note
    tag's fact dropped for the cash-flow one (_DA_CF_*). Needs the
    EDGAR cache, which is git-ignored: without it the check is skipped (CI)."""
    import json
    from research.fundamentals import build_panel as bp
    cache_dir = cache_dir or bp._CACHE_DIR
    da = panel[panel["concept"] == "depreciation_amortization"]
    fails = [f"DA_NA {t}: has depreciation_amortization rows" for t in sorted(set(da["ticker"]) & set(bp._DA_NA))]
    if not any(cache_dir.glob("CIK*.json")):
        logger.info("G6a skipped: no EDGAR cache at %s", cache_dir)
        return fails
    concept = "depreciation_amortization"
    sources = bp._CONCEPTS[concept][1] + bp._FALLBACK_TAGS.get(concept, [])
    iso = lambda d: "" if pd.isna(d) else pd.Timestamp(d).date().isoformat()
    for cik, g in da.groupby("cik"):
        path = cache_dir / f"CIK{int(cik):010d}.json"
        if not path.exists():
            continue
        facts = json.loads(path.read_text(encoding="utf-8")).get("facts", {})
        key = lambda r: (r.get("start", ""), r["end"], r["filed"])
        dep = {key(r): r["val"] for r in bp._facts_for_tag(facts, "us-gaap", "Depreciation", "USD") if bp._valid(r)}
        ok = {key(r) + (r["val"],) for t in sources
              for r in bp._facts_for_tag(facts, "us-gaap", t, "USD") if bp._valid(r)}
        ok |= {key(r) + (r["val"],) for b, parts in bp._FALLBACK_SUMS.get(concept, [])
               for r in bp._sum_records(facts, b, parts)}
        for t, gt in g.groupby("ticker"):
            bad = [k for k in ((iso(r.period_start), iso(r.period_end), iso(r.filed), r.value)
                               for r in gt.itertuples())
                   if dep.get(k[:3]) == k[3] and k not in ok]
            if bad:
                fails.append(f"DA_NARROW {t}: {len(bad)} rows are bare Depreciation (e.g. {bad[-1][1]})")
            if bp._da_cash_flow_tag(t, "") is None:
                continue
            # ПНЛ4д: a name in _DA_CF_* takes, in a filing with both tags, only the
            # cash-flow statement's one; a row that is the note tag's dropped fact fails
            cf, _n = bp._da_cash_flow_facts(t, facts)
            kept = {key(r) + (r["val"],) for s in sources
                    for r in bp._facts_for_tag(cf, "us-gaap", s, "USD") if bp._valid(r)}
            note = {key(r) + (r["val"],) for s in bp._CONCEPTS[concept][1]
                    for r in bp._facts_for_tag(facts, "us-gaap", s, "USD") if bp._valid(r)} - kept
            bad = [k for k in ((iso(r.period_start), iso(r.period_end), iso(r.filed), r.value)
                               for r in gt.itertuples()) if k in note]
            if bad:
                fails.append(f"DA_NOTE {t}: {len(bad)} rows are the note tag beside the cash-flow one (e.g. {bad[-1][1]})")
    return fails


def g6_all(panel: pd.DataFrame) -> dict:
    """G6 staleness ratchet plus the G6d debt declarations and the G6a D&A definition,
    in one verdict."""
    from research.fundamentals.build_panel import _DEBT_FREE
    g6 = g6_staleness(panel, load_staleness_baseline())
    g6["failures"] += g6d_debt_declarations(panel, _DEBT_FREE)
    g6["failures"] += g6a_da_full_line(panel)
    return g6


def _stamp_if_canonical(panel_path: Path, g6: dict) -> None:
    """Write the G6 stamp, but only for the canonical panel: a check against some other
    file (a mutation test, a candidate rebuild) must never stamp the real one."""
    if Path(panel_path).resolve() != _PANEL_PATH.resolve():
        return
    stamp = g6_stamp.write_stamp(not g6["failures"], len(g6["failures"]),
                                 panel_path=_PANEL_PATH, stamp_path=g6_stamp.STAMP_PATH)
    logger.info("G6 stamp written: %s sha256=%s...", stamp["result"], stamp["panel_sha256"][:12])


def write_staleness_baseline(panel: pd.DataFrame, path: Path = _STALENESS_BASELINE_PATH) -> int:
    stale = staleness_table(panel).sort_values(["concept", "ticker"])
    old = load_staleness_baseline(path)
    keep = {(r.ticker, r.concept): r.reason for r in old.itertuples()}
    stale["reason"] = [keep.get((r.ticker, r.concept), "") for r in stale.itertuples()]
    stale.to_csv(path, index=False)
    return len(stale)


def _report_g6(g6: dict) -> None:
    logger.info("=== G6 staleness (concept > %dd behind revenues, or absent) ===", _STALE_DAYS)
    logger.info("  stale pairs now: %d (baseline-known: %d, new: %d, regressed: %d, "
                "unexplained: %d, healed: %d)",
                len(g6["stale"]), len(g6["stale"]) - len(g6["new"]),
                len(g6["new"]), len(g6["regressed"]), len(g6["unexplained"]), len(g6["healed"]))
    if g6["healed"]:
        logger.info("  HEALED (refreeze the baseline to lock them in): %s",
                    ", ".join(f"{t}.{c}" for t, c in g6["healed"][:40]))
    if g6["failures"]:
        for f in g6["failures"][:60]:
            logger.error("  G6 FAIL %s", f)
        if len(g6["failures"]) > 60:
            logger.error("  ... and %d more", len(g6["failures"]) - 60)
    else:
        logger.info("G6 PASS: no stale concept beyond the baseline")


# ---------------------------------------------------------------------------
# Runner
# ---------------------------------------------------------------------------
def run() -> dict:
    if not _PANEL_PATH.exists():
        raise SystemExit(f"panel not found: {_PANEL_PATH} (run build_panel first)")

    logger.info("Loading panel %s ...", _PANEL_PATH)
    full = pit.load_panel(_PANEL_PATH)
    # --- G6 --- (every paneled name, guests included: the organ reads them all)
    g6 = g6_all(full)
    _report_g6(g6)
    _stamp_if_canonical(_PANEL_PATH, g6)

    panel = full[full["ticker"].isin(_canon_symbols())]     # guests (enabled=2) are not S&P coverage
    universe_n = _universe_size()

    # --- G2 ---
    concept_df, year_df, failures = g2_coverage(panel, universe_n)
    write_coverage_report(concept_df, year_df)

    logger.info("=== G2 coverage (per concept, 2024+) ===")
    for _, r in concept_df.iterrows():
        logger.info("  %-28s %4d  %6.1f%%", r["concept"], r["n_with_value_2024plus"],
                    100 * r["pct_of_universe"])
    logger.info("=== G2 per-year revenue coverage ===")
    logger.info("  %s", {int(r["year"]): int(r["companies_with_revenues"]) for _, r in year_df.iterrows()})
    if failures:
        logger.warning("G2 THRESHOLD FAILURES (required concept < 90%% in 2024+): %s", failures)
    else:
        logger.info("G2 PASS: all required concepts >= 90%% in 2024+")

    # --- G3 ---
    g3 = g3_roe_spotcheck(panel)
    logger.info("=== G3 megacap ROE spot-check (as_known_at today vs yfinance) ===")
    logger.info("\n%s", g3.to_string(index=False))

    return {
        "universe_n": universe_n,
        "g2_failures": failures,
        "coverage_path": str(_COVERAGE_PATH),
        "g3": g3,
        "g6_failures": g6["failures"],
    }


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Run G2/G3/G6 validation gates.")
    ap.add_argument("--panel", type=Path, default=None,
                    help="run G6 only, against this panel instead of the built one")
    ap.add_argument("--write-baseline", action="store_true",
                    help="refreeze research/data/staleness_baseline.csv from the built panel")
    ap.add_argument("--no-stamp", action="store_true",
                    help="with --panel on the canonical panel: check only, leave g6_stamp.json alone (CI)")
    args = ap.parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    if args.write_baseline:
        n = write_staleness_baseline(pit.load_panel(_PANEL_PATH))
        logger.info("G6 baseline frozen: %d known stale pairs -> %s", n, _STALENESS_BASELINE_PATH)
        return 0
    if args.panel is not None:
        g6 = g6_all(pit.load_panel(args.panel))
        _report_g6(g6)
        if not args.no_stamp:
            _stamp_if_canonical(args.panel, g6)
        return 1 if g6["failures"] else 0
    res = run()
    return 1 if res["g6_failures"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
