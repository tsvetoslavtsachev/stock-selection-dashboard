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

CLI
    python -m research.fundamentals.validate_gates
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

from research.fundamentals import pit  # noqa: E402

logger = logging.getLogger("validate_gates")

_UNIVERSE_PATH = _REPO_ROOT / "config" / "universe.csv"
_PANEL_PATH = _REPO_ROOT / "research" / "data" / "edgar_pit_panel.csv.gz"
_COVERAGE_PATH = _REPO_ROOT / "research" / "data" / "coverage_report.csv"
_RANKS_PATH = _REPO_ROOT / "data" / "processed" / "ranks.csv"

_UNIVERSE_N = 503
_RECENT_YEAR = 2024
_G2_THRESHOLD = 0.90
_G2_REQUIRED = ["net_income", "stockholders_equity", "total_assets", "revenues"]
_MEGACAPS = ["AAPL", "MSFT", "JPM", "XOM", "JNJ"]

_ALL_CONCEPTS = sorted(pit.STOCK_CONCEPTS | pit.FLOW_CONCEPTS)


def _universe_size() -> int:
    with open(_UNIVERSE_PATH, encoding="utf-8") as f:
        return sum(1 for r in csv.DictReader(f)
                   if (r.get("enabled", "1").strip() in ("1", "")) and (r.get("cik") or "").strip())


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
# Runner
# ---------------------------------------------------------------------------
def run() -> dict:
    if not _PANEL_PATH.exists():
        raise SystemExit(f"panel not found: {_PANEL_PATH} (run build_panel first)")

    logger.info("Loading panel %s ...", _PANEL_PATH)
    panel = pit.load_panel(_PANEL_PATH)
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
    }


def main(argv: list[str] | None = None) -> int:
    argparse.ArgumentParser(description="Run G2/G3 validation gates.").parse_args(argv)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
