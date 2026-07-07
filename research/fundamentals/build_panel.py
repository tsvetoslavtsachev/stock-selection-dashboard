"""
Build the point-in-time (PIT) fundamental panel from cached EDGAR companyfacts.

Reads every research/cache/edgar/CIK*.json, extracts the canonical concepts using
tag FALLBACK CHAINS (companies tag the same economic line differently; the chain
order is priority), and emits ONE row per raw fact filing. Every filing is kept —
we do NOT de-duplicate by period_end, because a later 10-K/A that restates a period
is a DIFFERENT row filed on a DIFFERENT date. The "as known at date X" question is
answered later (pit.py) by filtering on the ``filed`` column; the panel itself is
the immutable evidence.

Output
------
research/data/edgar_pit_panel.csv.gz with the Interface-P columns, exactly:

    ticker, cik, concept, unit, period_start, period_end, filed, form, fy, fp, value

* ``ticker`` is the dotted symbol from universe.csv (BRK.B, BF.B), not the CIK.
* ``period_start`` is empty for instantaneous (balance-sheet / shares) concepts.
* Units: only USD (and ``shares`` for share counts) are kept; a fact in a foreign
  presentation currency is skipped and counted.

One synthetic concept, ``total_debt``, is assembled by SUMMING LongTermDebtNoncurrent
and LongTermDebtCurrent at a matching (period_end, filed) key, falling back to the
single-tag chain LongTermDebt / DebtLongtermAndShorttermCombinedAmount when the split
pair is unavailable.

CLI
---
    python -m research.fundamentals.build_panel
    python -m research.fundamentals.build_panel --limit 20   # first N cache files
"""

from __future__ import annotations

import argparse
import csv
import gzip
import json
import logging
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

logger = logging.getLogger("build_panel")

_UNIVERSE_PATH = _REPO_ROOT / "config" / "universe.csv"
_CACHE_DIR = _REPO_ROOT / "research" / "cache" / "edgar"
_PANEL_PATH = _REPO_ROOT / "research" / "data" / "edgar_pit_panel.csv.gz"

PANEL_COLUMNS = [
    "ticker", "cik", "concept", "unit",
    "period_start", "period_end", "filed", "form", "fy", "fp", "value",
]

# ---------------------------------------------------------------------------
# Canonical concept -> (namespace, [tag fallback chain], expected_unit)
# The chain is tried in order; the FIRST tag that has any USD/shares facts for a
# given company supplies that concept's rows (we do not blend tags within a
# company — mixing SalesRevenueNet and Revenues for one issuer would create
# spurious period gaps).
# ---------------------------------------------------------------------------
_USD = "USD"
_SHARES = "shares"

# Simple single/fallback-chain concepts: (namespace, [tags], unit)
_CONCEPTS: dict[str, tuple[str, list[str], str]] = {
    # Revenue is spread across tags by BOTH accounting era (SalesRevenueNet ->
    # ASC 606 RevenueFromContractWithCustomer...) AND gross-vs-net-of-sales-tax
    # presentation. The IncludingAssessedTax variant is what many retailers /
    # utilities file (TJX, CPRT, ODFL, KHC, DUK...) and is the SAME total revenue
    # line, so it belongs in the chain. We deliberately do NOT add the
    # SalesRevenueGoodsNet / SalesRevenueServicesNet split tags: those are
    # sub-components of one period's revenue and unioning them would double-count
    # (verified: they collide with the total on the same period key with a different
    # value). Priority: the plain totals first, ...IncludingAssessedTax after the
    # Excluding variant so the earlier (net-of-tax) figure wins any same-key tie.
    "revenues": ("us-gaap", [
        "Revenues",
        "RevenueFromContractWithCustomerExcludingAssessedTax",
        "RevenueFromContractWithCustomerIncludingAssessedTax",
        "SalesRevenueNet",
    ], _USD),
    "gross_profit": ("us-gaap", ["GrossProfit"], _USD),
    "operating_income": ("us-gaap", ["OperatingIncomeLoss"], _USD),
    "net_income": ("us-gaap", ["NetIncomeLoss"], _USD),
    "stockholders_equity": ("us-gaap", [
        "StockholdersEquity",
        "StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest",
    ], _USD),
    "total_assets": ("us-gaap", ["Assets"], _USD),
    "current_liabilities": ("us-gaap", ["LiabilitiesCurrent"], _USD),
    "cash_and_equivalents": ("us-gaap", [
        "CashAndCashEquivalentsAtCarryingValue",
        "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalents",
    ], _USD),
    "depreciation_amortization": ("us-gaap", [
        "DepreciationDepletionAndAmortization",
        "DepreciationAndAmortization",
        "Depreciation",
    ], _USD),
    "operating_cash_flow": ("us-gaap", [
        "NetCashProvidedByUsedInOperatingActivities",
        "NetCashProvidedByUsedInOperatingActivitiesContinuingOperations",
    ], _USD),
    "capex": ("us-gaap", [
        "PaymentsToAcquirePropertyPlantAndEquipment",
        "PaymentsToAcquireProductiveAssets",
    ], _USD),
    "buybacks": ("us-gaap", ["PaymentsForRepurchaseOfCommonStock"], _USD),
    "dividends_paid": ("us-gaap", [
        "PaymentsOfDividendsCommonStock",
        "PaymentsOfDividends",
    ], _USD),
    "shares_outstanding": ("dei", [
        "EntityCommonStockSharesOutstanding",
    ], _SHARES),
}

# shares_outstanding fallback into us-gaap when dei is absent (handled explicitly).
_SHARES_GAAP_FALLBACK = ("us-gaap", "CommonStockSharesOutstanding")

# total_debt split pair (summed at matching period_end+filed) then fallback chain.
_DEBT_SPLIT = ("LongTermDebtNoncurrent", "LongTermDebtCurrent")
_DEBT_FALLBACK_TAGS = ["LongTermDebt", "DebtLongtermAndShorttermCombinedAmount"]


def load_universe(path: Path = _UNIVERSE_PATH) -> list[tuple[str, str]]:
    """Return [(ticker, cik10)] for enabled rows with a CIK."""
    out: list[tuple[str, str]] = []
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("enabled", "1").strip() not in ("1", ""):
                continue
            cik = (row.get("cik") or "").strip()
            if not cik:
                continue
            out.append((row["symbol"].strip(), str(int(cik)).zfill(10)))
    return out


def _facts_for_tag(facts: dict, namespace: str, tag: str, unit: str) -> list[dict]:
    """Return the unit-block list for one namespace/tag/unit, or [] if absent."""
    ns = facts.get(namespace, {})
    concept = ns.get(tag)
    if not concept:
        return []
    return concept.get("units", {}).get(unit, []) or []


def _first_populated_tag(facts: dict, namespace: str, tags: list[str], unit: str) -> tuple[str | None, list[dict]]:
    """Walk the fallback chain; return the first (tag, records) that has data."""
    for tag in tags:
        recs = _facts_for_tag(facts, namespace, tag, unit)
        if recs:
            return tag, recs
    return None, []


def _union_chain(facts: dict, namespace: str, tags: list[str], unit: str) -> tuple[list[str], list[dict]]:
    """UNION every tag in the fallback chain, de-duplicated by the fact identity
    (period_start, period_end, filed).

    Why union rather than "first populated tag wins": issuers migrate the tag for
    the SAME economic line across accounting-standard changes. AAPL's revenue, for
    instance, lives in SalesRevenueNet (FY2009-2018), then
    RevenueFromContractWithCustomerExcludingAssessedTax (FY2019+), with a handful of
    transitional ``Revenues`` facts — three disjoint eras of ONE series. Taking only
    the first populated tag would keep the 11 stray ``Revenues`` facts and drop 20
    years of history. Verified on AAPL: the three revenue tags share ZERO
    (start,end,filed) keys, so the union has no value conflicts.

    On the rare exact-key collision (two chain tags carrying the same period on the
    same filing), the EARLIER tag in the chain wins — preserving the priority the
    chain encodes. Returns (tags_used, records)."""
    seen: set[tuple] = set()
    out: list[dict] = []
    used: list[str] = []
    for tag in tags:
        recs = _facts_for_tag(facts, namespace, tag, unit)
        added = 0
        for r in recs:
            if "end" not in r or "filed" not in r or "val" not in r:
                continue
            key = (r.get("start", ""), r["end"], r["filed"])
            if key in seen:
                continue  # earlier tag already supplied this exact fact
            seen.add(key)
            out.append(r)
            added += 1
        if added:
            used.append(tag)
    return used, out


def _emit_rows(ticker: str, cik10: str, concept: str, unit: str, records: list[dict]) -> list[list]:
    """Turn raw fact records into panel rows. Instantaneous facts (no 'start')
    get an empty period_start. Records missing 'val'/'end'/'filed' are dropped."""
    rows: list[list] = []
    for r in records:
        if "val" not in r or "end" not in r or "filed" not in r:
            continue
        rows.append([
            ticker,
            str(int(cik10)),           # store CIK un-padded (numeric) in the panel
            concept,
            unit,
            r.get("start", ""),        # empty for instantaneous concepts
            r["end"],
            r["filed"],
            r.get("form", ""),
            r.get("fy", ""),
            r.get("fp", ""),
            r["val"],
        ])
    return rows


def _extract_total_debt(facts: dict) -> list[dict]:
    """Assemble synthetic total_debt records.

    Preference 1 — SUM the split pair (LongTermDebtNoncurrent + LongTermDebtCurrent)
    at a matching (end, filed, accn) key so we add only figures from the SAME
    filing snapshot (never mix a noncurrent value from one amendment with a current
    value from another). A period that has only ONE leg of the pair is skipped here
    and left to the fallback (a lone noncurrent figure is not total debt).

    Preference 2 — the single-tag fallback chain (LongTermDebt / combined), used
    only for (end, filed) keys the split pair did not already produce, so a company
    that reports both does not double-count.
    """
    noncur = _facts_for_tag(facts, "us-gaap", _DEBT_SPLIT[0], _USD)
    cur = _facts_for_tag(facts, "us-gaap", _DEBT_SPLIT[1], _USD)

    def _key(r):
        return (r.get("end"), r.get("filed"), r.get("accn"))

    cur_by_key = {}
    for r in cur:
        cur_by_key.setdefault(_key(r), r)

    out: list[dict] = []
    produced_end_filed: set[tuple] = set()
    for rn in noncur:
        k = _key(rn)
        rc = cur_by_key.get(k)
        if rc is None:
            continue
        merged = {
            "end": rn["end"],
            "filed": rn["filed"],
            "val": rn["val"] + rc["val"],
            "form": rn.get("form", ""),
            "fy": rn.get("fy", ""),
            "fp": rn.get("fp", ""),
            "accn": rn.get("accn", ""),
        }
        out.append(merged)
        produced_end_filed.add((rn["end"], rn["filed"]))

    # Fallback chain for (end, filed) keys the split pair did not cover.
    _tag, fb_recs = _first_populated_tag(facts, "us-gaap", _DEBT_FALLBACK_TAGS, _USD)
    for r in fb_recs:
        if (r.get("end"), r.get("filed")) in produced_end_filed:
            continue
        out.append(r)

    return out


def _extract_company(ticker: str, cik10: str, facts: dict, stats: dict) -> list[list]:
    """Extract all canonical concepts for one company -> panel rows."""
    rows: list[list] = []

    for concept, (namespace, tags, unit) in _CONCEPTS.items():
        # Union the chain (tag migration of the SAME line across accounting eras);
        # de-duplicated by (start,end,filed), earlier tag wins an exact collision.
        _tags_used, recs = _union_chain(facts, namespace, tags, unit)

        # shares_outstanding: fall back from dei to us-gaap:CommonStockSharesOutstanding.
        if concept == "shares_outstanding" and not recs:
            gaap_ns, gaap_tag = _SHARES_GAAP_FALLBACK
            recs = _facts_for_tag(facts, gaap_ns, gaap_tag, unit)

        if recs:
            rows.extend(_emit_rows(ticker, cik10, concept, unit, recs))
        else:
            stats["missing_concept"][concept] = stats["missing_concept"].get(concept, 0) + 1

    # Synthetic total_debt.
    debt_recs = _extract_total_debt(facts)
    if debt_recs:
        rows.extend(_emit_rows(ticker, cik10, "total_debt", _USD, debt_recs))
    else:
        stats["missing_concept"]["total_debt"] = stats["missing_concept"].get("total_debt", 0) + 1

    return rows


def _count_foreign(facts: dict, stats: dict) -> None:
    """Count facts presented in a non-USD/non-shares currency (skipped)."""
    for namespace in ("us-gaap", "dei"):
        for concept in facts.get(namespace, {}).values():
            for unit in concept.get("units", {}):
                if unit not in (_USD, _SHARES) and "/" not in unit:
                    # bare currency codes like EUR/GBP/JPY; skip per-share (USD/shares)
                    if unit.isalpha() and len(unit) == 3:
                        stats["foreign_unit_facts"] += len(concept["units"][unit])


def build(limit: int | None = None) -> dict:
    """Build the panel over all cached companyfacts. Returns a summary dict."""
    universe = load_universe()
    # A CIK can map to MULTIPLE tickers (dual-class share classes filed under one
    # entity: GOOG/GOOGL, FOX/FOXA, NWS/NWSA). Each class gets its own panel rows so
    # the downstream dashboard can score both symbols.
    cik_to_tickers: dict[str, list[str]] = {}
    for tk, cik in universe:
        cik_to_tickers.setdefault(cik, []).append(tk)

    cache_files = sorted(_CACHE_DIR.glob("CIK*.json"))
    if limit is not None:
        cache_files = cache_files[:limit]

    stats = {
        "companies_seen": 0,
        "companies_no_cache": 0,
        "rows": 0,
        "foreign_unit_facts": 0,
        "missing_concept": {},
    }

    _PANEL_PATH.parent.mkdir(parents=True, exist_ok=True)

    logger.info("Building panel from %d cache files -> %s", len(cache_files), _PANEL_PATH)

    with gzip.open(_PANEL_PATH, "wt", encoding="utf-8", newline="") as gz:
        w = csv.writer(gz)
        w.writerow(PANEL_COLUMNS)

        for i, path in enumerate(cache_files, start=1):
            # CIK from filename: CIK0000320193.json -> 0000320193
            cik10 = path.stem.replace("CIK", "")
            tickers = cik_to_tickers.get(cik10)
            if not tickers:
                # cache file for a CIK not in the (current) universe — skip.
                continue

            try:
                with open(path, encoding="utf-8") as f:
                    data = json.load(f)
            except (json.JSONDecodeError, OSError) as exc:
                logger.warning("Bad cache file %s: %s", path.name, exc)
                stats["companies_no_cache"] += 1
                continue

            facts = data.get("facts", {})
            _count_foreign(facts, stats)
            for ticker in tickers:
                rows = _extract_company(ticker, cik10, facts, stats)
                w.writerows(rows)
                stats["rows"] += len(rows)
                stats["companies_seen"] += 1

            if i % 50 == 0 or i == len(cache_files):
                logger.info("[%d/%d] rows so far=%d", i, len(cache_files), stats["rows"])

    logger.info("DONE: %d companies, %d rows, %d foreign-unit facts skipped",
                stats["companies_seen"], stats["rows"], stats["foreign_unit_facts"])
    # Concepts missing for the MOST companies (extraction health signal).
    worst = sorted(stats["missing_concept"].items(), key=lambda kv: -kv[1])[:5]
    if worst:
        logger.info("Concepts missing for most companies: %s", worst)
    return stats


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description="Build the EDGAR PIT panel from cached companyfacts.")
    p.add_argument("--limit", type=int, default=None, help="only the first N cache files")
    args = p.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )
    build(limit=args.limit)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
