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

One synthetic concept, ``total_debt``, is assembled by SUMMING a noncurrent+current
tag pair at a matching (period_end, filed) key (four pairs, in priority order), then
filling the keys no pair covered from the single-tag chain LongTermDebt /
DebtLongtermAndShorttermCombinedAmount (unioned, not first-tag-wins). Names that are
genuinely debt-free and tag no debt line at all (_DEBT_FREE) get an explicit 0.

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

# enabled=1 is the S&P 500 canon; enabled=2 is a GUEST row (a name outside the index that the
# organ must read anyway, LYFT from УБР3 29.09.2026): SEC facts are collected and paneled for it,
# but the daily pipeline (src/jobs, read_universe) and every consensus ingest read only ==1.
# Mirrored in collect_edgar.SEC_STATUSES; tests/test_universe_status.py pins both.
SEC_STATUSES = ("1", "2", "")

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
    # Extracted by _extract_net_income, not the generic union: ProfitLoss includes
    # the noncontrolling interest, so it may only stand in for NetIncomeLoss under
    # the rules documented there.
    "net_income": ("us-gaap", ["NetIncomeLoss", "ProfitLoss"], _USD),
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

# shares_outstanding — ONE economic quantity (common shares outstanding, point in
# time) tagged across TWO namespaces by era/filer: the dei cover-page tag and the
# us-gaap balance-sheet tag. We UNION both (dei priority on an exact key collision)
# so a name whose dei facts went stale — or dropped out because the SEC
# companyfacts API discards DIMENSIONAL facts and multi-class issuers tag their
# cover-page shares PER CLASS (StatementClassOfStockAxis) — can still be carried by
# a fresh us-gaap:CommonStockSharesOutstanding fact (recovers e.g. ARES, HSY).
# NB: dei facts stamp the cover-page "as of" date (later); us-gaap facts stamp the
# balance-sheet quarter-end (earlier). So for a name with a fresh dei fact, dei
# still wins the latest-period_end selection — the us-gaap tag only surfaces when
# dei is stale or absent. (Names whose recent counts exist ONLY dimensionally —
# Visa, Comcast, Nike, Meta — remain honestly uncovered here and are made
# honest-empty downstream by the valuation-organ М32 sentinel/staleness guards.)
_SHARES_TAGS = (
    ("dei", "EntityCommonStockSharesOutstanding"),
    ("us-gaap", "CommonStockSharesOutstanding"),
)

# net_income: NetIncomeLoss (attributable to the parent) first; ProfitLoss (incl. the
# noncontrolling interest) fills only the keys NetIncomeLoss does not carry. AVGO
# tags its whole income statement with ProfitLoss from FY2025 on (NetIncomeLoss
# stops at 2024-11-03), so without it the TTM ladder ran on a stale year and P/E
# read 577. But blind union is wrong where the NCI is material (IBKR: ProfitLoss is
# about 12x NetIncomeLoss; FCX about 1.75x), so a ProfitLoss fact is admitted only
#   (a) minus NetIncomeLossAttributableToNoncontrollingInterest at the same key, or
#   (b) as-is, when the issuer's own filings show the two tags agree within
#       _NCI_IMMATERIAL on its latest _NCI_EVIDENCE_N shared keys.
# Otherwise the key stays empty (honest gap, caught by the staleness gate).
_NI_TAG = "NetIncomeLoss"
_NI_CONSOLIDATED_TAG = "ProfitLoss"
_NCI_TAG = "NetIncomeLossAttributableToNoncontrollingInterest"
_NCI_IMMATERIAL = 0.02
_NCI_EVIDENCE_N = 8

# Sub-million sentinel floor for shares_outstanding. A cover-page share count
# below this is never a real issuer: it is a spin-off placeholder (FOXA = 1.0,
# 2019), a zero-class sentinel (CVNA/DDOG/HOOD/SPG/TAP = 0.0), or a cover-page
# count mis-scaled to THOUSANDS (CMG "27,962" @2022 = 27.96M; RMD "145,681" @2021
# = 145.68M). We reject them AT THE SOURCE so the immutable panel evidence itself
# is clean; the valuation-organ М32 <1e6 guard stays as belt-and-suspenders.
SHARES_SENTINEL_MIN = 1_000_000

# total_debt: noncurrent+current PAIRS (summed within one filing), priority order,
# then the single-tag chain for keys no pair covered. Issuers move between these
# presentations over time: ORCL went to LongTermNotesAndLoans + NotesPayableCurrent
# (its only LongTermDebt fact is a stray 0 at 2022-05-31), MU to
# LongTermDebtAndCapitalLeaseObligations + DebtCurrent (LongTermDebt stops
# 2025-11-27), SMCI to DebtLongtermAndShorttermCombinedAmount (LongTermDebt stops
# 2023-06-30). Same ladder as valuation-organ scripts/build_cash_table.DEBT_FALLBACK,
# except the combined tag stays LAST here so names already carried by a pair or by
# LongTermDebt keep their long-term-debt definition.
_DEBT_PAIRS = (
    ("LongTermDebtNoncurrent", "LongTermDebtCurrent"),
    ("LongTermNotesAndLoans", "NotesPayableCurrent"),
    ("LongTermDebtAndCapitalLeaseObligations", "DebtCurrent"),
    # its own current-portion partner: 53 names whose debt went stale had this
    # pair on their latest balance sheet (12.10.2026)
    ("LongTermDebtAndCapitalLeaseObligations", "LongTermDebtAndCapitalLeaseObligationsCurrent"),
)
_DEBT_FALLBACK_TAGS = ["LongTermDebt", "DebtLongtermAndShorttermCombinedAmount"]

# Names that are genuinely debt-free and therefore tag NO debt line: their
# total_debt is an explicit 0 at every balance-sheet key (the Assets facts), not a
# hole that reads like missing data. Each entry carries its evidence; a declared
# name that starts tagging nonzero debt is logged so the declaration gets revisited.
_DEBT_FREE: dict[str, str] = {
    "PLTR": "no borrowings tagged since LongTermDebtNoncurrent=0 at 2021-12-31; "
            "revolver undrawn (cache checked 12.10.2026)",
}


def load_universe(path: Path = _UNIVERSE_PATH) -> list[tuple[str, str]]:
    """Return [(ticker, cik10)] for rows with enabled in SEC_STATUSES (1 = S&P canon,
    2 = guest) and a CIK."""
    out: list[tuple[str, str]] = []
    with open(path, encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row.get("enabled", "1").strip() not in SEC_STATUSES:
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


def _extract_shares(facts: dict) -> tuple[list[dict], int]:
    """Common-shares-outstanding records, unioned across the dei cover-page tag and
    the us-gaap balance-sheet tag (dei priority on an exact start/end/filed
    collision), with sub-million sentinels rejected at the source.

    A dropped sentinel does NOT reserve its key, so a real value at the same key
    from the other tag still comes through (prefer a real count over a placeholder).
    Returns (records, n_sentinels_dropped)."""
    seen: set[tuple] = set()
    out: list[dict] = []
    dropped = 0
    for ns, tag in _SHARES_TAGS:
        for r in _facts_for_tag(facts, ns, tag, _SHARES):
            if "end" not in r or "filed" not in r or "val" not in r:
                continue
            key = (r.get("start", ""), r["end"], r["filed"])
            if key in seen:
                continue  # earlier tag (dei) already supplied this exact fact
            val = r["val"]
            if not isinstance(val, (int, float)) or isinstance(val, bool) or val < SHARES_SENTINEL_MIN:
                dropped += 1
                continue  # placeholder / zero-class / thousands-scaled — not a real count
            seen.add(key)
            out.append(r)
    return out, dropped


def _fact_key(r: dict) -> tuple:
    return (r.get("start", ""), r["end"], r["filed"])


def _valid(r: dict) -> bool:
    return "end" in r and "filed" in r and "val" in r


def _extract_net_income(facts: dict) -> tuple[list[dict], int]:
    """NetIncomeLoss records, with ProfitLoss filling the keys it does not carry
    under the NCI rules above (_NI_CONSOLIDATED_TAG comment block).
    Returns (records, n_profitloss_keys_refused)."""
    ni = [r for r in _facts_for_tag(facts, "us-gaap", _NI_TAG, _USD) if _valid(r)]
    out: list[dict] = []
    ni_by_key: dict[tuple, float] = {}
    for r in ni:
        k = _fact_key(r)
        if k in ni_by_key:
            continue
        ni_by_key[k] = r["val"]
        out.append(r)

    pl = [r for r in _facts_for_tag(facts, "us-gaap", _NI_CONSOLIDATED_TAG, _USD) if _valid(r)]
    if not pl:
        return out, 0
    nci = {_fact_key(r): r["val"]
           for r in _facts_for_tag(facts, "us-gaap", _NCI_TAG, _USD) if _valid(r)}

    shared = sorted((r for r in pl if ni_by_key.get(_fact_key(r))),
                    key=lambda r: (r["end"], r["filed"]))[-_NCI_EVIDENCE_N:]
    devs = sorted(abs(r["val"] / ni_by_key[_fact_key(r)] - 1) for r in shared)
    immaterial = bool(devs) and devs[len(devs) // 2] <= _NCI_IMMATERIAL

    refused = 0
    seen = set(ni_by_key)
    for r in pl:
        k = _fact_key(r)
        if k in seen:
            continue
        if k in nci:
            val = r["val"] - nci[k]
        elif immaterial:
            val = r["val"]
        else:
            refused += 1
            continue
        seen.add(k)
        out.append({**r, "val": val})
    return out, refused


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


def _sum_pair(facts: dict, pair: tuple[str, str]) -> list[dict]:
    """SUM one noncurrent+current pair at a matching (end, filed, accn) key, so we add
    only figures from the SAME filing snapshot (never a noncurrent value from one
    amendment with a current value from another). A key with only ONE leg is
    skipped (a lone noncurrent figure is not total debt)."""
    noncur = _facts_for_tag(facts, "us-gaap", pair[0], _USD)
    cur = _facts_for_tag(facts, "us-gaap", pair[1], _USD)

    def _key(r):
        return (r.get("end"), r.get("filed"), r.get("accn"))

    cur_by_key = {}
    for r in cur:
        cur_by_key.setdefault(_key(r), r)

    out: list[dict] = []
    for rn in noncur:
        if "end" not in rn or "filed" not in rn or "val" not in rn:
            continue
        rc = cur_by_key.get(_key(rn))
        if rc is None or "val" not in rc:
            continue
        out.append({
            "end": rn["end"],
            "filed": rn["filed"],
            "val": rn["val"] + rc["val"],
            "form": rn.get("form", ""),
            "fy": rn.get("fy", ""),
            "fp": rn.get("fp", ""),
            "accn": rn.get("accn", ""),
        })
    return out


def _extract_total_debt(facts: dict) -> list[dict]:
    """Assemble synthetic total_debt records.

    Preference 1 — the _DEBT_PAIRS in order, each summed within one filing
    (_sum_pair). A later pair only fills (end, filed) keys an earlier pair did not.

    Preference 2 — the single-tag chain (LongTermDebt, then the combined tag),
    UNIONED (not first-tag-wins) and used only for (end, filed) keys no pair
    produced, so a company that reports several presentations does not
    double-count, and one stray LongTermDebt fact (ORCL: a single 0 at 2022-05-31)
    can no longer shut the combined tag out for good.
    """
    out: list[dict] = []
    produced_end_filed: set[tuple] = set()
    for pair in _DEBT_PAIRS:
        for r in _sum_pair(facts, pair):
            k = (r["end"], r["filed"])
            if k in produced_end_filed:
                continue
            out.append(r)
            produced_end_filed.add(k)

    _tags, fb_recs = _union_chain(facts, "us-gaap", _DEBT_FALLBACK_TAGS, _USD)
    for r in fb_recs:
        if (r["end"], r["filed"]) in produced_end_filed:
            continue
        out.append(r)

    return out


def _debt_free_zeros(facts: dict, covered: set[tuple]) -> list[dict]:
    """Explicit total_debt = 0 for a declared debt-free name, one record per
    balance-sheet key (the Assets facts) that real debt facts do not cover."""
    out: list[dict] = []
    seen: set[tuple] = set(covered)
    for r in _facts_for_tag(facts, "us-gaap", "Assets", _USD):
        if "end" not in r or "filed" not in r:
            continue
        k = (r["end"], r["filed"])
        if k in seen:
            continue
        seen.add(k)
        out.append({"end": r["end"], "filed": r["filed"], "val": 0,
                    "form": r.get("form", ""), "fy": r.get("fy", ""), "fp": r.get("fp", "")})
    return out


def _extract_company(ticker: str, cik10: str, facts: dict, stats: dict) -> list[list]:
    """Extract all canonical concepts for one company -> panel rows."""
    rows: list[list] = []

    for concept, (namespace, tags, unit) in _CONCEPTS.items():
        if concept == "shares_outstanding":
            # dei ∪ us-gaap common-shares tags, sub-million sentinels dropped at
            # source (see _extract_shares). NOT the generic _union_chain — this one
            # spans two namespaces and screens the value.
            recs, n_drop = _extract_shares(facts)
            if n_drop:
                stats["shares_sentinels_dropped"] = stats.get("shares_sentinels_dropped", 0) + n_drop
                stats.setdefault("shares_sentinel_names", {})[ticker] = n_drop
        elif concept == "net_income":
            recs, n_refused = _extract_net_income(facts)
            if n_refused:
                stats["profitloss_refused"] = stats.get("profitloss_refused", 0) + n_refused
        else:
            # Union the chain (tag migration of the SAME line across accounting
            # eras); de-duplicated by (start,end,filed), earlier tag wins a tie.
            _tags_used, recs = _union_chain(facts, namespace, tags, unit)

        if recs:
            rows.extend(_emit_rows(ticker, cik10, concept, unit, recs))
        else:
            stats["missing_concept"][concept] = stats["missing_concept"].get(concept, 0) + 1

    # Synthetic total_debt.
    debt_recs = _extract_total_debt(facts)
    if ticker in _DEBT_FREE:
        nonzero = [r for r in debt_recs if r["val"]]
        if nonzero:
            last = max(nonzero, key=lambda r: r["end"])
            logger.warning("%s is declared debt-free but tags debt %s at %s; revisit _DEBT_FREE",
                           ticker, last["val"], last["end"])
        debt_recs = debt_recs + _debt_free_zeros(facts, {(r["end"], r["filed"]) for r in debt_recs})
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
        "shares_sentinels_dropped": 0,   # sub-1e6 shares facts rejected at source
        "shares_sentinel_names": {},     # ticker -> count of dropped sentinels
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
    if stats.get("profitloss_refused"):
        logger.info("net_income: %d ProfitLoss facts refused (material NCI, no NCI fact to subtract)",
                    stats["profitloss_refused"])
    if stats["shares_sentinels_dropped"]:
        logger.info(
            "shares_outstanding: %d sub-1e6 sentinel facts dropped at source across %d names: %s",
            stats["shares_sentinels_dropped"], len(stats["shares_sentinel_names"]),
            sorted(stats["shares_sentinel_names"].items()),
        )
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
