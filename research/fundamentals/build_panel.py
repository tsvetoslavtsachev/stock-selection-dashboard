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

One synthetic concept, ``total_debt``, is the FINANCIAL debt (ПНЛ2): long-term debt
with its current portion + short-term borrowings + commercial paper + finance leases,
assembled within ONE filing snapshot from the filer's balance-sheet face lines, or by
the value rules when the face is unusable (research/fundamentals/debt.py). Segment-
split filers are read from their filing instances (debt.FILING_SPECS: the industrial
part of F, GM, CAT ...); banks, broker-dealers and insurers get none (debt.DEBT_NA);
names that are genuinely debt-free (_DEBT_FREE) get an explicit 0.

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
from datetime import date
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from research.fundamentals import debt as _debt  # noqa: E402
from research.fundamentals import collect_own_tags as _own  # noqa: E402

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
    # The FULL line only (ПНЛ4б, 13.10.2026). `Depreciation` is the PP&E part alone and
    # never joins this union: 200 names tag it beside the full line, and the union then
    # took its discrete quarter over the full line's year-to-date one, or differenced a
    # full-line FY against a Depreciation 9M (36 names moved, BAX EBITDA -43%; AMD, ELV
    # negative TTM). A filer that stopped tagging the full line continues only through a
    # proven substitute (_FALLBACK_TAGS, _FALLBACK_SUMS); otherwise an honest gap.
    "depreciation_amortization": ("us-gaap", [
        "DepreciationDepletionAndAmortization",
        "DepreciationAndAmortization",
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

# total_debt = the FINANCIAL debt (ПНЛ2, 13.10.2026): long-term debt with its current
# portion + short-term borrowings + commercial paper + finance leases, at carrying
# value. The definition, its nesting rules and its tables (face lines, filing route,
# not-applicable names) live in research/fundamentals/debt.py.
_DEBT_CACHE_DIR = _REPO_ROOT / "research" / "cache" / "edgar_debt"

# Names that are genuinely debt-free and therefore tag NO debt line: their
# total_debt is an explicit 0 at every balance-sheet key (the Assets facts), not a
# hole that reads like missing data. Each entry carries its evidence: the latest
# balance sheet's face (debt_face.csv: the presentation linkbase of that filing)
# shows no debt line, that filing carries no nonzero debt fact, and the last nonzero
# debt fact is old. A declared name that tags nonzero debt is logged here and FAILS
# validate_gates G6d, so the declaration gets revisited.
_DEBT_FREE: dict[str, str] = {
    "PLTR": "no borrowings tagged since LongTermDebtNoncurrent=0 at 2021-12-31; "
            "revolver undrawn (cache checked 12.10.2026)",
    # ПНЛ2, 13.10.2026: <accn of the latest balance sheet> (filed): its face shows no
    # debt line, that filing carries no nonzero debt-line tag; the last nonzero one ever
    "ALGN": "0001097149-26-000060 (2026-08-05): балансът без ред за дълг, 0 ненулеви дългови тага; последен ненулев: FinanceLeaseLiability 56 млн (2019-03-31), усвоено 0 (2024-12-31)",
    "ANET": "0001596532-26-000175 (2026-08-05): балансът без ред за дълг, 0 ненулеви дългови тага; последен ненулев: FinanceLeaseLiability 38 млн (2018-12-31)",
    "CMG": "0001058090-26-000066 (2026-07-31): балансът без ред за дълг, 0 ненулеви дългови тага; последен ненулев: FinanceLeaseLiability 2 млн (2021-12-31)",
    "DECK": "0000910521-26-000022 (2026-07-30): балансът без ред за дълг, 0 ненулеви дългови тага; последен ненулев: NotesPayable 30 млн (2020-12-31)",
    "EXPD": "0001193125-26-334457 (2026-08-05): балансът без ред за дълг, 0 ненулеви дългови тага; последен ненулев: ShortTermBankLoansAndNotesPayable 30 млн (2025-12-31)",
    "FFIV": "0001048695-26-000067 (2026-08-06): балансът без ред за дълг, 0 ненулеви дългови тага; последен ненулев: LongTermDebtCurrent 350 млн (2022-09-30)",
    "GRMN": "0001193125-26-188908 (2026-04-29): балансът без ред за дълг, 0 ненулеви дългови тага; последен ненулев: никога",
    "ISRG": "0001035267-26-000058 (2026-07-21): балансът без ред за дълг, 0 ненулеви дългови тага; последен ненулев: никога",
    "MNST": "0001104659-26-092193 (2026-08-07): балансът без ред за дълг, 0 ненулеви дългови тага; последен ненулев: FinanceLeaseLiability 8 млн (2025-03-31)",
    "MPWR": "0001628280-26-053275 (2026-08-05): балансът без ред за дълг, 0 ненулеви дългови тага; последен ненулев: OtherLongTermDebtNoncurrent 1 млн (2013-09-30)",
    "TTD": "0001671933-26-000086 (2026-08-06): балансът без ред за дълг, 0 ненулеви дългови тага; последен ненулев: LongTermLineOfCredit 72 млн (2020-09-30)",
    "VEEV": "0001393052-26-000036 (2026-08-27): балансът без ред за дълг, 0 ненулеви дългови тага; последен ненулев: FinanceLeaseLiability 1 млн (2020-10-31)",
    "WSM": "0000719955-26-000208 (2026-08-28): балансът без ред за дълг, 0 ненулеви дългови тага; последен ненулев: LongTermDebtCurrent 299 млн (2021-01-31)",
    "ERIE": "0001628280-26-051079 (2026-07-30): балансът без ред за дълг, 0 ненулеви дългови тага; последен ненулев: ShortTermBorrowings 40 млн (2022-06-30)",
    "LULU": "0001397187-26-000127 (2026-09-03): балансът без ред за дълг, 0 ненулеви дългови тага; последен ненулев: никога",
    "TROW": "0001628280-26-051335 (2026-07-31): балансът без ред за дълг, 0 ненулеви дългови тага; последен ненулев: DebtInstrumentFairValue 168 млн (2011-03-31)",
}

# Proven per-filer fallbacks (ПНЛ4, 13.10.2026). These tags are NOT the same line for
# every filer, so they cannot join the global chains: across the names tagging both,
# PaymentsOfOrdinaryDividends equals PaymentsOfDividendsCommonStock on every shared
# period at 19 of 29 names, PaymentsForRepurchaseOfEquity equals ...OfCommonStock at
# 19 of 31, DepreciationAmortizationAndAccretionNet equals
# DepreciationDepletionAndAmortization at 12 of 37. So a fallback tag continues ONE
# filer's chain, and only when that filer's own numbers prove it is the same line:
#   (a) overlap: on the latest _FALLBACK_EVIDENCE_N periods (start, end) it shares with
#       the chain (at least _FALLBACK_MIN_SHARED of them) every value agrees within
#       _FALLBACK_TOL (AON buybacks, ACN dividends, WMT D&A ...); or, only when the
#       chain has no fact at all,
#   (b) identity (_IDENTITY_TAGS): net income = basic EPS x weighted basic shares on the
#       latest _FALLBACK_EVIDENCE_N periods, dividends paid = dividend per share x
#       weighted basic shares on the latest _IDENTITY_YEARS fiscal years, each within
#       _IDENTITY_TOL (ITW tags only ProfitLoss; JNJ and PFE only
#       PaymentsOfOrdinaryDividends).
# A proven tag only CONTINUES the series (facts ending after the chain's last
# period_end): a name whose chain is fresh gets nothing from it, no existing row changes.
_FALLBACK_TAGS: dict[str, list[str]] = {
    "buybacks": ["PaymentsForRepurchaseOfEquity"],
    # ПНЛ4в (13.10.2026, Р1 на Ц.): the filer's own capex line under another us-gaap tag,
    # read in its latest filing (HOOD, ROP; MTB; KKR), admitted only where proven
    "capex": ["PaymentsForCapitalImprovements", "PaymentsToAcquireOtherPropertyPlantAndEquipment",
              "PaymentsToAcquireOtherProductiveAssets", "PaymentsForProceedsFromProductiveAssets",
              "PaymentsToAcquireFurnitureAndFixtures"],
    "cash_and_equivalents": [
        "CashCashEquivalentsRestrictedCashAndRestrictedCashEquivalentsIncludingDisposalGroupAndDiscontinuedOperations",
        "Cash",
    ],
    "depreciation_amortization": [
        "DepreciationAmortizationAndAccretionNet",
        "CostOfGoodsAndServicesSoldDepreciationAndAmortization",
        # ПНЛ4в (Р1): the cash-flow „Depreciation and amortization“ line moved to this
        # tag (AMD, MRVL, WDAY; equal to the old DDA on every shared period)
        "OtherDepreciationAndAmortization",
    ],
    "dividends_paid": ["PaymentsOfOrdinaryDividends"],
    # after preferred dividends: the basic-EPS numerator (equal to NetIncomeLoss for
    # a filer without preferred stock; BKNG tags its 2026 quarters only with it)
    "net_income": ["NetIncomeLossAvailableToCommonStockholdersBasic"],
}
# Proven per-filer SUMS (ПНЛ4б, 13.10.2026): (base tag, part tags) added from ONE
# filing (same accn, start, end; the first part tag that filing carries for the
# period). Admitted after _FALLBACK_TAGS by the same overlap proof. Depreciation +
# intangible amortization equals the full D&A line at 8 of the 31 names where both
# can be compared and differs at 23 (the full line also carries software, contract
# cost and right-of-use amortization), so it is never a global definition. Unlike a
# fallback tag, a proven sum also fills the periods the series lacks after its last
# sub-annual period (a full line gone annual-only), never a period the series has.
_FALLBACK_SUMS: dict[str, list[tuple[str, tuple[str, ...]]]] = {
    "depreciation_amortization": [("Depreciation", (
        "AmortizationOfIntangibleAssets",
        "FiniteLivedIntangibleAssetsAmortizationExpense",
        "AmortizationOfAcquiredIntangibleAssets",
    ))],
}
# Names whose D&A tags are not operating D&A: no rows, the gap is explained in G6.
_DA_NA: dict[str, str] = {
    "WRB": "застраховател: DepreciationAndAmortization и DepreciationAmortizationAndAccretionNet са нетното натрупване по инвестициите (2025 FY -48,1 млн), не оперативна амортизация (ПНЛ4б, Р2)",
    "PRU": "застраховател: DepreciationDepletionAndAmortization носи нетната амортизация на премиите и дисконтите по инвестициите (FY2024 383 млн, FY2025 128, 2026 Q1 143, H1 33, т.е. Q2 -110), не оперативна амортизация (ПНЛ4б, Ц. 13.10.2026)",
}
# The D&A tag of the CASH-FLOW STATEMENT, BY NAME (ПНЛ4д, Р1 V5 на Ц., 14.10.2026). These
# filers tag DepreciationDepletionAndAmortization AND DepreciationAndAmortization in one
# filing: one is the cash-flow line, the other a note (PP&E, segments, income statement).
# The union kept DDA on a shared key and the note's facts on every other key, e.g. a
# segment table's discrete quarter over the cash-flow YTD (AMGN D&A TTM 2 702 vs 4 667 mln,
# UDR 347 vs 676). In a filing carrying both tags only the cash-flow one stays. Read from
# each filing's presentation linkbase (1 623 filings, 0 with both tags in the cash flow).
# Not here: PNC, COF (the cash-flow line is net of accretion, Р2), DVA, KEY, MCK, WTW (own
# cash-flow tag). BEN moved the line from DandA to DDA with the 10-K filed 11.2020.
_DA_CF_DDA = frozenset({
    "ALGN", "AMGN", "ARE", "ATO", "BA", "BAX", "BXP", "CPT", "CTSH", "D", "DUK", "DXCM", "ECHO",
    "EFX", "EIX", "EXE", "FICO", "GPC", "HD", "HSIC", "HWM", "KIM", "LOW", "MAA", "MSCI", "NCLH",
    "PCAR", "PEP", "PLD", "PYPL", "SBUX", "SWK", "TGT", "TPR", "TTD", "UDR", "UHS", "URI", "VRSK",
    "WBD", "WELL", "WMB", "YUM",
})
_DA_CF_DANDA = frozenset({
    "ADSK", "AES", "AMZN", "CRM", "DDOG", "EBAY", "FDXF", "FIS", "GWW", "IEX", "L", "LHX", "MCD",
    "MCO", "NDAQ", "NTAP", "TSLA", "UPS",
})
_DA_CF_SWITCH: dict[str, tuple[str, str, str]] = {
    "BEN": ("2020-06-01", "DepreciationAndAmortization", "DepreciationDepletionAndAmortization"),
}
# Own (extension) tags, BY NAME (ПНЛ4г, 13.10.2026): own_tags.csv lists the filers whose
# us-gaap line stopped because they moved it to their own tag; collect_own_tags reads
# those facts from the filings' instances (companyfacts drops them). Admitted after the
# fallbacks by the same overlap proof against the filer's own series, only CONTINUING it
# (facts ending after its last period_end). Never global: a tag outside the table, or one
# whose proof fails (AMT, MAR, TPL, URI), adds nothing. Two refinements (Р3 на Ц.,
# 13.10.2026): a row may SUBTRACT us-gaap facts of the same filing (ACN: the cash-flow
# line minus OperatingLeaseCost, the lease rent it carries since ASC 842), and may run
# its proof against a named us-gaap tag instead of the series (TSLA: the chain is the
# PP&E note, the cash-flow line is DepreciationAndAmortization up to Q1 2018).
_OWN_CACHE_DIR = _own.CACHE_DIR
_IDENTITY_TAGS: dict[str, list[str]] = {
    "net_income": ["NetIncomeLossAvailableToCommonStockholdersBasic", _NI_CONSOLIDATED_TAG],
    "dividends_paid": ["PaymentsOfOrdinaryDividends"],
}
_FALLBACK_TOL = 0.005
_FALLBACK_MIN_SHARED = 2
_FALLBACK_EVIDENCE_N = 8
_IDENTITY_TOL = 0.02
_IDENTITY_YEARS = 4
_EPS_TAG = "EarningsPerShareBasic"
_DPS_TAGS = ("CommonStockDividendsPerShareCashPaid", "CommonStockDividendsPerShareDeclared")
_WASO_TAG = "WeightedAverageNumberOfSharesOutstandingBasic"


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


# Proxy statements are not financial statements. Their pay-versus-performance table
# tags NetIncomeLoss with whatever the issuer calls net income there (TKO: 546M for
# 2025 = its consolidated ProfitLoss, against 195M in the 10-K), sometimes in another
# scale; 177 of the 876 proxy facts with a 10-K twin differ by more than 0.5%
# (ПНЛ4, 13.10.2026). They carry no fiscal-year tag, which kept them out of the TTM
# ladder only by accident, and a later filing date would let them win any
# latest-filed read. So they never enter the panel.
_PROXY_FORMS = frozenset({"DEF 14A", "DEFA14A", "DEFR14A", "PRE 14A", "PRER14A", "PREC14A", "DEFC14A"})


def _facts_for_tag(facts: dict, namespace: str, tag: str, unit: str) -> list[dict]:
    """Return the unit-block list for one namespace/tag/unit (proxy-statement facts
    dropped, see _PROXY_FORMS), or [] if absent."""
    ns = facts.get(namespace, {})
    concept = ns.get(tag)
    if not concept:
        return []
    return [r for r in concept.get("units", {}).get(unit, []) or []
            if r.get("form") not in _PROXY_FORMS]


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


def _da_cash_flow_tag(ticker: str, filed: str) -> str | None:
    """The cash-flow statement's D&A tag for this filer's filing, or None (not in the table)."""
    if ticker in _DA_CF_SWITCH:
        since, before, after = _DA_CF_SWITCH[ticker]
        return after if filed >= since else before
    if ticker in _DA_CF_DDA:
        return "DepreciationDepletionAndAmortization"
    if ticker in _DA_CF_DANDA:
        return "DepreciationAndAmortization"
    return None


def _da_cash_flow_facts(ticker: str, facts: dict, unit: str = _USD) -> tuple[dict, int]:
    """facts with the NOTE tag dropped from every filing that also carries the cash-flow
    tag (_DA_CF_*), and from any key the cash-flow tag has. Returns (facts, n_dropped)."""
    dda, da = "DepreciationDepletionAndAmortization", "DepreciationAndAmortization"
    recs = {t: _facts_for_tag(facts, "us-gaap", t, unit) for t in (dda, da)}
    both = {r.get("accn") for r in recs[dda]} & {r.get("accn") for r in recs[da]}
    keys = {t: {(r.get("start", ""), r["end"], r["filed"]) for r in recs[t] if "end" in r and "filed" in r}
            for t in (dda, da)}
    gaap = dict(facts.get("us-gaap", {}))
    dropped = 0
    for tag, other in ((dda, da), (da, dda)):
        if tag not in gaap:
            continue
        keep = [r for r in recs[tag]
                if not ("end" in r and "filed" in r
                        and _da_cash_flow_tag(ticker, r["filed"]) == other
                        and (r.get("accn") in both or (r.get("start", ""), r["end"], r["filed"]) in keys[other]))]
        dropped += len(recs[tag]) - len(keep)
        units = dict(gaap[tag].get("units", {}))
        units[unit] = keep
        gaap[tag] = {**gaap[tag], "units": units}
    return {**facts, "us-gaap": gaap}, dropped


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


# ---------------------------------------------------------------------------
# Per-class share counts (ПНЛ1, 12.10.2026)
# For the multi-class issuers whose cover-page count exists ONLY per class (the
# companyfacts API drops it, see the _SHARES_TAGS comment), the count is read from
# the filing's own XBRL instance (collect_edgar_classes.py -> research/cache/
# edgar_classes/) and summed into ONE economic count in units of the class the
# ticker trades in, by the coefficient table share_classes.csv. Every coefficient
# there carries its source; it is data with evidence, not a literal here.
#   coef = a number   -> that many unit-class shares per share of the member
#                        (BRK class A = 1500 B; ERIE class B = 2400 A; a class with
#                        no claim on the parent's earnings, e.g. the Up-C voting
#                        class B of CVNA/TKO, = 0)
#   coef = eps_ratio  -> read per filing as the member's diluted EPS over the unit
#                        class's (Visa B-1/B-2/B-3/C, whose conversion rates move
#                        every quarter with the litigation escrow)
# The coefficient is the CONVERSION rate (what the unit class's diluted EPS counts),
# not the share of undistributed earnings: an ERIE class B share takes 150x the
# class A earnings under the two-class method but converts into 2,400 A, and the
# class A diluted EPS divides by the converted count.
# GUARD (the company's own denominator): ref = NetIncomeLoss / diluted EPS of the
# unit class (basic when no diluted is tagged, as at BRK) over the longest period
# ending at the filing's period end. An economic count outside CLASS_GUARD_BAND of
# ref is logged (stats class_guard_breach) and KEPT: measured on the 36 names, most
# breaches are the guard's own blind spots (a diluted EPS that exchanges Up-C units
# with the NCI added back, ProfitLoss carrying LP units, a fiscal-year average after
# heavy buybacks), and the point-in-time cover count is the better number. Only a
# ticker declared role=weighted in the table (with its evidence) carries the
# company's weighted count instead (the exact diluted, else basic, weighted count
# when it agrees with ref within CLASS_FALLBACK_AGREE, else ref): ACN, whose cover
# count includes the Class A shares the registrant itself holds. With
# |EPS| < CLASS_GUARD_MIN_EPS or a loss the ref is too coarse and the record goes
# through unguarded (counted).
_CLASS_TABLE_PATH = Path(__file__).resolve().parent / "share_classes.csv"
_CLASS_CACHE_DIR = _REPO_ROOT / "research" / "cache" / "edgar_classes"
CLASS_GUARD_BAND = (0.90, 1.07)
CLASS_GUARD_MIN_EPS = 0.50
CLASS_FALLBACK_AGREE = 0.02
EPS_RATIO = "eps_ratio"


def load_class_table(path: Path = _CLASS_TABLE_PATH) -> dict[str, dict]:
    """ticker -> {"coef": {member: float | EPS_RATIO}, "unit": member, "eps": member|None,
    "weighted": bool}. Rows: role=unit (the traded class, coef 1), role=class (another
    class and its coefficient), role=eps (the member carrying the unit class's EPS,
    when it is not the undimensioned fact or the unit member itself — BRK tags EPS on
    brka:EquivalentClassBMember), role=weighted (on a guard breach take the
    company's weighted count; member and coef empty)."""
    out: dict[str, dict] = {}
    if not path.exists():
        return out
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            t = out.setdefault(r["ticker"], {"coef": {}, "unit": None, "eps": None, "weighted": False})
            role, member = r["role"].strip(), r["member"].strip()
            if role == "weighted":
                t["weighted"] = True
                continue
            if role == "eps":
                t["eps"] = member
                continue
            coef = r["coef"].strip()
            t["coef"][member] = EPS_RATIO if coef == EPS_RATIO else float(coef)
            if role == "unit":
                t["unit"] = member
    return out


def _period_pair(entry: dict, kind: str, member: str, end: str) -> dict[tuple, float]:
    return {(s, e): v for m, s, e, v in entry.get(kind, []) if m == member and e == end}


def _guard_ni(entry: dict) -> dict[tuple, float]:
    """{(start, end): parent net income} from one filing: NetIncomeLoss, else
    ProfitLoss minus the NCI share at the same period (MA, IBKR, SPG, FOX, EL and
    ABNB tag only ProfitLoss)."""
    ni = {(s, e): v for s, e, v in entry.get("ni", [])}
    nci = {(s, e): v for s, e, v in entry.get("nci", [])}
    for s, e, v in entry.get("pl", []):
        ni.setdefault((s, e), v - nci.get((s, e), 0.0))
    return ni


def _longest_eps(entry: dict, members: list[str], end: str) -> tuple | None:
    """(period, EPS, member, kind) — diluted EPS first, then basic — over the longest
    period ending at `end` that also has a parent net income (_guard_ni)."""
    ni = {k for k in _guard_ni(entry) if k[1] == end}
    for kind in ("eps", "epsb"):
        for m in members:
            eps = _period_pair(entry, kind, m, end)
            common = sorted(set(eps) & ni)  # earliest start first = longest period
            if common:
                return common[0], eps[common[0]], m, kind
    return None


def _class_records(ticker: str, spec: dict, cache: dict, stats: dict) -> list[dict]:
    """Economic share-count records (unit-class units) from the per-class cache."""
    out: list[dict] = []
    unit = spec["unit"]
    eps_members = [m for m in (spec["eps"], "", unit) if m is not None]
    for accn, e in sorted(cache.items(), key=lambda kv: kv[1]["filed"]):
        if not e.get("instance") or not e.get("ecso"):
            continue
        # dei:DocumentPeriodEndDate can sit a few days off the contexts' end
        # (52/53-week years); then the latest net-income end is the period end.
        ni_ends = {k[1] for k in _guard_ni(e)}
        end = e.get("period", "")
        if end not in ni_ends and ni_ends:
            end = max(ni_ends)
        by_instant: dict[str, list] = {}
        for m, inst, v in e["ecso"]:
            by_instant.setdefault(inst, []).append((m, v))
        got = _longest_eps(e, eps_members, end)
        ni = _guard_ni(e)
        for inst, classes in sorted(by_instant.items()):
            econ, ok = 0.0, True
            for m, v in classes:
                if m not in spec["coef"]:
                    if v:
                        stats.setdefault("class_unknown_member", set()).add((ticker, m))
                        ok = False
                    continue
                c = spec["coef"][m]
                if c == EPS_RATIO:
                    if not v:
                        continue
                    mine = _period_pair(e, "eps", m, end) or _period_pair(e, "epsb", m, end)
                    base = _period_pair(e, "eps", unit, end) or _period_pair(e, "epsb", unit, end)
                    k = sorted(set(mine) & set(base))
                    if not k or not base[k[0]]:
                        stats.setdefault("class_ratio_missing", []).append((ticker, accn, m))
                        ok = False
                        continue
                    c = mine[k[0]] / base[k[0]]
                econ += c * v
            if not ok or econ < SHARES_SENTINEL_MIN:
                continue
            val = econ
            if got is not None:
                per, eps, m_eps, kind = got
                n = ni.get(per)
                if n and n > 0 and eps >= CLASS_GUARD_MIN_EPS:
                    ref = n / eps
                    if not (CLASS_GUARD_BAND[0] <= econ / ref <= CLASS_GUARD_BAND[1]):
                        if spec.get("weighted"):
                            val = ref
                            for wk in ("wdil", "wbas"):
                                w = _period_pair(e, wk, m_eps, end).get(per)
                                if w and abs(w / ref - 1) <= CLASS_FALLBACK_AGREE:
                                    val = w
                                    break
                            stats.setdefault("class_fallback", []).append(
                                (ticker, e["filed"], round(econ), round(ref), round(val)))
                        else:
                            stats.setdefault("class_guard_breach", []).append(
                                (ticker, e["filed"], round(econ), round(ref)))
                else:
                    stats["class_unguarded"] = stats.get("class_unguarded", 0) + 1
            else:
                stats["class_unguarded"] = stats.get("class_unguarded", 0) + 1
            if val < SHARES_SENTINEL_MIN:
                continue
            out.append({"end": inst, "filed": e["filed"], "val": round(val),
                        "form": e["form"], "fy": e.get("fy", ""), "fp": e.get("fp", "")})
    return out


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


def _period_values(records: list[dict]) -> dict[tuple, float]:
    """(start, end) -> value from the latest filing that carries that period."""
    out: dict[tuple, float] = {}
    for r in sorted(records, key=lambda r: r["filed"]):
        out[(r.get("start", ""), r["end"])] = r["val"]
    return out


def _agrees(expected: float, val: float, tol: float) -> bool:
    return val == expected or (expected != 0 and abs(val / expected - 1) <= tol)


def _overlap_proven(chain: list[dict], cand: list[dict]) -> bool:
    ch, cd = _period_values(chain), _period_values(cand)
    shared = sorted((k for k in cd if k in ch), key=lambda k: (k[1], k[0]))[-_FALLBACK_EVIDENCE_N:]
    return (len(shared) >= _FALLBACK_MIN_SHARED
            and all(_agrees(ch[k], cd[k], _FALLBACK_TOL) for k in shared))


def _identity_proven(facts: dict, concept: str, cand: list[dict]) -> bool:
    """cand == per-share amount x weighted basic shares (see _FALLBACK_TAGS)."""
    shares = _period_values([r for r in _facts_for_tag(facts, "us-gaap", _WASO_TAG, _SHARES) if _valid(r)])
    if concept == "net_income":
        per_share = _period_values([r for r in _facts_for_tag(facts, "us-gaap", _EPS_TAG, "USD/shares") if _valid(r)])
        n, annual = _FALLBACK_EVIDENCE_N, False
    else:
        per_share = {}
        for tag in _DPS_TAGS:   # cash paid first, declared fills its gaps
            for k, v in _period_values([r for r in _facts_for_tag(facts, "us-gaap", tag, "USD/shares")
                                        if _valid(r)]).items():
                per_share.setdefault(k, v)
        n, annual = _IDENTITY_YEARS, True
    vals = _period_values(cand)
    keys = [k for k in vals if per_share.get(k) and shares.get(k)
            and (not annual or (k[0] and (date.fromisoformat(k[1]) - date.fromisoformat(k[0])).days >= 350))]
    keys = sorted(keys, key=lambda k: (k[1], k[0]))[-n:]
    return (len(keys) >= _FALLBACK_MIN_SHARED
            and all(_agrees(per_share[k] * shares[k], vals[k], _IDENTITY_TOL) for k in keys))


def _is_annual(r: dict) -> bool:
    return bool(r.get("start")) and (date.fromisoformat(r["end"]) - date.fromisoformat(r["start"])).days >= 350


def _sum_records(facts: dict, base: str, parts: tuple[str, ...]) -> list[dict]:
    """base + the first part tag carried by the SAME filing for the same period
    (_FALLBACK_SUMS); a base fact without a part in its own filing is dropped."""
    part: dict[tuple, dict] = {}
    for tag in parts:
        for r in _facts_for_tag(facts, "us-gaap", tag, _USD):
            if _valid(r):
                part.setdefault((r.get("accn"), r.get("start", ""), r["end"]), r)
    out = []
    for r in _facts_for_tag(facts, "us-gaap", base, _USD):
        p = part.get((r.get("accn"), r.get("start", ""), r["end"])) if _valid(r) else None
        if p is not None:
            out.append(dict(r, val=r["val"] + p["val"]))
    return out


def _continue_with_fallbacks(facts: dict, concept: str, chain: list[dict]) -> tuple[list[dict], list[str]]:
    """Append the proven fallback tags' (then sums') facts that end after the series'
    last period_end; a proven sum also fills an annual-only stretch (_FALLBACK_TAGS,
    _FALLBACK_SUMS). Returns (records, tags_admitted)."""
    tags = list(_FALLBACK_TAGS.get(concept, []))
    if not chain:
        tags += [t for t in _IDENTITY_TAGS.get(concept, []) if t not in tags]
    sums = {f"{b}+{'|'.join(p)}": (b, p) for b, p in _FALLBACK_SUMS.get(concept, [])} if chain else {}
    out, admitted = list(chain), []
    for tag in tags + list(sums):
        if tag in sums:
            cand = _sum_records(facts, *sums[tag])
        else:
            cand = [r for r in _facts_for_tag(facts, "us-gaap", tag, _USD) if _valid(r)]
        if not cand:
            continue
        if chain:
            proven = _overlap_proven(chain, cand)
        else:
            proven = tag in _IDENTITY_TAGS.get(concept, []) and _identity_proven(facts, concept, cand)
        if not proven:
            continue
        last = max((r["end"] for r in out), default="")
        seen = {_fact_key(r) for r in out}
        # a proven SUM also fills the periods the series lacks once it went annual-only
        # (SPGI, ROP, FAST, SNA: the full line is tagged only for the fiscal year, the
        # sum brings the quarters and the 9M level Q4 needs): after the series' last
        # sub-annual period, never a period the series has (Ц., ПНЛ4б 13.10.2026)
        if tag in sums:
            periods = {(r.get("start", ""), r["end"]) for r in out}
            sub_annual = max((r["end"] for r in out if not _is_annual(r)), default="")
        new = [r for r in cand if _fact_key(r) not in seen
               and (r["end"] > last or (tag in sums and r["end"] > sub_annual
                                        and (r.get("start", ""), r["end"]) not in periods))]
        if new:
            out.extend(new)
            admitted.append(tag)
    return out, admitted


def _own_candidate(facts: dict, own_cache: dict, spec: "_own.OwnSpec") -> list[dict]:
    """The own tags' records (summed from one filing) minus the spec's us-gaap tags
    from the SAME filing and period; a record without every part is dropped."""
    recs = _own.own_records(own_cache, spec.tags)
    for tag in spec.minus:
        part = {(r.get("accn"), r.get("start", ""), r["end"]): r["val"]
                for r in _facts_for_tag(facts, "us-gaap", tag.split(":", 1)[1], _USD) if _valid(r)}
        recs = [dict(r, val=r["val"] - part[k]) for r in recs
                if (k := (r["accn"], r.get("start", ""), r["end"])) in part]
    return recs


def _continue_with_own(series: list[dict], cand: list[dict],
                       ref: list[dict] | None = None) -> tuple[list[dict], bool]:
    """Append the own tag's facts ending after the series' last period_end, when the
    overlap proof against `ref` (default: the series) holds (see _OWN_CACHE_DIR).
    Returns (records, admitted)."""
    if not series or not cand or not _overlap_proven(series if ref is None else ref, cand):
        return series, False
    last = max(r["end"] for r in series)
    seen = {_fact_key(r) for r in series}
    new = [r for r in cand if r["end"] > last and _fact_key(r) not in seen]
    return (series + new, True) if new else (series, False)


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


def _extract_total_debt(facts: dict, face: tuple | None = None, custom: bool = False) -> list[dict]:
    """Synthetic total_debt records: one per filing snapshot (end, filed, accn) that
    composes under debt.compose_face — the filer's balance-sheet face lines first,
    the value rules otherwise. Figures are only ever added within ONE snapshot (never
    a noncurrent value from one filing with a current value from another). On two
    snapshots sharing (end, filed) the earlier accession wins."""
    snaps = _debt.snapshots(lambda tag: _facts_for_tag(facts, "us-gaap", tag, _USD), extra=tuple(face or ()))
    out: list[dict] = []
    seen: set[tuple] = set()
    for (end, filed, accn) in sorted(snaps):
        if (end, filed) in seen:
            continue
        e = snaps[(end, filed, accn)]
        val, _path = _debt.compose_face(e["tags"], face, custom)
        if val is None:
            continue
        seen.add((end, filed))
        r = e["rec"]
        out.append({"end": end, "filed": filed, "val": val, "accn": accn,
                    "form": r.get("form", ""), "fy": r.get("fy", ""), "fp": r.get("fp", "")})
    return out


def _filing_route_debt(ticker: str, facts: dict, face_row: tuple, cache: dict, spec: dict) -> list[dict]:
    """total_debt of a filing-route name (debt.filing_spec), from its filing instances.
    A "sum" (consolidated) name keeps the companyfacts figure for every period the
    filing route does not produce (its history before 2019, a multi-registrant filing
    whose face line has no parent-level fact); a "member" (industrial-only) name does
    not, so its series never mixes the consolidated figure with the industrial one."""
    recs = _debt.filing_records(spec, face_row[2], cache)
    if spec["mode"] == "sum":
        ends = {r["end"] for r in recs}
        recs += [r for r in _extract_total_debt(facts, face_row[0], face_row[1]) if r["end"] not in ends]
    return recs


def _debt_free_zeros(facts: dict, covered: set[tuple]) -> list[dict]:
    """Explicit total_debt = 0 for a declared debt-free name, one record per
    balance-sheet key (the Assets facts) that real debt facts do not cover, and only
    AFTER the last nonzero debt-line fact (EXPD: bank loans 30M at 2025-12-31 must
    not read as 0 there)."""
    last_debt = ""
    for tag, concept in facts.get("us-gaap", {}).items():
        if not _debt.is_debt_line(tag):
            continue
        for r in concept.get("units", {}).get(_USD, []) or []:
            if "start" not in r and r.get("val") and r.get("end", "") > last_debt:
                last_debt = r["end"]
    out: list[dict] = []
    seen: set[tuple] = set(covered)
    for r in _facts_for_tag(facts, "us-gaap", "Assets", _USD):
        if "end" not in r or "filed" not in r or r["end"] <= last_debt:
            continue
        k = (r["end"], r["filed"])
        if k in seen:
            continue
        seen.add(k)
        out.append({"end": r["end"], "filed": r["filed"], "val": 0,
                    "form": r.get("form", ""), "fy": r.get("fy", ""), "fp": r.get("fp", "")})
    return out


def _extract_company(ticker: str, cik10: str, facts: dict, stats: dict,
                     class_spec: dict | None = None, class_cache: dict | None = None,
                     debt_face: tuple | None = None, debt_cache: dict | None = None,
                     own_specs: dict[str, "_own.OwnSpec"] | None = None,
                     own_cache: dict | None = None) -> list[list]:
    """Extract all canonical concepts for one company -> panel rows. A ticker in the
    coefficient table also gets its per-class economic share counts (class_cache =
    its research/cache/edgar_classes JSON), winning an exact key collision. A ticker in
    own_tags.csv (own_specs: concept -> OwnSpec) continues that concept from own_cache
    (its research/cache/edgar_own JSON) only where the overlap proof holds."""
    rows: list[list] = []

    for concept, (namespace, tags, unit) in _CONCEPTS.items():
        if concept == "depreciation_amortization" and ticker in _DA_NA:
            stats.setdefault("da_na", []).append(ticker)
            continue
        if concept == "shares_outstanding":
            # dei ∪ us-gaap common-shares tags, sub-million sentinels dropped at
            # source (see _extract_shares). NOT the generic _union_chain — this one
            # spans two namespaces and screens the value.
            recs, n_drop = _extract_shares(facts)
            if n_drop:
                stats["shares_sentinels_dropped"] = stats.get("shares_sentinels_dropped", 0) + n_drop
                stats.setdefault("shares_sentinel_names", {})[ticker] = n_drop
            if class_spec is not None:
                crecs = _class_records(ticker, class_spec, class_cache or {}, stats)
                stats["class_records"] = stats.get("class_records", 0) + len(crecs)
                ckeys = {_fact_key(r) for r in crecs}
                recs = [r for r in recs if _fact_key(r) not in ckeys] + crecs
        elif concept == "net_income":
            recs, n_refused = _extract_net_income(facts)
            if n_refused:
                stats["profitloss_refused"] = stats.get("profitloss_refused", 0) + n_refused
        else:
            # Union the chain (tag migration of the SAME line across accounting
            # eras); de-duplicated by (start,end,filed), earlier tag wins a tie.
            src = facts
            if concept == "depreciation_amortization" and _da_cash_flow_tag(ticker, "") is not None:
                src, n_cf = _da_cash_flow_facts(ticker, facts, unit)
                if n_cf:
                    stats.setdefault("da_cf_dropped", {})[ticker] = n_cf
            _tags_used, recs = _union_chain(src, namespace, tags, unit)
        if concept in _FALLBACK_TAGS:
            recs, admitted = _continue_with_fallbacks(facts, concept, recs)
            for tag in admitted:
                stats.setdefault("fallbacks", {}).setdefault(f"{concept}:{tag}", []).append(ticker)
        if own_specs and concept in own_specs:
            spec = own_specs[concept]
            ref = None
            if spec.proof:
                ref = [r for r in _facts_for_tag(facts, "us-gaap", spec.proof.split(":", 1)[1], unit)
                       if _valid(r)]
            recs, ok = _continue_with_own(recs, _own_candidate(facts, own_cache or {}, spec), ref)
            stats.setdefault("own_admitted" if ok else "own_refused", []).append(
                f"{ticker}.{concept}:{'+'.join(spec.tags)}{''.join('-' + t for t in spec.minus)}")

        if recs:
            rows.extend(_emit_rows(ticker, cik10, concept, unit, recs))
        else:
            stats["missing_concept"][concept] = stats["missing_concept"].get(concept, 0) + 1

    # Synthetic total_debt (definition: research/fundamentals/debt.py).
    if ticker in _debt.DEBT_NA:
        stats.setdefault("debt_na", []).append(ticker)
        return rows
    face_row = debt_face if debt_face is not None else (None, False, ())
    route = _debt.filing_spec(ticker, face_row)
    if route is not None:
        debt_recs = _filing_route_debt(ticker, facts, face_row, debt_cache or {}, route)
    else:
        debt_recs = _extract_total_debt(facts, face_row[0], face_row[1])
    if ticker in _DEBT_FREE:
        # old, repaid debt is history; debt at the latest balance sheet breaks the claim
        nonzero = [r for r in debt_recs if r["val"]]
        latest = max((r["end"] for r in _facts_for_tag(facts, "us-gaap", "Assets", _USD) if "end" in r),
                     default="")
        if nonzero and max(r["end"] for r in nonzero) >= latest:
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

    class_table = load_class_table()
    own_table: dict[str, dict[str, _own.OwnSpec]] = {}
    for (tk, concept), spec in _own.load_table().items():
        own_table.setdefault(tk, {})[concept] = spec
    debt_face = _debt.read_face()

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
                spec = class_table.get(ticker)
                ccache = None
                if spec is not None:
                    cpath = _CLASS_CACHE_DIR / f"CIK{cik10}.json"
                    if cpath.exists():
                        ccache = json.loads(cpath.read_text(encoding="utf-8"))
                    else:
                        stats.setdefault("class_cache_missing", []).append(ticker)
                dcache = None
                if _debt.filing_spec(ticker, debt_face.get(ticker)) is not None:
                    dpath = _DEBT_CACHE_DIR / f"CIK{cik10}.json"
                    if dpath.exists():
                        dcache = json.loads(dpath.read_text(encoding="utf-8"))
                    else:
                        stats.setdefault("debt_cache_missing", []).append(ticker)
                ospecs, ocache = own_table.get(ticker), None
                if ospecs:
                    opath = _OWN_CACHE_DIR / f"CIK{cik10}.json"
                    if opath.exists():
                        ocache = json.loads(opath.read_text(encoding="utf-8"))
                    else:
                        stats.setdefault("own_cache_missing", []).append(ticker)
                rows = _extract_company(ticker, cik10, facts, stats, spec, ccache,
                                        debt_face.get(ticker), dcache, ospecs, ocache)
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
    for key, names in sorted(stats.get("fallbacks", {}).items()):
        logger.info("proven fallback %s continues %d names: %s", key, len(names), names)
    if stats.get("own_admitted") or stats.get("own_refused"):
        logger.info("own tags (own_tags.csv): proven %s; proof fails %s",
                    stats.get("own_admitted", []), stats.get("own_refused", []))
    if stats.get("own_cache_missing"):
        logger.warning("own tags: no cache for %s (run collect_own_tags)", sorted(stats["own_cache_missing"]))
    if stats.get("da_na"):
        logger.info("depreciation_amortization: not applicable (_DA_NA) %s", stats["da_na"])
    if stats.get("da_cf_dropped"):
        logger.info("depreciation_amortization: note-tag facts dropped for the cash-flow tag (_DA_CF_*): %d at %d names",
                    sum(stats["da_cf_dropped"].values()), len(stats["da_cf_dropped"]))
    if class_table:
        logger.info("shares_outstanding per class: %d records for %d tickers; %d unguarded; "
                    "weighted fallback %d %s; guard breaches kept %d %s",
                    stats.get("class_records", 0), len(class_table), stats.get("class_unguarded", 0),
                    len(stats.get("class_fallback", [])),
                    sorted({f[0] for f in stats.get("class_fallback", [])}),
                    len(stats.get("class_guard_breach", [])),
                    sorted({f[0] for f in stats.get("class_guard_breach", [])}))
        for k in ("class_cache_missing", "class_unknown_member", "class_ratio_missing"):
            if stats.get(k):
                logger.warning("shares_outstanding per class: %s %s", k, sorted(stats[k])[:20])
    logger.info("total_debt: %d names without (debt.DEBT_NA), filing route %d",
                len(stats.get("debt_na", [])),
                sum(1 for t, _ in universe if _debt.filing_spec(t, debt_face.get(t)) is not None))
    if stats.get("debt_cache_missing"):
        logger.warning("total_debt: no filing cache for %s (run collect_debt_filing)",
                       sorted(stats["debt_cache_missing"]))
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
