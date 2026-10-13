"""
total_debt: the definition and its evidence tables (ПНЛ2, 13.10.2026).

Definition (Ц., СТОП А of ПНЛ2, option (a))
-------------------------------------------
total_debt is the FINANCIAL debt at balance-sheet carrying value:

    long-term debt (noncurrent + its current portion)
  + short-term borrowings (bank loans, commercial paper, other short-term debt)
  + finance-lease liabilities
    (operating leases never)

Two further choices of the same decision:
* a finance arm (F, GM, CAT, PCAR, TXT; the homebuilders' mortgage arms; ARES'
  consolidated funds) is left out: only the industrial part, read per segment from
  the filing's own XBRL instance (FILING_SPECS);
* banks, broker-dealers and insurers get NO total_debt (DEBT_NA): their deposits and
  borrowings are the raw material of the business, and EV is not their measure.

How a value is assembled, per filing snapshot (end, filed, accn)
----------------------------------------------------------------
1. FACE. The filer's balance-sheet debt lines (debt_face.csv, from the presentation
   linkbase of its latest filing, collect_debt_face.py). When every one of them is
   tagged in the snapshot, total = their sum, plus the debt the face does not show:
   the current portion / short-term borrowings when the face has no current debt
   line at all (NOC, PYPL: they sit in "other current liabilities"), and the finance
   leases when no face line carries leases (ORCL: in "other liabilities"). A note tag
   that is a component of a face line is never added (PEP: commercial paper 6.10 and
   current maturities 1.60 are inside its face line ShortTermBorrowings 10.60).
2. VALUE RULES, when the face is unusable (a custom or dimensioned face line, or an
   older snapshot that predates the current face tags): a long-term measure (N =
   noncurrent tag + current part, or T = a tag that already includes the current
   part; the wider wins, N on a tie) + short-term borrowings + finance leases, with
   the nesting rules in compose(). The combined tag
   DebtLongtermAndShorttermCombinedAmount is the last resort: at AVGO it is the
   principal (61.08) and not the carrying value (59.42), so it never competes.
   A face line presented as a total (totalLabel) replaces its components: debt_face.csv
   keeps them apart in "parts" (KMI 29.8 = 29.7 + 0.1; DTE, CNP, AEP).
3. FL-ONLY: a filer whose face shows no debt line and whose snapshot carries only
   finance leases (CPRT, EME) has total_debt = its finance leases.
4. THE FILING: segment-split or dimension-only face lines (FILING_SPECS) and any face
   with a custom, non-us-gaap debt line (filing_spec: TSLA, KR, SMCI, the REITs'
   credit lines ...) are read from the filing's own XBRL instance instead.

A snapshot with only a current figure and no long-term measure yields nothing: a
lone short-term number (CVX 0.40 against ~37 of debt) would read fresh and be wrong.
"""

from __future__ import annotations

import csv
import re
from collections import defaultdict
from pathlib import Path
from typing import Callable

FACE_PATH = Path(__file__).resolve().parent / "debt_face.csv"

# --- which concepts are debt lines -----------------------------------------
DEBT_LINE = re.compile(r"(Debt|Borrowing|CommercialPaper|Notes|LineOfCredit|LinesOfCredit|FinanceLease|"
                       r"CapitalLease|LoansPayable|Bonds)")
NOT_DEBT = re.compile(r"(Operating|Receivable|Securities|Investment|Interest|Issuance|Discount|"
                      r"Asset|Derivative|Policyholder|Deposit|Payments|Proceeds|Repayment|"
                      r"LiabilitiesOtherThan|Capitalization|AtCarryingValue|Capacity|Accumulated|Reserve)")


def is_debt_line(concept: str) -> bool:
    """True for a balance-sheet debt line ("prefix_LocalName" or bare local name)."""
    local = concept.split("_", 1)[-1]
    if local.endswith("Abstract"):
        return False
    return bool(DEBT_LINE.search(local)) and not NOT_DEBT.search(local)


# --- value rules -------------------------------------------------------------
# noncurrent long-term debt; its current part; long-term totals that already hold
# the current part. First present tag of each group wins inside a snapshot.
N_TAGS = ("LongTermDebtNoncurrent", "LongTermDebtAndCapitalLeaseObligations", "LongTermNotesAndLoans")
C_TAGS = ("LongTermDebtCurrent", "LongTermDebtAndCapitalLeaseObligationsCurrent", "NotesPayableCurrent")
T_TAGS = ("LongTermDebt", "DebtAndCapitalLeaseObligations",
          "LongTermDebtAndCapitalLeaseObligationsIncludingCurrentMaturities")
# long-term tags whose definition includes finance (capital) leases
LEASE_INCL = frozenset({"LongTermDebtAndCapitalLeaseObligations", "DebtAndCapitalLeaseObligations",
                        "LongTermDebtAndCapitalLeaseObligationsIncludingCurrentMaturities"})
FL_TAGS = ("FinanceLeaseLiability", "FinanceLeaseLiabilityNoncurrent", "FinanceLeaseLiabilityCurrent")
COMBINED = "DebtLongtermAndShorttermCombinedAmount"
VALUE_TAGS = frozenset(N_TAGS + C_TAGS + T_TAGS + FL_TAGS + (
    COMBINED, "DebtCurrent", "ShortTermBorrowings", "CommercialPaper", "OtherShortTermBorrowings"))
TOL = 0.01   # two figures within 1% are one line shown twice (YUM: current LTD 2.82 = STB 2.81)


def _same(a: float, b: float) -> bool:
    return abs(a - b) <= TOL * max(abs(a), abs(b), 1)


def _first(s: dict, tags: tuple) -> tuple[str | None, float | None]:
    for t in tags:
        if t in s:
            return t, s[t]
    return None, None


def finance_leases(s: dict) -> float | None:
    if "FinanceLeaseLiability" in s:
        return s["FinanceLeaseLiability"]
    if "FinanceLeaseLiabilityNoncurrent" in s or "FinanceLeaseLiabilityCurrent" in s:
        return s.get("FinanceLeaseLiabilityNoncurrent", 0) + s.get("FinanceLeaseLiabilityCurrent", 0)
    return None


def compose(s: dict) -> tuple[float | None, str]:
    """Value rules over one snapshot {tag: val}. Returns (total, path) or (None, why).

    Current side: DebtCurrent when tagged (it is current LTD + short-term borrowings
    by definition); else the current LTD and the short-term borrowings, added unless
    they are one line shown twice (within TOL). Commercial paper counts only when no
    ShortTermBorrowings is tagged (it is a component of it) or when it is larger
    than ShortTermBorrowings (then it cannot be inside it).
    """
    nt, n = _first(s, N_TAGS)
    ct, c = _first(s, C_TAGS)
    tt, t = _first(s, T_TAGS)
    dc = s.get("DebtCurrent")
    stb, cp = s.get("ShortTermBorrowings"), s.get("CommercialPaper")
    st = stb
    if st is None and cp is not None:
        st = cp + (s.get("OtherShortTermBorrowings") or 0)
    elif st is not None and cp is not None and cp > st * (1 + TOL):
        st = st + cp

    if dc is not None:
        cur, cur_path = dc, "DC"
    elif c is not None and st is not None:
        cur, cur_path = (c, "C=ST") if _same(c, st) else (c + st, "C+ST")
    elif c is not None:
        cur, cur_path = c, "C"
    elif st is not None:
        cur, cur_path = st, "ST"
    else:
        cur, cur_path = None, ""

    cands = []
    if n is not None:
        cands.append((n + (cur or 0), nt + ("/" + cur_path if cur is not None else "|only-nc"), nt))
    if t is not None:
        extra, extra_path = 0, ""
        if dc is not None and c is not None:
            extra, extra_path = max(dc - c, 0), "/DC-C"
        elif dc is None and st is not None and not (c is not None and _same(c, st)):
            extra, extra_path = st, "/ST"
        cands.append((t + extra, tt + extra_path, tt))
    if cands:
        # N may be one component only (EQIX: LongTermNotesAndLoans 2.3 of 19.9 total):
        # the wider long-term measure wins, N on a tie (within TOL)
        best = cands[0]
        for cand in cands[1:]:
            if cand[0] > best[0] * (1 + TOL):
                best = cand
        total, path, lt_tag = best
    elif COMBINED in s:
        return s[COMBINED], COMBINED
    else:
        return None, "no long-term measure"
    fl = finance_leases(s)
    if fl and lt_tag not in LEASE_INCL:
        total, path = total + fl, path + "/+FL"
    return total, path


def _is_current_line(t: str) -> bool:
    return ("Current" in t and "Noncurrent" not in t) or "ShortTerm" in t or t == "CommercialPaper"


def compose_face(s: dict, face: tuple | None, custom: bool = False) -> tuple[float | None, str]:
    """FACE first (see the module docstring), the value rules otherwise.

    face   = the filer's us-gaap balance-sheet debt lines (local names); None when
             the filer has no row in debt_face.csv; () when its face shows none.
    custom = the face also has a non-us-gaap debt line (never in companyfacts).
    """
    if face and not custom and all(t in s for t in face):
        total, path = sum(s[t] for t in face), "FACE"
        if not any(_is_current_line(t) or t in T_TAGS for t in face):
            _, c = _first(s, C_TAGS)
            if c is not None:
                total, path = total + c, path + "+C"
            else:
                tt, t = _first(s, T_TAGS)
                nf = total
                if t is not None and tt not in LEASE_INCL and t > nf * (1 + TOL):
                    total, path = total + (t - nf), path + "+T-N"
            st = s.get("ShortTermBorrowings", s.get("CommercialPaper"))
            if st:
                total, path = total + st, path + "+ST"
        if not any("Lease" in t for t in face):
            fl = finance_leases(s)
            if fl:
                total, path = total + fl, path + "+FL"
        return total, path
    total, path = compose(s)
    if total is None and face == () and not custom:
        fl = finance_leases(s)
        if fl:
            return fl, "FL-only"
    return total, path


def snapshots(get: Callable[[str], list], extra: tuple = ()) -> dict:
    """{(end, filed, accn): {"tags": {tag: val}, "rec": first record}} over the
    instant facts of VALUE_TAGS + extra; get(tag) returns companyfacts records."""
    snap: dict = defaultdict(lambda: {"tags": {}, "rec": None})
    for tag in sorted(VALUE_TAGS | set(extra)):
        for r in get(tag):
            if "start" in r or "end" not in r or "filed" not in r or "val" not in r:
                continue
            e = snap[(r["end"], r["filed"], r.get("accn", ""))]
            e["tags"].setdefault(tag, r["val"])
            if e["rec"] is None:
                e["rec"] = r
    return snap


def read_face(path: Path = FACE_PATH) -> dict[str, tuple[tuple, bool, tuple]]:
    """{ticker: (us-gaap face lines, has_custom_line, all face lines)}."""
    if not path.exists():
        return {}
    out = {}
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            if not r.get("role"):
                continue
            lines = tuple(x for x in r["face"].split(";") if x and is_debt_line(x))
            us = tuple(x.split("_", 1)[1] for x in lines if x.startswith("us-gaap_"))
            out[r["ticker"]] = (us, len(us) < len(lines), lines)
    return out


# --- the filing route: segment or dimension-only face lines ---------------------
# mode "member": the industrial part = the face lines' facts on exactly (axis, member);
#                a face line tagged only for the finance arm counts 0 (PCAR: every
#                debt line carries FinancialServices only, its truck business has none).
# mode "sum":    consolidated = the undimensioned fact, else the sum over the members
#                of the one axis the filer splits the line by (TMUS: related party).
# exclude:       face lines that ARE the finance arm (PHM's FinancialServicesDebt).
# The checked values are the latest balance sheet, read from the filing 13.10.2026.
FILING_SPECS: dict[str, dict] = {
    "F": {"mode": "member", "axis": "StatementBusinessSegmentsAxis", "member": "CompanyExcludingFordCreditMember",
          "why": "дългът е по сегменти; без Ford Credit: 3,268 + 16,327 = 19,60 млрд (2026-03-31)"},
    "GM": {"mode": "member", "axis": "BusinessGroupAxis", "member": "AutomotiveMember",
           "why": "дългът е по сегменти; Automotive: 0,514 + 15,465 = 15,98 млрд (2026-06-30)"},
    "CAT": {"mode": "member", "axis": "ProductOrServiceAxis", "member": "MachineryPowerEnergyMember",
            "why": "дългът е по сегменти; ME&T: 0,035 + 10,655 = 10,69 млрд, краткосрочните заеми са само на Financial Products (2026-06-30)"},
    "PCAR": {"mode": "member", "axis": "StatementBusinessSegmentsAxis", "member": "TruckPartsAndOtherMember",
             "why": "дългът е по сегменти; всичките редове (pcar:CommercialPaperAndBankLoans 4,789, NotesPayable 9,908) са само на Financial Services, индустриалната част е без дълг (2026-06-30)"},
    "TXT": {"mode": "member", "axis": "LegalEntityAxis", "member": "ManufacturingGroupMember",
            "why": "дългът е по групи; Manufacturing group: 0,355 + 3,111 = 3,47 млрд, Finance group 0,339 вън (2026-07-04)"},
    "NVR": {"mode": "member", "axis": "StatementBusinessSegmentsAxis", "member": "HomeBuildingSegmentMember",
            "why": "дългът е по сегменти; Homebuilding SeniorNotes 0,908 млрд, ипотечното рамо вън (2026-06-30)"},
    "LEN": {"mode": "member", "axis": "StatementBusinessSegmentsAxis",
            "member": "LennarHomebuildingEastCentralWestHoustonandOtherMember",
            "why": "дългът е по сегменти; Homebuilding 4,047 млрд, Financial Services 1,964 вън (2026-05-31)"},
    "PHM": {"mode": "sum", "axis": "", "exclude": ("FinancialServicesDebt",),
            "why": "собственият ред phm:FinancialServicesDebt (0,478) е ипотечното рамо, вън; NotesPayable 1,820 млрд (2026-06-30)"},
    "ARES": {"mode": "member", "axis": "ConsolidatedEntitiesAxis", "member": "ParentCompanyMember",
             "why": "LongTermDebt на Ares Management 4,577 млрд; ares:FundBorrowings 2,531 са на консолидираните фондове, вън (2026-06-30)"},
    "AES": {"mode": "sum", "axis": "RecourseStatusAxis",
            "why": "редовете са само с регрес / без регрес: 0,300 + 3,162 + 5,800 + 22,855 = 32,12 млрд (2026-06-30)"},
    "APD": {"mode": "sum", "axis": "RelatedPartyTransactionsByRelatedPartyAxis",
            "why": "LongTermDebtAndCapitalLeaseObligations е само по свързани / несвързани лица: 0,127 + 0,769 + 16,585 + 0,186 = 17,67 млрд (2026-06-30)"},
    "BAX": {"mode": "sum", "axis": "",
            "why": "нетекущият ред е собствен таг bax:LongTermDebtAndLeaseObligationExcludingCurrentMaturities: 0 + 0,844 + 8,615 = 9,46 млрд (2026-06-30)"},
    "O": {"mode": "sum", "axis": "",
          "why": "собствен ред o:RevolvingCreditFacilityAndCommercialPaper 2,763 + 2,760 + 0,037 + 25,092 = 30,65 млрд (2026-06-30)"},
    "TMUS": {"mode": "sum", "axis": "RelatedPartyTransactionsByRelatedPartyAxis",
             "why": "LongTermDebtNoncurrent е само по свързани / несвързани лица (78,504 + 0) в 10-Q (2026-06-30)"},
}


def filing_spec(ticker: str, face_row: tuple | None) -> dict | None:
    """The filing-route spec of a name: its FILING_SPECS entry, or — for a filer whose
    balance-sheet face has a custom (non-us-gaap) debt line that companyfacts never
    carries (TSLA, KR: all their debt; D: securitization bonds; the REITs' credit lines
    and commercial paper) — the consolidated sum of its face lines."""
    if ticker in FILING_SPECS:
        return FILING_SPECS[ticker]
    if ticker in DEBT_NA or not face_row or not face_row[1]:
        return None
    custom = [x for x in face_row[2] if not x.startswith("us-gaap_")]
    return {"mode": "sum", "axis": "", "why": "собствен ред в лицето на баланса: " + ", ".join(custom)}


def filing_records(spec: dict, face_lines: tuple, cache: dict) -> list[dict]:
    """total_debt records from the per-filing instance cache (collect_debt_filing).

    face_lines = the filer's face debt lines ("prefix_Local"); a filing instant
    counts only when every face line (minus spec["exclude"]) has a usable fact there:
    in "member" mode any fact (a line shown only for the finance arm counts 0), in
    "sum" mode the undimensioned fact or, when the spec names an axis, its members.
    In "sum" mode the finance leases are added when no face line carries leases."""
    want = [x.split("_", 1)[1] for x in face_lines]
    want = [w for w in want if w not in spec.get("exclude", ())]
    leases_on_face = any("Lease" in w for w in want)
    out = []
    for accn, e in sorted(cache.items(), key=lambda kv: kv[1].get("filed", "")):
        by_inst: dict = defaultdict(lambda: defaultdict(list))
        for concept, inst, dims, val in e.get("facts", []):
            by_inst[inst][concept].append((tuple(tuple(d) for d in dims), val))
        for inst, per in by_inst.items():
            if not want or any(w not in per for w in want):
                continue
            total, ok = 0.0, True
            for w in want:
                facts = per[w]
                if spec["mode"] == "member":
                    total += sum(v for d, v in facts if d == ((spec["axis"], spec["member"]),))
                    continue
                plain = [v for d, v in facts if not d]
                on_axis = [v for d, v in facts if spec["axis"] and len(d) == 1 and d[0][0] == spec["axis"]]
                if plain:
                    total += plain[0]
                elif on_axis:
                    total += sum(on_axis)
                else:
                    ok = False   # only on another axis: no consolidated figure here
                    break
            if not ok:
                continue
            if spec["mode"] == "sum" and not leases_on_face:
                fl = finance_leases({c: v[0][1] for c, v in per.items()
                                     if c in FL_TAGS and v and not v[0][0]})
                total += fl or 0
            out.append({"end": inst, "filed": e["filed"], "val": total, "accn": accn,
                        "form": e.get("form", ""), "fy": e.get("fy", ""), "fp": e.get("fp", "")})
    return out


# --- no total_debt at all: not applicable, or not computable ------------------------
_BANK = "неприложимо: банка (депозитите и заемите са суровината, EV не е мярката)"
_BROKER = "неприложимо: брокер-дилър / банков холдинг (заемите финансират клиентските позиции)"
_INSURER = "неприложимо: застраховател (балансът е резерви и инвестиции, EV не е мярката)"
DEBT_NA: dict[str, str] = {
    **{t: _BANK for t in ("BAC", "C", "JPM", "PNC", "TFC", "USB", "WFC",            # diversified
                          "CFG", "FITB", "HBAN", "KEY", "MTB", "RF",                # regional
                          "BNY", "NTRS", "STT",                                     # custody banks
                          "AXP", "COF", "SYF")},                                    # card banks
    **{t: _BROKER for t in ("GS", "MS", "SCHW", "RJF", "IBKR", "HOOD")},
    **{t: _INSURER for t in ("AFL", "GL", "MET", "PFG", "PRU", "AIG", "AIZ", "L",
                             "ACGL", "ALL", "CB", "CINF", "HIG", "PGR", "TRV", "WRB", "EG")},
    "BRK.B": "неприложимо: застрахователен конгломерат; дългът е по сегменти (Insurance and other 43,3 / "
             "Railroad, utilities and energy 85,3 млрд, 2026-06-30)",
    "AMP": "неприложимо: застраховател и банка в групата (RiverSource, Ameriprise Bank)",
    "KKR": "неприложимо: консолидира застрахователя Global Atlantic и CLO фондове; LongTermDebt е само "
           "по сегменти (Asset Management 49,7 / Insurance 3,8 млрд, 2026-06-30)",
    "APO": "неприложимо: консолидира застрахователя Athene",
    # not computable under the chosen definition — no total_debt rather than a wrong one
    "DE": "друго: финансовото рамо (John Deere Financial) не е отделено в XBRL на 10-Q (нито един ред за "
          "дълг с член за Equipment Operations, 0001104659-26-102213 / 2026-08-02); консолидираният дълг "
          "17,115 + 6,095 + 40,626 = 63,84 млрд е предимно на рамото, а решението е само индустриалната част",
}
