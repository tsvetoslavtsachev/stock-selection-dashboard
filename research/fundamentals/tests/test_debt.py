"""
total_debt = the financial debt (ПНЛ2): one test per component and per nesting rule,
plus the filing route and the declarations. Values are the real ones read on
13.10.2026 (AAPL, PEP, ORCL, AVGO, YUM, EQIX, F, PCAR, TMUS, PHM). No cache needed.

    python -m pytest research/fundamentals/tests/test_debt.py -q
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from research.fundamentals import build_panel, debt, validate_gates  # noqa: E402
from research.fundamentals.debt import compose, compose_face  # noqa: E402


def _v(res):
    return None if res[0] is None else round(res[0], 3)


# ---------------------------------------------------------------------------
# components, each on its own
# ---------------------------------------------------------------------------
def test_long_term_noncurrent_plus_current_portion():
    assert _v(compose({"LongTermDebtNoncurrent": 57.167, "LongTermDebtCurrent": 2.252})) == 59.419


def test_short_term_borrowings_added_to_long_term():
    # WMT: separate face lines, all of them debt
    s = {"LongTermDebtNoncurrent": 36.462, "LongTermDebtCurrent": 3.470, "ShortTermBorrowings": 10.479}
    assert _v(compose(s)) == 50.411


def test_commercial_paper_counts_when_no_short_term_borrowings_tag():
    s = {"LongTermDebtNoncurrent": 71.340, "LongTermDebtCurrent": 11.007, "CommercialPaper": 1.997}
    assert _v(compose(s)) == 84.344                     # AAPL


def test_finance_leases_added_unless_the_long_term_tag_holds_them():
    s = {"LongTermNotesAndLoans": 117.712, "NotesPayableCurrent": 7.625, "FinanceLeaseLiability": 9.185}
    assert _v(compose(s)) == 134.522                    # ORCL
    s = {"LongTermDebtAndCapitalLeaseObligations": 10.422, "DebtCurrent": 0.927,
         "FinanceLeaseLiability": 2.0}
    assert _v(compose(s)) == 11.349                     # VLO: leases inside its LT tag


def test_finance_lease_split_tags_are_summed():
    s = {"LongTermDebtNoncurrent": 1.0, "FinanceLeaseLiabilityNoncurrent": 0.3,
         "FinanceLeaseLiabilityCurrent": 0.1}
    assert _v(compose(s)) == 1.4


def test_operating_leases_never_count():
    s = {"LongTermDebtNoncurrent": 1.0, "OperatingLeaseLiability": 5.0}
    assert _v(compose(s)) == 1.0


# ---------------------------------------------------------------------------
# nesting: a component is never added on top of the line that holds it
# ---------------------------------------------------------------------------
def test_debt_current_replaces_current_portion_and_short_term():
    s = {"LongTermDebtNoncurrent": 13.51, "DebtCurrent": 3.30, "LongTermDebtCurrent": 1.0,
         "ShortTermBorrowings": 2.3}
    assert _v(compose(s)) == 16.81                      # BDX shape


def test_one_line_shown_twice_counts_once():
    s = {"LongTermDebtAndCapitalLeaseObligations": 9.46, "LongTermDebtCurrent": 2.82,
         "ShortTermBorrowings": 2.81}
    assert _v(compose(s)) == 12.28                      # YUM


def test_commercial_paper_inside_short_term_borrowings():
    s = {"LongTermDebtNoncurrent": 10, "ShortTermBorrowings": 4.86, "CommercialPaper": 4.53}
    assert _v(compose(s)) == 14.86                      # LIN shape
    s["CommercialPaper"] = 6.0                          # larger: cannot be inside
    assert _v(compose(s)) == 20.86


def test_face_lines_decide_nesting_pep():
    # PEP: the face shows ONE short-term line; current maturities 1.60 and CP 6.10 are note
    # components of it and must not be added again (value rules alone would give 54.81)
    s = {"LongTermDebtAndCapitalLeaseObligations": 42.612, "ShortTermBorrowings": 10.602,
         "LongTermDebtCurrent": 1.600, "CommercialPaper": 6.100}
    face = ("ShortTermBorrowings", "LongTermDebtAndCapitalLeaseObligations")
    assert _v(compose_face(s, face)) == 53.214
    assert _v(compose(s)) == 54.814


def test_face_adds_finance_leases_not_on_the_face():
    s = {"LongTermNotesAndLoans": 117.712, "NotesPayableCurrent": 7.625, "FinanceLeaseLiability": 9.185}
    assert _v(compose_face(s, ("LongTermNotesAndLoans", "NotesPayableCurrent"))) == 134.522
    face_with_leases = ("LongTermDebtNoncurrent", "FinanceLeaseLiabilityNoncurrent")
    s = {"LongTermDebtNoncurrent": 78.5, "FinanceLeaseLiabilityNoncurrent": 1.1, "FinanceLeaseLiability": 2.3}
    assert _v(compose_face(s, face_with_leases)) == 79.6


def test_face_with_a_custom_line_falls_back_to_value_rules():
    s = {"LongTermDebtNoncurrent": 1.06, "DebtCurrent": 0.98}
    assert _v(compose_face(s, ("LongTermDebtNoncurrent",), custom=True)) == 2.04


def test_combined_tag_never_competes_with_carrying_value():
    # AVGO: the combined tag is the principal (61.079), the panel keeps carrying value
    s = {"LongTermDebtNoncurrent": 57.167, "LongTermDebtCurrent": 2.252,
         "DebtLongtermAndShorttermCombinedAmount": 61.079}
    assert _v(compose(s)) == 59.419
    assert _v(compose({"DebtLongtermAndShorttermCombinedAmount": 8.48})) == 8.48   # last resort


def test_wider_long_term_measure_wins():
    # EQIX: LongTermNotesAndLoans is one component; LongTermDebt is the whole
    s = {"LongTermNotesAndLoans": 2.29, "LongTermDebt": 19.87}
    assert _v(compose(s)) == 19.87


# ---------------------------------------------------------------------------
# the "noncurrent only" rule (Г4 mutation gate)
# ---------------------------------------------------------------------------
def test_only_noncurrent_when_no_current_part_is_reported():
    total, path = compose({"LongTermDebtNoncurrent": 3.52})
    assert total == 3.52 and path.endswith("|only-nc")   # APP: no current leg tagged


@pytest.mark.parametrize("cur_tag", ["LongTermDebtCurrent", "DebtCurrent", "ShortTermBorrowings",
                                     "CommercialPaper", "LongTermDebtAndCapitalLeaseObligationsCurrent",
                                     "NotesPayableCurrent"])
def test_only_noncurrent_rule_never_takes_a_reported_current_part(cur_tag):
    # mutation: a rule that returned the noncurrent figure alone here would fail
    total, path = compose({"LongTermDebtNoncurrent": 10.0, cur_tag: 1.5})
    assert total == 11.5 and "only-nc" not in path


def test_face_with_noncurrent_line_only_adds_the_current_part_from_the_notes():
    # NOC / PYPL: the current portion sits in "other current liabilities"
    s = {"LongTermDebtNoncurrent": 9.42, "LongTermDebtCurrent": 1.46}
    assert _v(compose_face(s, ("LongTermDebtNoncurrent",))) == 10.88
    s = {"LongTermDebtNoncurrent": 9.42, "LongTermDebt": 10.88}
    assert _v(compose_face(s, ("LongTermDebtNoncurrent",))) == 10.88


def test_lone_current_figure_is_not_total_debt():
    # CVX: only ShortTermBorrowings 0.40 in companyfacts, ~37 of real debt
    assert compose({"ShortTermBorrowings": 0.40})[0] is None


def test_finance_leases_only_when_the_face_shows_no_debt():
    s = {"FinanceLeaseLiability": 0.006}
    assert _v(compose_face(s, ())) == 0.006              # EME
    assert compose_face(s, None)[0] is None              # face unknown: no claim


# ---------------------------------------------------------------------------
# panel assembly
# ---------------------------------------------------------------------------
def _f(end, val, filed, accn="a1", form="10-Q"):
    return {"end": end, "val": val, "filed": filed, "accn": accn, "form": form, "fy": 2026, "fp": "Q2"}


def _facts(**tags):
    return {"us-gaap": {t: {"units": {"USD": recs}} for t, recs in tags.items()}}


def test_figures_from_different_filings_are_never_summed():
    facts = _facts(LongTermDebtNoncurrent=[_f("2026-06-30", 10, "2026-07-30", accn="x")],
                   LongTermDebtCurrent=[_f("2026-06-30", 1, "2026-08-15", accn="y")])
    recs = build_panel._extract_total_debt(facts)
    assert [(r["filed"], r["val"]) for r in recs] == [("2026-07-30", 10)]


def test_not_applicable_name_gets_no_debt_rows():
    facts = _facts(LongTermDebt=[_f("2026-06-30", 300e9, "2026-08-01")])
    rows = build_panel._extract_company("JPM", "0000019617", facts, {"missing_concept": {}})
    assert not [r for r in rows if r[2] == "total_debt"]


# ---------------------------------------------------------------------------
# the filing route
# ---------------------------------------------------------------------------
def _cache(facts, accn="a1", filed="2026-04-30"):
    return {accn: {"form": "10-Q", "filed": filed, "fy": "2026", "fp": "Q1", "facts": facts}}


def test_member_mode_keeps_the_industrial_part_only():
    seg = "StatementBusinessSegmentsAxis"
    facts = [["DebtCurrent", "2026-03-31", [[seg, "CompanyExcludingFordCreditMember"]], 3.268e9],
             ["DebtCurrent", "2026-03-31", [[seg, "FordCreditMember"]], 47.523e9],
             ["LongTermDebtAndCapitalLeaseObligations", "2026-03-31", [[seg, "CompanyExcludingFordCreditMember"]], 16.327e9],
             ["LongTermDebtAndCapitalLeaseObligations", "2026-03-31", [[seg, "FordCreditMember"]], 90.008e9]]
    face = ("us-gaap_DebtCurrent", "us-gaap_LongTermDebtAndCapitalLeaseObligations")
    recs = debt.filing_records(debt.FILING_SPECS["F"], face, _cache(facts))
    assert [(r["end"], round(r["val"] / 1e9, 3)) for r in recs] == [("2026-03-31", 19.595)]


def test_member_mode_line_only_on_the_arm_counts_zero():
    seg = "StatementBusinessSegmentsAxis"
    facts = [["CommercialPaperAndBankLoans", "2026-06-30", [[seg, "FinancialServicesMember"]], 4.789e9],
             ["NotesPayable", "2026-06-30", [[seg, "FinancialServicesMember"]], 9.908e9]]
    face = ("pcar_CommercialPaperAndBankLoans", "us-gaap_NotesPayable")
    recs = debt.filing_records(debt.FILING_SPECS["PCAR"], face, _cache(facts))
    assert [r["val"] for r in recs] == [0]


def test_sum_mode_adds_the_members_of_the_one_axis():
    ax = "RelatedPartyTransactionsByRelatedPartyAxis"
    facts = [["ShortTermBorrowings", "2026-06-30", [], 6.117e9],
             ["LongTermDebtNoncurrent", "2026-06-30", [[ax, "NonrelatedPartyMember"]], 78.504e9],
             ["LongTermDebtNoncurrent", "2026-06-30", [[ax, "RelatedPartyMember"]], 0.0],
             ["LongTermDebtNoncurrent", "2026-06-30", [["CounterpartyNameAxis", "ThirdPartyMember"]], 78.504e9]]
    face = ("us-gaap_ShortTermBorrowings", "us-gaap_LongTermDebtNoncurrent")
    recs = debt.filing_records(debt.FILING_SPECS["TMUS"], face, _cache(facts))
    assert [round(r["val"] / 1e9, 3) for r in recs] == [84.621]


def test_excluded_arm_line_and_incomplete_instants():
    facts = [["FinancialServicesDebt", "2026-06-30", [], 0.478e9],
             ["NotesPayable", "2026-06-30", [], 1.820e9],
             ["FinancialServicesDebt", "2025-12-31", [], 0.6e9]]          # NotesPayable missing there
    face = ("phm_FinancialServicesDebt", "us-gaap_NotesPayable")
    recs = debt.filing_records(debt.FILING_SPECS["PHM"], face, _cache(facts))
    assert [(r["end"], round(r["val"] / 1e9, 3)) for r in recs] == [("2026-06-30", 1.82)]


def test_custom_face_line_routes_through_the_filing():
    face_row = (("ShortTermBorrowings",), True, ("us-gaap_ShortTermBorrowings", "tsla_LongTermDebtAndFinanceLeasesNoncurrent"))
    spec = debt.filing_spec("TSLA", face_row)
    assert spec["mode"] == "sum" and "tsla_LongTermDebtAndFinanceLeasesNoncurrent" in spec["why"]
    assert debt.filing_spec("AAPL", (("LongTermDebtNoncurrent",), False, ("us-gaap_LongTermDebtNoncurrent",))) is None
    assert debt.filing_spec("JPM", face_row) is None                     # not applicable stays out
    assert debt.filing_spec("F", None) is debt.FILING_SPECS["F"]


def test_sum_mode_skips_a_line_shown_only_on_another_axis_and_adds_leases():
    spec = {"mode": "sum", "axis": "", "why": ""}
    face = ("us-gaap_ShortTermBorrowings", "o_RevolvingCreditFacilityAndCommercialPaper")
    only_dims = [["ShortTermBorrowings", "2026-06-30", [], 1e9],
                 ["RevolvingCreditFacilityAndCommercialPaper", "2026-06-30", [["SomeAxis", "XMember"]], 2e9]]
    assert debt.filing_records(spec, face, _cache(only_dims)) == []      # no silent 0 for the line
    plain = [["ShortTermBorrowings", "2026-06-30", [], 1e9],
             ["RevolvingCreditFacilityAndCommercialPaper", "2026-06-30", [], 2e9],
             ["FinanceLeaseLiability", "2026-06-30", [], 0.5e9]]
    assert [r["val"] for r in debt.filing_records(spec, face, _cache(plain))] == [3.5e9]


def test_face_noise_lines_are_not_debt():
    assert not debt.is_debt_line("pg_ReserveForEsopDebtRetirement")
    assert not debt.is_debt_line("apa_AccumulatedDepreciationDepletionAndAmortizationPropertyAndEquipmentIncludingFinanceLease")
    assert not debt.is_debt_line("us-gaap_LineOfCreditFacilityMaximumBorrowingCapacity")
    assert not debt.is_debt_line("us-gaap_CommercialPaperAtCarryingValue")
    assert not debt.is_debt_line("us-gaap_LiabilitiesOtherThanLongtermDebtNoncurrent")
    assert debt.is_debt_line("us-gaap_SeniorLongTermNotes") and debt.is_debt_line("us-gaap_CommercialPaper")


# ---------------------------------------------------------------------------
# G6d: the declarations (Г4 gate for _DEBT_FREE)
# ---------------------------------------------------------------------------
def _panel(rows):
    return pd.DataFrame(rows, columns=["ticker", "concept", "period_end", "filed", "value"])


def test_g6d_fails_a_debt_free_name_with_debt():
    ok = _panel([["PLTR", "total_debt", "2026-06-30", "2026-08-04", 0]])
    bad = _panel([["PLTR", "total_debt", "2026-03-31", "2026-05-04", 0],
                  ["PLTR", "total_debt", "2026-06-30", "2026-08-04", 5e8]])
    assert validate_gates.g6d_debt_declarations(ok, {"PLTR": "x"}) == []
    assert validate_gates.g6d_debt_declarations(bad, {"PLTR": "x"}) == [
        "DEBT_FREE PLTR: total_debt 500,000,000 at 2026-06-30"]


def test_g6d_fails_a_not_applicable_name_with_debt_rows():
    p = _panel([["JPM", "total_debt", "2026-06-30", "2026-08-01", 3e11]])
    assert validate_gates.g6d_debt_declarations(p, {}) == ["DEBT_NA JPM: has total_debt rows"]


def test_declared_tables_do_not_overlap():
    assert not set(build_panel._DEBT_FREE) & set(debt.DEBT_NA)
    assert not set(debt.FILING_SPECS) & set(debt.DEBT_NA)
    assert not set(debt.FILING_SPECS) & set(build_panel._DEBT_FREE)
