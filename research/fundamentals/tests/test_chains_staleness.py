"""
Synthetic tests for the widened tag chains (total_debt pairs, NCI-aware net_income,
explicit debt-free names) and the G6 staleness ratchet. No cache needed.

    python -m pytest research/fundamentals/tests/test_chains_staleness.py -q
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from research.fundamentals import build_panel, validate_gates  # noqa: E402


def _f(end, val, filed, accn="a1", start=None, form="10-Q"):
    r = {"end": end, "val": val, "filed": filed, "accn": accn, "form": form, "fy": 2026, "fp": "Q1"}
    if start:
        r["start"] = start
    return r


def _facts(**tags):
    return {"us-gaap": {t: {"units": {"USD": recs}} for t, recs in tags.items()}}


# ---------------------------------------------------------------------------
# total_debt
# ---------------------------------------------------------------------------
def test_debt_notes_pair_beats_stray_longtermdebt_zero():
    # ORCL shape: one stray LongTermDebt=0 used to shut every later tag out.
    facts = _facts(
        LongTermDebt=[_f("2022-05-31", 0, "2022-06-21")],
        LongTermNotesAndLoans=[_f("2026-08-31", 117.7, "2026-09-11")],
        NotesPayableCurrent=[_f("2026-08-31", 7.6, "2026-09-11")],
        DebtLongtermAndShorttermCombinedAmount=[_f("2026-05-31", 129.5, "2026-06-22")],
    )
    recs = {(r["end"], r["filed"]): r["val"] for r in build_panel._extract_total_debt(facts)}
    assert recs[("2026-08-31", "2026-09-11")] == 117.7 + 7.6
    assert recs[("2026-05-31", "2026-06-22")] == 129.5      # combined tag no longer shut out
    assert recs[("2022-05-31", "2022-06-21")] == 0


def test_debt_pair_needs_both_legs_from_one_filing():
    facts = _facts(
        LongTermDebtAndCapitalLeaseObligations=[_f("2026-05-28", 5.1, "2026-06-25", accn="x")],
        DebtCurrent=[_f("2026-05-28", 0.6, "2026-06-25", accn="y")],   # other filing snapshot
    )
    assert build_panel._extract_total_debt(facts) == []


def test_debt_earlier_pair_wins_same_key():
    facts = _facts(
        LongTermDebtNoncurrent=[_f("2026-06-30", 10, "2026-07-30")],
        LongTermDebtCurrent=[_f("2026-06-30", 1, "2026-07-30")],
        LongTermDebtAndCapitalLeaseObligations=[_f("2026-06-30", 12, "2026-07-30")],
        DebtCurrent=[_f("2026-06-30", 2, "2026-07-30")],
        LongTermDebt=[_f("2026-06-30", 11, "2026-07-30")],
    )
    recs = build_panel._extract_total_debt(facts)
    assert [r["val"] for r in recs] == [11]   # split pair, once; no double count


def test_debt_free_name_gets_explicit_zero():
    facts = _facts(Assets=[_f("2026-06-30", 8e9, "2026-08-04")],
                   RevenueFromContractWithCustomerExcludingAssessedTax=[
                       _f("2026-06-30", 1e9, "2026-08-04", start="2026-04-01")])
    rows = build_panel._extract_company("PLTR", "0001321655", facts, {"missing_concept": {}})
    debt = [r for r in rows if r[2] == "total_debt"]
    assert [(r[5], r[10]) for r in debt] == [("2026-06-30", 0)]
    rows = build_panel._extract_company("ZZZ", "0000000001", facts, {"missing_concept": {}})
    assert not [r for r in rows if r[2] == "total_debt"]   # undeclared names stay honest-empty


# ---------------------------------------------------------------------------
# net_income
# ---------------------------------------------------------------------------
def test_profitloss_fills_when_issuer_shows_nci_immaterial():
    # AVGO shape: NetIncomeLoss stops, ProfitLoss carries on, both equal where shared.
    old = [_f("2024-11-03", 5.9, "2024-12-20", start="2024-08-05")]
    facts = _facts(
        NetIncomeLoss=old,
        ProfitLoss=old + [_f("2026-08-02", 9.7, "2026-09-10", start="2026-05-04")],
    )
    recs, refused = build_panel._extract_net_income(facts)
    assert refused == 0
    assert {r["end"]: r["val"] for r in recs} == {"2024-11-03": 5.9, "2026-08-02": 9.7}


def test_profitloss_refused_when_nci_material_and_no_nci_fact():
    # IBKR shape: ProfitLoss ~12x NetIncomeLoss where shared.
    shared = _f("2025-06-30", 0.2, "2025-08-01", start="2025-04-01")
    facts = _facts(
        NetIncomeLoss=[shared],
        ProfitLoss=[{**shared, "val": 2.4},
                    _f("2026-06-30", 2.6, "2026-08-01", start="2026-04-01")],
    )
    recs, refused = build_panel._extract_net_income(facts)
    assert refused == 1
    assert [r["end"] for r in recs] == ["2025-06-30"]


def test_profitloss_minus_nci_fact_when_present():
    shared = _f("2025-06-30", 0.2, "2025-08-01", start="2025-04-01")
    fill = _f("2026-06-30", 2.6, "2026-08-01", start="2026-04-01")
    facts = _facts(
        NetIncomeLoss=[shared],
        ProfitLoss=[{**shared, "val": 2.4}, fill],
        NetIncomeLossAttributableToNoncontrollingInterest=[{**fill, "val": 2.35}],
    )
    recs, refused = build_panel._extract_net_income(facts)
    assert refused == 0
    assert abs({r["end"]: r["val"] for r in recs}["2026-06-30"] - 0.25) < 1e-12


# ---------------------------------------------------------------------------
# G6 staleness ratchet
# ---------------------------------------------------------------------------
def _panel(rows):
    df = pd.DataFrame(rows, columns=["ticker", "concept", "period_end"])
    df["period_end"] = pd.to_datetime(df["period_end"])
    return df


def _full(ticker, end, skip=(), override=None):
    override = override or {}
    rows = [(ticker, "revenues", end)]
    for c in validate_gates._ALL_CONCEPTS:
        if c != "revenues" and c not in skip:
            rows.append((ticker, c, override.get(c, end)))
    return rows


def test_g6_fresh_panel_passes_with_empty_baseline():
    g6 = validate_gates.g6_staleness(_panel(_full("AAA", "2026-06-30")),
                                     validate_gates.load_staleness_baseline(Path("nope.csv")))
    assert g6["failures"] == []


def test_g6_fires_on_stale_and_absent_concepts():
    rows = _full("ORCL", "2026-08-31", skip=("net_income",),
                 override={"total_debt": "2022-05-31"})
    g6 = validate_gates.g6_staleness(_panel(rows), validate_gates.load_staleness_baseline(Path("nope.csv")))
    assert sorted(g6["failures"]) == [
        "NEW ORCL.net_income: last absent, revenues 2026-08-31",
        "NEW ORCL.total_debt: last 2022-05-31, revenues 2026-08-31",
    ]


def test_g6_threshold_is_strictly_over_100_days():
    ok = _full("AAA", "2026-06-30", override={"total_debt": "2026-03-22"})     # 100 days
    bad = _full("BBB", "2026-06-30", override={"total_debt": "2026-03-21"})    # 101 days
    g6 = validate_gates.g6_staleness(_panel(ok + bad), validate_gates.load_staleness_baseline(Path("nope.csv")))
    assert [r.ticker for r in g6["new"]] == ["BBB"]


def test_g6_baseline_known_regressed_and_healed():
    base = pd.DataFrame([
        {"ticker": "AAA", "concept": "buybacks", "last_period_end": "2023-12-31",
         "revenues_period_end": "2026-06-30", "lag_days": 912},
        {"ticker": "AAA", "concept": "capex", "last_period_end": "2025-12-31",
         "revenues_period_end": "2026-06-30", "lag_days": 181},
        {"ticker": "AAA", "concept": "gross_profit", "last_period_end": None,
         "revenues_period_end": "2026-06-30", "lag_days": None},
    ]).assign(reason="тест")
    rows = _full("AAA", "2026-06-30", skip=("gross_profit",),
                 override={"buybacks": "2023-12-31", "capex": "2025-09-30",
                           "total_debt": "2026-06-30"})
    g6 = validate_gates.g6_staleness(_panel(rows), base)
    assert g6["failures"] == ["REGRESSED AAA.capex: last 2025-09-30 < baseline 2025-12-31"]
    assert g6["healed"] == []

    rows = _full("AAA", "2026-06-30", skip=("gross_profit",), override={"buybacks": "2023-12-31"})
    g6 = validate_gates.g6_staleness(_panel(rows), base)
    assert g6["failures"] == [] and g6["healed"] == [("AAA", "capex")]


# ---------------------------------------------------------------------------
# Proven per-filer fallbacks (ПНЛ4) and the reason column of the G6 baseline
# ---------------------------------------------------------------------------
def _q(end, start, val, filed):
    return _f(end, val, filed, start=start)


_CHAIN = [_q("2024-03-31", "2024-01-01", 100.0, "2024-04-30"),
          _q("2024-06-30", "2024-04-01", 110.0, "2024-07-30")]


def test_fallback_continues_chain_after_proven_overlap_only_forward():
    # ACN shape: the chain tag stops, the new tag re-files the old quarters (same
    # numbers, later filing) and carries on. Only facts ending AFTER the chain's last
    # period enter; the re-filed comparatives do not duplicate or shift anything.
    ordinary = [_q("2024-03-31", "2024-01-01", 100.0, "2025-04-30"),
                _q("2024-06-30", "2024-04-01", 110.2, "2025-07-30"),     # 0.18% off: agrees
                _q("2025-06-30", "2025-04-01", 120.0, "2025-07-30")]
    facts = _facts(PaymentsOfDividendsCommonStock=_CHAIN, PaymentsOfOrdinaryDividends=ordinary)
    recs, admitted = build_panel._continue_with_fallbacks(facts, "dividends_paid", list(_CHAIN))
    assert admitted == ["PaymentsOfOrdinaryDividends"]
    assert [(r["end"], r["val"]) for r in recs] == [
        ("2024-03-31", 100.0), ("2024-06-30", 110.0), ("2025-06-30", 120.0)]


def test_fallback_refused_when_a_shared_period_differs():
    # AIZ shape: DepreciationAmortizationAndAccretionNet is a wider number than the chain.
    wide = [_q("2024-03-31", "2024-01-01", 100.0, "2025-04-30"),
            _q("2024-06-30", "2024-04-01", 111.0, "2025-07-30"),         # 0.9% off
            _q("2025-06-30", "2025-04-01", 120.0, "2025-07-30")]
    facts = _facts(Depreciation=_CHAIN, DepreciationAmortizationAndAccretionNet=wide)
    recs, admitted = build_panel._continue_with_fallbacks(facts, "depreciation_amortization", list(_CHAIN))
    assert admitted == [] and len(recs) == 2


def test_fallback_needs_two_shared_periods():
    one = [_q("2024-06-30", "2024-04-01", 110.0, "2025-07-30"),
           _q("2025-06-30", "2025-04-01", 120.0, "2025-07-30")]
    facts = _facts(PaymentsForRepurchaseOfCommonStock=_CHAIN, PaymentsForRepurchaseOfEquity=one)
    _recs, admitted = build_panel._continue_with_fallbacks(facts, "buybacks", list(_CHAIN))
    assert admitted == []


def _with_per_share(facts, per_share_tag, per_share, shares):
    g = facts.setdefault("us-gaap", {})
    g[per_share_tag] = {"units": {"USD/shares": per_share}}
    g["WeightedAverageNumberOfSharesOutstandingBasic"] = {"units": {"shares": shares}}
    return facts


def test_identity_admits_profitloss_when_eps_times_shares_matches():
    # ITW shape: no NetIncomeLoss at all, only ProfitLoss and no NCI fact, so the NCI
    # rule refuses it; basic EPS x weighted shares proves it is the parent's income.
    q = [("2026-03-31", "2026-01-01", 10.0), ("2026-06-30", "2026-04-01", 12.0)]
    pl = [_q(e, s, v * 100, "2026-07-30") for e, s, v in q]
    eps = [_q(e, s, v, "2026-07-30") for e, s, v in q]
    sh = [_q(e, s, 100.0, "2026-07-30") for e, s, _ in q]
    facts = _with_per_share(_facts(ProfitLoss=pl), "EarningsPerShareBasic", eps, sh)
    recs, refused = build_panel._extract_net_income(facts)
    assert recs == [] and refused == 2
    recs, admitted = build_panel._continue_with_fallbacks(facts, "net_income", recs)
    assert admitted == ["ProfitLoss"] and [r["val"] for r in recs] == [1000.0, 1200.0]

    eps_off = [_q(e, s, v * 1.03, "2026-07-30") for e, s, v in q]                # 3% off
    facts = _with_per_share(_facts(ProfitLoss=pl), "EarningsPerShareBasic", eps_off, sh)
    _recs, admitted = build_panel._continue_with_fallbacks(facts, "net_income", [])
    assert admitted == []


def test_identity_for_dividends_uses_fiscal_years_only():
    # JNJ shape: quarters drift with declared-vs-paid timing, fiscal years do not.
    years = [("2024-12-31", "2024-01-01"), ("2025-12-31", "2025-01-01")]
    paid = [_q(e, s, 400.0, "2026-02-15") for e, s in years] +            [_q("2026-03-31", "2026-01-01", 150.0, "2026-04-30")]                 # quarter: 1.5x
    dps = [_q(e, s, 4.0, "2026-02-15") for e, s in years] +           [_q("2026-03-31", "2026-01-01", 1.0, "2026-04-30")]
    sh = [_q(e, s, 100.0, "2026-02-15") for e, s in years] +          [_q("2026-03-31", "2026-01-01", 100.0, "2026-04-30")]
    facts = _with_per_share(_facts(PaymentsOfOrdinaryDividends=paid),
                            "CommonStockDividendsPerShareDeclared", dps, sh)
    recs, admitted = build_panel._continue_with_fallbacks(facts, "dividends_paid", [])
    assert admitted == ["PaymentsOfOrdinaryDividends"] and len(recs) == 3


def test_identity_never_used_when_chain_has_facts():
    # with a chain, only the overlap proof counts: no shared period -> nothing enters
    facts = _with_per_share(
        _facts(PaymentsOfDividendsCommonStock=_CHAIN,
               PaymentsOfOrdinaryDividends=[_q("2025-12-31", "2025-01-01", 400.0, "2026-02-15")]),
        "CommonStockDividendsPerShareDeclared",
        [_q("2025-12-31", "2025-01-01", 4.0, "2026-02-15")],
        [_q("2025-12-31", "2025-01-01", 100.0, "2026-02-15")])
    _recs, admitted = build_panel._continue_with_fallbacks(facts, "dividends_paid", list(_CHAIN))
    assert admitted == []


def test_g6_fails_on_a_baseline_pair_without_reason():
    base = pd.DataFrame([{"ticker": "AAA", "concept": "buybacks", "last_period_end": "2023-12-31",
                          "revenues_period_end": "2026-06-30", "lag_days": 912, "reason": ""}])
    rows = _full("AAA", "2026-06-30", override={"buybacks": "2023-12-31"})
    g6 = validate_gates.g6_staleness(_panel(rows), base)
    assert g6["failures"] == ["UNEXPLAINED AAA.buybacks: no reason in the baseline"]
    g6 = validate_gates.g6_staleness(_panel(rows), base.assign(reason="в/спряло: последен факт 2023-12-31"))
    assert g6["failures"] == []


def test_write_baseline_keeps_reasons_and_leaves_new_pairs_empty(tmp_path):
    path = tmp_path / "baseline.csv"
    pd.DataFrame([{"ticker": "AAA", "concept": "buybacks", "last_period_end": "2023-12-31",
                   "revenues_period_end": "2026-06-30", "lag_days": 912,
                   "reason": "в/спряло"}]).to_csv(path, index=False)
    rows = _full("AAA", "2026-06-30", override={"buybacks": "2023-12-31", "capex": "2025-09-30"})
    assert validate_gates.write_staleness_baseline(_panel(rows), path) == 2
    got = validate_gates.load_staleness_baseline(path).set_index("concept")["reason"].to_dict()
    assert got == {"buybacks": "в/спряло", "capex": ""}


def test_proxy_statement_facts_never_enter():
    # TKO shape: the DEF 14A pay-versus-performance table re-tags FY net income with
    # the consolidated figure, filed after the 10-K.
    tenk = _f("2025-12-31", 195.0, "2026-02-25", start="2025-01-01", form="10-K")
    proxy = _f("2025-12-31", 546.0, "2026-04-23", start="2025-01-01", form="DEF 14A")
    recs, _ = build_panel._extract_net_income(_facts(NetIncomeLoss=[tenk, proxy]))
    assert [r["val"] for r in recs] == [195.0]
