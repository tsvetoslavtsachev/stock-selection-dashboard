"""
Point-in-time (PIT) accessor for the EDGAR fundamental panel.

The panel (edgar_pit_panel.csv.gz) is the immutable evidence: one row per raw fact
filing, each stamped with the date it became public (``filed``). This module answers
the backtest question "what was KNOWN about company X on date D?" with NO lookahead:
only rows with ``filed <= D`` are ever consulted.

Two kinds of concept
--------------------
* STOCK (balance-sheet snapshot: equity, assets, cash, debt, current liabilities,
  shares) — a level at an instant. as_known_at picks, per company, the fact with
  the latest ``period_end`` among those filed on/before D; ties broken by latest
  ``filed`` (a restatement supersedes the original for the same instant).

* FLOW (income / cash-flow: revenues, net_income, operating_income, gross_profit,
  D&A, operating_cash_flow, capex, buybacks, dividends_paid) — accumulates over a
  period. We report the TTM (trailing twelve months) = the sum of the last four
  DISTINCT quarters known at D.

The quarter problem (the delicate part)
---------------------------------------
XBRL flow facts come at mixed durations because filers report cumulative
year-to-date figures alongside (sometimes) the discrete quarter:

    ~90 days   -> a discrete fiscal quarter          (use directly)
    ~180 days  -> H1 year-to-date  (Q1+Q2)
    ~270 days  -> 9-month year-to-date (Q1+Q2+Q3)
    ~360 days  -> full year (FY, from the 10-K)      (= Q1+Q2+Q3+Q4)

To get clean quarters we:
  1. Take, per fiscal year, the best-known figure at each cumulative horizon
     (discrete-Q if present; else the YTD level).
  2. DIFFERENCE consecutive cumulative levels within a fiscal year to recover the
     discrete quarter: Q2 = H1 - Q1, Q3 = 9M - H1, Q4 = FY - 9M. When a discrete
     ~90-day fact is directly present we prefer it over the differenced value
     (fewer rounding artifacts), but the FY-minus-9M path is the only way to get Q4.
  3. TTM = sum of the four most recent distinct quarters (by fiscal period_end),
     requiring all four to be present (else NaN — an honest gap, not a partial-year
     figure masquerading as annual).

Fiscal-year grouping uses each fact's ``fy`` field (present on every EDGAR fact),
which is robust to off-calendar fiscal years (AAPL's September year-end etc.).
"""

from __future__ import annotations

import logging
from datetime import date, datetime

import numpy as np
import pandas as pd

logger = logging.getLogger("pit")

# Concept taxonomy ----------------------------------------------------------------
STOCK_CONCEPTS = frozenset({
    "stockholders_equity",
    "total_assets",
    "current_liabilities",
    "cash_and_equivalents",
    "total_debt",
    "shares_outstanding",
})

FLOW_CONCEPTS = frozenset({
    "revenues",
    "gross_profit",
    "operating_income",
    "net_income",
    "depreciation_amortization",
    "operating_cash_flow",
    "capex",
    "buybacks",
    "dividends_paid",
})

ALL_CONCEPTS = STOCK_CONCEPTS | FLOW_CONCEPTS

# Duration classification (days). A fiscal quarter is ~91d; filers vary (13 vs 14
# week quarters), so we bucket by nearest cumulative horizon rather than exact.
_Q_DAYS = 91
_TOL = 25          # +/- window around each horizon
_HORIZONS = {1: 91, 2: 182, 3: 273, 4: 365}   # n-quarters-cumulative -> nominal days


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------
def load_panel(path) -> pd.DataFrame:
    """Load the gzipped panel with correct dtypes (dates parsed, value numeric)."""
    df = pd.read_csv(
        path,
        dtype={"ticker": str, "concept": str, "unit": str, "form": str, "fp": str},
        parse_dates=["period_start", "period_end", "filed"],
    )
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["fy"] = pd.to_numeric(df["fy"], errors="coerce")
    return df


def _as_date(d) -> date:
    if isinstance(d, date) and not isinstance(d, datetime):
        return d
    if isinstance(d, datetime):
        return d.date()
    return pd.Timestamp(d).date()


# ---------------------------------------------------------------------------
# STOCK concepts
# ---------------------------------------------------------------------------
def _latest_stock_value(g: pd.DataFrame) -> float:
    """Given all rows for one (ticker, stock-concept) already filtered to filed<=D,
    return the value at the latest period_end; ties -> latest filed (restatement
    wins for the same instant)."""
    if g.empty:
        return np.nan
    g = g.sort_values(["period_end", "filed"])
    return float(g.iloc[-1]["value"])


# ---------------------------------------------------------------------------
# FLOW concepts -> quarters -> TTM
# ---------------------------------------------------------------------------
def _n_quarters(period_start, period_end) -> int | None:
    """Classify a flow fact's duration into 1..4 cumulative quarters, or None if it
    matches no horizon (e.g. a stray partial period)."""
    if pd.isna(period_start) or pd.isna(period_end):
        return None
    days = (_as_date(period_end) - _as_date(period_start)).days
    for n, nominal in _HORIZONS.items():
        if abs(days - nominal) <= _TOL:
            return n
    return None


def _best_by(records: list[dict], key) -> dict:
    """Collapse duplicate facts (restatements / re-filings of the same period) to the
    single value known 'latest': latest filed wins, then latest period_end. Keyed by
    ``key(record)``; returns {key: {'value','period_end','filed'}}."""
    out: dict = {}
    for r in records:
        k = key(r)
        prev = out.get(k)
        cand = (r["filed"], r["period_end"])
        if prev is None or cand >= (prev["filed"], prev["period_end"]):
            out[k] = {"value": r["value"], "period_end": r["period_end"], "filed": r["filed"]}
    return out


def _quarters_for_flow(g: pd.DataFrame) -> pd.DataFrame:
    """Reduce all known flow facts for one (ticker, concept) to a table of DISCRETE
    quarters: columns [fy, q_index, period_end, value].

    Two filing shapes coexist and are reconciled:

    * A DISCRETE quarter (~90d) is a standalone ~13-week window; its ``period_end``
      is the quarter's own close. These are used directly, keyed by period_end.

    * CUMULATIVE facts (~180/270/365d) share the fiscal-year START and differ only
      in ``period_end`` (H1, 9M, FY). A quarter is recovered by differencing a
      cumulative level against the previous cumulative level in the SAME fiscal year
      (Q2 = H1 - Q1cum..., Q4 = FY - 9M). Cumulative differencing is the ONLY route
      to Q4 (the 10-K reports FY, never a discrete Q4).

    Where BOTH exist for the same quarter-end, the directly-tagged discrete value is
    preferred (it is the filer's own quarter figure, no rounding from differencing).
    """
    if g.empty:
        return pd.DataFrame(columns=["fy", "q_index", "period_end", "value"])

    g = g.copy()
    g["ndur"] = [
        _n_quarters(ps, pe) for ps, pe in zip(g["period_start"], g["period_end"])
    ]
    g = g[g["ndur"].notna() & g["value"].notna() & g["fy"].notna()]
    if g.empty:
        return pd.DataFrame(columns=["fy", "q_index", "period_end", "value"])
    g["ndur"] = g["ndur"].astype(int)

    recs = g.to_dict("records")

    # --- discrete quarters (~90d): keyed by period_end, deduped to latest-known ---
    discrete = _best_by(
        [r for r in recs if r["ndur"] == 1],
        key=lambda r: r["period_end"],
    )

    # --- cumulative levels (~180/270/365d) grouped per fiscal year, deduped ---
    # Key by (fy, ndur) so a re-filed H1 collapses to its latest-known value. Within
    # a fiscal year we then have an ordered ladder of cumulative period_ends.
    cum = _best_by(
        [r for r in recs if r["ndur"] >= 2],
        key=lambda r: (r["fy"], r["ndur"]),
    )
    # Also treat a discrete Q1 as the 1-quarter cumulative level (they coincide: the
    # first quarter's cumulative == the first quarter itself), so the ladder can
    # start differencing from Q1 even when only discrete-Q1 + cumulative-H1 exist.
    for pe, d in discrete.items():
        fy_match = [r for r in recs if r["ndur"] == 1 and r["period_end"] == pe]
        if fy_match:
            fy = fy_match[0]["fy"]
            cum.setdefault((fy, 1), {"value": d["value"], "period_end": pe, "filed": d["filed"]})

    quarters: dict[pd.Timestamp, dict] = {}  # period_end -> {fy, q_index, value}

    # 1) discrete quarters go in directly.
    for pe, d in discrete.items():
        quarters[pe] = {"fy": None, "q_index": None, "value": d["value"], "period_end": pe}

    # 2) cumulative differencing per fiscal year fills the rest (esp. Q4).
    fys = sorted({k[0] for k in cum})
    for fy in fys:
        ladder = sorted(
            [(n, cum[(fy, n)]) for n in range(1, 5) if (fy, n) in cum],
            key=lambda t: t[0],
        )
        prev_val = 0.0
        prev_n = 0
        for n, lvl in ladder:
            pe = lvl["period_end"]
            if n - prev_n == 1:
                q_val = lvl["value"] - prev_val
            else:
                # Non-contiguous ladder (missing an intermediate horizon): cannot
                # difference this step cleanly. Skip deriving THIS quarter, but keep
                # the level as the new baseline for later contiguous steps.
                prev_val = lvl["value"]
                prev_n = n
                continue
            # Prefer a directly-tagged discrete quarter landing on this end.
            if pe not in quarters:
                quarters[pe] = {"fy": fy, "q_index": n, "value": q_val, "period_end": pe}
            else:
                # already have discrete; just annotate fy/q_index if missing.
                if quarters[pe]["fy"] is None:
                    quarters[pe]["fy"] = fy
                    quarters[pe]["q_index"] = n
            prev_val = lvl["value"]
            prev_n = n

    out = pd.DataFrame(list(quarters.values()))
    if out.empty:
        return pd.DataFrame(columns=["fy", "q_index", "period_end", "value"])
    return out.sort_values("period_end").reset_index(drop=True)[["fy", "q_index", "period_end", "value"]]


def _ttm_from_quarters(qdf: pd.DataFrame) -> float:
    """Sum the last four DISTINCT quarters (by period_end). Requires exactly four
    distinct quarter end-dates; fewer -> NaN (no partial-year TTM)."""
    if qdf.empty:
        return np.nan
    # Distinct quarters keyed by period_end; if duplicates, take the last.
    q = qdf.drop_duplicates(subset=["period_end"], keep="last")
    q = q.sort_values("period_end")
    if len(q) < 4:
        return np.nan
    last4 = q.tail(4)
    return float(last4["value"].sum())


# ---------------------------------------------------------------------------
# Public accessor
# ---------------------------------------------------------------------------
def as_known_at(panel: pd.DataFrame, as_of) -> pd.DataFrame:
    """Return a wide DataFrame (index=ticker) of every concept's value KNOWN at
    ``as_of`` with no lookahead:

      * stock concepts   -> the level at the latest period_end filed on/before D;
      * flow concepts    -> a ``<concept>_ttm`` column = sum of the last 4 distinct
                            known quarters (and the raw concept column left NaN).

    Only rows with ``filed <= as_of`` are consulted.
    """
    as_of_ts = pd.Timestamp(_as_date(as_of))
    known = panel[panel["filed"] <= as_of_ts]

    tickers = sorted(known["ticker"].dropna().unique())
    result: dict[str, dict] = {t: {} for t in tickers}

    # STOCK concepts.
    for concept in STOCK_CONCEPTS:
        sub = known[known["concept"] == concept]
        for ticker, g in sub.groupby("ticker"):
            result[ticker][concept] = _latest_stock_value(g)

    # FLOW concepts -> TTM.
    for concept in FLOW_CONCEPTS:
        sub = known[known["concept"] == concept]
        for ticker, g in sub.groupby("ticker"):
            qdf = _quarters_for_flow(g)
            result[ticker][f"{concept}_ttm"] = _ttm_from_quarters(qdf)

    wide = pd.DataFrame.from_dict(result, orient="index")
    wide.index.name = "ticker"

    # Stable column order: stock concepts, then <flow>_ttm.
    cols = [c for c in sorted(STOCK_CONCEPTS) if c in wide.columns]
    cols += [f"{c}_ttm" for c in sorted(FLOW_CONCEPTS) if f"{c}_ttm" in wide.columns]
    wide = wide.reindex(columns=cols)
    return wide.sort_index()


def as_known_at_ticker(panel: pd.DataFrame, ticker: str, as_of) -> dict:
    """Convenience: the concept dict for a single ticker (used in tests/spot checks)."""
    one = panel[panel["ticker"] == ticker]
    wide = as_known_at(one, as_of)
    if ticker not in wide.index:
        return {}
    return wide.loc[ticker].to_dict()
