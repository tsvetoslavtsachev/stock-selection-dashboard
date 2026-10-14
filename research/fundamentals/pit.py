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
  3. TTM = the latest twelve months that contiguous pieces tile with no hole (a
     piece is a quarter, or a multi-quarter block such as H1 when Q1 was never
     tagged), ending at the concept's latest quarter or one quarter before it; else
     NaN (an honest gap, not a partial-year or years-old figure masquerading as
     the trailing twelve months). See ``ttm_window``.

The fiscal year of a fact is its PERIOD START (Q1, H1, 9M and FY of one year share
it), never the filing's ``fy`` stamp: SEC stamps comparative periods with the new
filing's fy, so grouping by fy paired FY25 with 9M FY26 (WDAY) and skipped quarters
(ПНЛ7, 14.10.2026).
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

# Periodic reports only (an 8-K earnings release is not the statement); a 12-month
# fact only from an annual form (prefix match: 10-K, 10-K/A, 10-KT, 20-F, 20-F/A).
_PERIODIC_FORMS = frozenset({"10-Q", "10-Q/A", "10-QT", "10-K", "10-K/A", "10-KT", "20-F", "20-F/A"})
_ANNUAL_PREFIXES = ("10-K", "20-F")
_DAY = pd.Timedelta(days=1)
_JOIN_TOL = 7          # days: two pieces join if one starts within a week of the other's end
# A TTM whose window ends more than this many days before the ticker's latest
# revenues period_end is NaN (100 = the G6 threshold). None = off: Ц. 14.10.2026
# (ПНЛ7, СТОП А) - off until the screen shows an unknown dividend as unknown, not
# 0% (reverse.dividend_flat), and the P/E history rule is decided.
_STALE_TTM_DAYS = None
# When the latest quarter of the concept itself cannot be tiled, the window may end
# at most this many days earlier (one quarter back); further back is NaN, not a
# years-old sum (EIX net income: no FY2021/FY2022 -> the 12 months to 2021-09-30
# would stand in for 2022-2026). None = fall back without limit.
_FALLBACK_DAYS = 100


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


def _flow_records(g: pd.DataFrame) -> list[dict]:
    """The usable flow facts of one (ticker, concept), each tagged with ``ndur``.

    * Only periodic reports (10-Q / 10-K / 20-F and their /A, T variants): an 8-K
      earnings release is not the filed statement.
    * A ~12-month fact only from an ANNUAL form. 10-Qs carry stray 12-month facts
      (FIX 10-Q of 2026-04-23: "2025-01-01..2025-12-31 = 1.83bn", the Q1 figure
      under a full-year context); latest-filed-wins would let it beat the 10-K.
    * No ``fy`` filter: the fiscal year is read from the period itself (see
      ``_segments``), never from the filing's ``fy`` stamp."""
    if g.empty:
        return []
    g = g[g["period_start"].notna() & g["period_end"].notna() & g["value"].notna()]
    if "form" in g.columns:
        g = g[g["form"].isin(_PERIODIC_FORMS)]
    if g.empty:
        return []
    g = g.copy()
    g["ndur"] = [_n_quarters(ps, pe) for ps, pe in zip(g["period_start"], g["period_end"])]
    g = g[g["ndur"].notna()]
    g["ndur"] = g["ndur"].astype(int)
    if "form" in g.columns:
        annual = g["form"].str.startswith(_ANNUAL_PREFIXES, na=False)
        g = g[(g["ndur"] < 4) | annual]
    return g.to_dict("records")


def _segments(recs: list[dict]) -> list[dict]:
    """Contiguous pieces of the flow, each covering 1..4 fiscal quarters:
    {begin, end, nq, value, kind, filed, fy, q_index}.

    * kind "q": a discrete ~90-day fact, taken as filed (latest filed per period).
    * kind "d": a difference of two cumulative levels of ONE fiscal year. A fiscal
      year is identified by its START (``period_start``): Q1, H1, 9M and FY of a year
      share it, whatever ``fy`` each filing stamps on them (SEC stamps a comparative
      period with the NEW filing's fy -> WDAY's 9M FY26 and FY25 both under fy 2025).
      Adjacent known levels m < n give a piece of n - m quarters (H1 alone after the
      year start = 2 quarters; FY - 9M = Q4). The lower level is taken as known when
      the upper one was filed (same vintage): after a divestiture the restated 9M
      minus the old 10-K FY is no quarter at all (BDX FY25: 21.84bn - 13.54bn)."""
    segs: list[dict] = []
    for (ps, pe), d in _best_by(
        [r for r in recs if r["ndur"] == 1], key=lambda r: (r["period_start"], r["period_end"])
    ).items():
        segs.append({"begin": ps, "end": pe, "nq": 1, "value": d["value"],
                     "kind": "q", "filed": d["filed"], "fy": None, "q_index": None})

    cum = _best_by(recs, key=lambda r: (r["period_start"], r["ndur"]))
    hist: dict = {}
    for r in recs:
        hist.setdefault((r["period_start"], r["ndur"]), []).append(r)
    for ps in sorted({k[0] for k in cum}):
        levels = [n for n in range(1, 5) if (ps, n) in cum]
        if levels == [1]:
            continue  # a lone ~90d level is just the discrete quarter, already above
        if (ps, 4) in cum:
            label = cum[(ps, 4)]["period_end"].year
        else:
            label = (ps + pd.Timedelta(days=364)).year
        prev_n, prev_end = 0, ps - _DAY
        for n in levels:
            top = cum[(ps, n)]
            if prev_n == 0:
                base = 0.0
            else:
                older = [r for r in hist[(ps, prev_n)] if r["filed"] <= top["filed"]]
                base = (_best_by(older, key=lambda r: 0)[0] if older else cum[(ps, prev_n)])["value"]
            segs.append({"begin": prev_end + _DAY, "end": top["period_end"], "nq": n - prev_n,
                         "value": top["value"] - base, "kind": "d", "filed": top["filed"],
                         "fy": label, "q_index": n})
            prev_n, prev_end = n, top["period_end"]
    return segs


def _chain(segs: list[dict], end, need: int) -> list[dict] | None:
    """Pieces that tile exactly ``need`` quarters back from ``end`` with no hole (each
    piece starts the day after the previous one ends, +/- _JOIN_TOL days). Finest
    pieces first, a filed discrete quarter before a derived one."""
    if need == 0:
        return []
    cands = [s for s in segs if s["nq"] <= need and abs((s["end"] - end).days) <= _JOIN_TOL]
    cands.sort(key=lambda s: (s["nq"], s["kind"] != "q", -pd.Timestamp(s["filed"]).value))
    for s in cands:
        rest = _chain(segs, s["begin"] - _DAY, need - s["nq"])
        if rest is not None:
            return rest + [s]
    return None


def _quarters_for_flow(g: pd.DataFrame) -> pd.DataFrame:
    """All known flow facts for one (ticker, concept) -> a table of DISCRETE quarters:
    columns [fy, q_index, period_end, value].

    A quarter is either filed directly (~90d) or the difference of two adjacent
    cumulative levels of the same fiscal year (Q2 = H1 - Q1, Q3 = 9M - H1,
    Q4 = FY - 9M; see ``_segments``). Where both exist for the same quarter-end the
    filed one wins. ``fy`` is the calendar year of the fiscal year's end."""
    segs = [s for s in _segments(_flow_records(g)) if s["nq"] == 1]
    if not segs:
        return pd.DataFrame(columns=["fy", "q_index", "period_end", "value"])
    labels = {s["end"]: (s["fy"], s["q_index"]) for s in segs if s["kind"] == "d"}
    starts = sorted({s["begin"] for s in segs if s["kind"] == "d" and s["q_index"] == 1})
    quarters: dict = {}
    for s in sorted(segs, key=lambda s: (s["kind"] == "q", s["filed"])):
        pe = s["end"]
        fy, qi = labels.get(pe, (None, None))
        if fy is None:
            own = [st for st in starts if st <= s["begin"] and (pe - st).days <= 380]
            st = own[0] if own else s["begin"]   # no ladder yet: a lone Q1 of a new year
            qi = min(4, max(1, round((pe - st).days / 91.3)))
            fy = (st + pd.Timedelta(days=364)).year
        quarters[pe] = {"fy": fy, "q_index": qi, "period_end": pe, "value": s["value"]}
    out = pd.DataFrame(list(quarters.values()))
    return out.sort_values("period_end").reset_index(drop=True)[["fy", "q_index", "period_end", "value"]]


def ttm_window(g: pd.DataFrame, stale_ref=None) -> dict:
    """TTM of one (ticker, concept) from the facts in ``g`` (already filtered to
    filed <= D): the latest twelve months that the facts tile WITHOUT a hole.

    Returns {"value", "start", "end", "q_ends", "stale"}; value NaN when no four contiguous
    quarters exist, or when ``stale_ref`` (the ticker's latest revenues period_end)
    is given and the window ends more than _STALE_TTM_DAYS before it: a sum of
    quarters from years ago is not a trailing twelve months (INTC D&A from 2019)."""
    empty = {"value": np.nan, "start": None, "end": None, "q_ends": [], "stale": False}
    segs = _segments(_flow_records(g))
    if not segs:
        return empty
    last = max(s["end"] for s in segs)
    for end in sorted({s["end"] for s in segs}, reverse=True):
        if _FALLBACK_DAYS is not None and (last - end).days > _FALLBACK_DAYS:
            break
        ch = _chain(segs, end, 4)
        if ch is None:
            continue
        if (stale_ref is not None and _STALE_TTM_DAYS is not None and not pd.isna(stale_ref)
                and (pd.Timestamp(stale_ref) - end).days > _STALE_TTM_DAYS):
            return {**empty, "end": end, "stale": True}
        return {"value": float(sum(s["value"] for s in ch)), "start": ch[0]["begin"],
                "end": end, "q_ends": [s["end"] for s in ch], "stale": False}
    return empty


def _ttm_from_quarters(qdf: pd.DataFrame) -> float:
    """Sum the last four DISTINCT quarters (by period_end). Requires exactly four
    distinct quarter end-dates; fewer -> NaN (no partial-year TTM). Kept for callers
    that hold a quarter table; ``as_known_at`` uses ``ttm_window``."""
    if qdf.empty:
        return np.nan
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

    # FLOW concepts -> TTM (a window stale against revenues is NaN).
    rev_end = known[known["concept"] == "revenues"].groupby("ticker")["period_end"].max()
    for concept in FLOW_CONCEPTS:
        sub = known[known["concept"] == concept]
        for ticker, g in sub.groupby("ticker"):
            w = ttm_window(g, stale_ref=rev_end.get(ticker))
            result[ticker][f"{concept}_ttm"] = w["value"]

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
