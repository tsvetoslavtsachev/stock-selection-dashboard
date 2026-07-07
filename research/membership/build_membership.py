# -*- coding: utf-8 -*-
"""
Build the S&P 500 point-in-time membership file for the backtest's
pre-inclusion filter.

Source: the saved Wikipedia "List of S&P 500 companies" page
(raw/sp500_wikipedia_2026-07-06.html holds the dated snapshot for
reproducibility).

Primary source for `added_date` is the "Selected changes" table
(raw/raw_changes_table.csv), matched against each universe.csv ticker's
most recent "Added Ticker" event. The constituents table's own "Date added"
column (raw/raw_constituents_table.csv) is used only as an independent
cross-check for specific special cases (renames, dual-class siblings) —
see NOTES.md for full methodology and every special-case resolution.

Output: membership.csv with columns ticker,added_date,source_note
  - ticker: the dotted universe.csv form (e.g. BRK.B)
  - added_date: YYYY-MM-DD, or empty if the ticker never appears as an
    Added Ticker in the changes table (source_note = "not-in-changes")
  - source_note: provenance, e.g. "wiki-changes 2024-03-18"

Usage:
    python research/membership/build_membership.py
"""
import re

import pandas as pd

RAW_DIR = r"C:\Projects\dashboards\stock-selection-dashboard\research\membership\raw"
UNIVERSE_PATH = r"C:\Projects\dashboards\stock-selection-dashboard\config\universe.csv"
OUT_MEMBERSHIP = r"C:\Projects\dashboards\stock-selection-dashboard\research\membership\membership.csv"

universe = pd.read_csv(UNIVERSE_PATH)
assert universe.shape[0] == 503, f"Expected 503 rows, got {universe.shape[0]}"

changes = pd.read_csv(f"{RAW_DIR}\\raw_changes_table.csv")
changes["Date_parsed"] = pd.to_datetime(changes["Date"], errors="coerce")


# ---------- Normalization helpers ----------
def norm_ticker(t):
    """Normalize a ticker for matching: uppercase, strip, unify separators."""
    if pd.isna(t):
        return None
    return str(t).strip().upper().replace(" ", "")


def ticker_variants(t):
    """Generate variant forms of a ticker (dot/hyphen/slash interchangeable)."""
    if t is None:
        return set()
    variants = {t}
    for sep_from in [".", "-", "/"]:
        for sep_to in [".", "-", "/"]:
            variants.add(t.replace(sep_from, sep_to))
    return variants


def norm_company_name(name):
    """Normalize a company name for fuzzy matching."""
    if pd.isna(name):
        return ""
    s = str(name).lower()
    for junk in [
        "incorporated", "inc.", "inc", "corporation", "corp.", "corp",
        "company", "co.", "co", "ltd.", "ltd", "l.p.", "lp", "plc",
        "holdings", "holding", "group", "the ", " the",
        "class a", "class b", "class c", "class d",
        "'a'", "'b'", "'c'",
    ]:
        s = s.replace(junk, "")
    s = re.sub(r"[^\w\s]", "", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


changes["AddedTicker_norm"] = changes["Added Ticker"].apply(norm_ticker)
changes["AddedSecurity_norm"] = changes["Added Security"].apply(norm_company_name)
universe["ticker_norm"] = universe["symbol"].apply(norm_ticker)
universe["name_norm"] = universe["name"].apply(norm_company_name)

# ---------- Matching ----------
results = []
for _, row in universe.iterrows():
    ticker = row["symbol"]
    variants = ticker_variants(row["ticker_norm"])

    direct_matches = changes[changes["AddedTicker_norm"].isin(variants)]
    matched_date, source_note = None, None

    if not direct_matches.empty:
        best = direct_matches.loc[direct_matches["Date_parsed"].idxmax()]
        matched_date = best["Date_parsed"]
    else:
        name_matches = changes[changes["AddedSecurity_norm"] == row["name_norm"]]
        if not name_matches.empty:
            best = name_matches.loc[name_matches["Date_parsed"].idxmax()]
            matched_date = best["Date_parsed"]

    if matched_date is not None:
        results.append({
            "ticker": ticker,
            "added_date": matched_date.strftime("%Y-%m-%d"),
            "source_note": f"wiki-changes {matched_date.strftime('%Y-%m-%d')}",
        })
    else:
        results.append({"ticker": ticker, "added_date": "", "source_note": "not-in-changes"})

membership = pd.DataFrame(results)

# ---------- Manual overrides (Trap 1 rename + Trap 3 dual-class propagation) ----------
# See NOTES.md "Trap 1" and "Trap 3" sections for full reasoning + cross-validation.
manual_overrides = {
    "META": {  # FB -> META rename, same CIK 1326801
        "added_date": "2013-12-23",
        "source_note": "wiki-changes 2013-12-23 (rename FB->META)",
    },
    "GOOG": {  # dual-class sibling of GOOGL; only GOOGL has its own Added Ticker row
        "added_date": "2014-04-03",
        "source_note": "wiki-changes 2014-04-03 (dual-class sibling of GOOGL)",
    },
}
for ticker, override in manual_overrides.items():
    membership.loc[membership["ticker"] == ticker, "added_date"] = override["added_date"]
    membership.loc[membership["ticker"] == ticker, "source_note"] = override["source_note"]

membership.to_csv(OUT_MEMBERSHIP, index=False)
print(f"Saved {OUT_MEMBERSHIP} with {membership.shape[0]} rows")
print(f"Non-empty added_date: {(membership['added_date'] != '').sum()}")
print(f"Empty (not-in-changes): {(membership['added_date'] == '').sum()}")
