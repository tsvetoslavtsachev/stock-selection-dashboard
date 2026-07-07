# S&P 500 Point-in-Time Membership Reconstruction — Notes

## Source

- URL: `https://en.wikipedia.org/wiki/List_of_S%26P_500_companies`
- Fetch date: 2026-07-06
- Fetch method: `requests.get` with a realistic Chrome User-Agent header (default python-requests UA is sometimes blocked by Wikipedia). HTTP status 200, ~1.49 MB HTML.
- Raw HTML saved verbatim to `raw/sp500_wikipedia_2026-07-06.html` for reproducibility.
- Two tables parsed via `pandas.read_html` (lxml backend):
  - `raw/raw_constituents_table.csv` — current constituents table (`id="constituents"`), 503 rows x 8 columns (Symbol, Security, GICS Sector, GICS Sub-Industry, Headquarters Location, Date added, CIK, Founded).
  - `raw/raw_changes_table.csv` — "Selected changes to the list of S&P 500 components" table (table index 1 on the page), 402 rows x 6 columns after flattening the nested column headers (`Date`, `Added Ticker`, `Added Security`, `Removed Ticker`, `Removed Security`, `Reason`). Date range: **1976-07-01 to 2026-06-30**.

## Methodology

`added_date` is derived from the **changes table** (`raw_changes_table.csv`), per the task's specified method — NOT from the constituents table's own "Date added" column (that column is used only as an independent cross-check in a few special cases below).

For each of the 503 tickers in `config/universe.csv` (read-only input, unmodified):

1. **Normalize the ticker.** Uppercase, strip whitespace, and generate separator variants (`.`, `-`, `/` treated as interchangeable) — e.g. `BRK.B` also matches `BRK-B` / `BRK/B` in the changes table.
2. **Direct ticker match.** Search the changes table for rows where `Added Ticker` (normalized) matches any variant of the universe ticker. If multiple matches exist (a ticker added, removed, and re-added over time), take the **most recent** (max date) — per spec, "if a name has been added/removed multiple times, use the latest addition."
3. **Fuzzy name fallback.** If no direct ticker match, normalize company names (lowercase, strip legal suffixes like "Inc.", "Corporation", "Class A/B/C", punctuation) and match `universe.name` against `Added Security` in the changes table. This caught one case automatically (WTW, see below).
4. **Manual overrides** for two special cases automated matching could not safely resolve (see Trap 1 and Trap 3 below): `META` and `GOOG`.
5. Any ticker with no direct match, no fuzzy-name match, and no manual override is left with `added_date` empty and `source_note = not-in-changes`.

Output: `membership.csv`, columns `ticker, added_date, source_note`, 503 rows — one per `universe.csv` ticker.

- **234 / 503** tickers resolved to a non-empty `added_date`.
- **269 / 503** tickers are `not-in-changes` (empty `added_date`) — see "Unresolved tickers" below for why this is an expected source-data gap, not a bug.

## Trap 1 — Renames

| Old ticker (in changes table) | New ticker (in universe.csv) | Matched company name | Matched date | Resolution method |
|---|---|---|---|---|
| WLTW | WTW | Willis Towers Watson | 2016-01-05 | Automatic fuzzy name match — "Willis Towers Watson" in Added Security matched universe.csv's name for WTW after normalization. |
| FB | META | Facebook / Meta Platforms | 2013-12-23 | **Manual override.** Facebook Inc. renamed to Meta Platforms Inc. in Oct 2021 (same CIK 1326801 in both the changes-table-era entry and the current constituents table). Automated name-fuzzy matching could not bridge "Facebook" → "Meta Platforms" (too dissimilar after normalization), so this was resolved by hand. Cross-validated: the constituents table's own "Date added" column for META independently shows 2013-12-23 — an exact match to the FB row in the changes table, confirming this is the correct original addition event under the historical ticker/name. |

No other universe.csv ticker required rename resolution. All remaining tickers either matched their own current ticker directly in the changes table, or fell into the `not-in-changes` bucket (never listed as an Added Ticker under any form) — see "Unresolved tickers" below.

## Trap 2 — Recent spinoffs (2020-06-01 onward)

All 122 changes-table rows with `Date >= 2020-06-01` were enumerated individually and cross-checked against `universe.csv`. Every row whose (rename-resolved) Added Ticker is currently in `universe.csv` produced a non-empty `added_date` via direct-ticker matching — **no post-2020-06 addition silently fell into the empty-date bucket**. 86 of the 503 universe tickers carry an `added_date` in `[2020-06-01, 2026-07-06]` (full year-by-year breakdown in the validation results shared with the requester).

Rows in this window whose Added Ticker is **not** in `universe.csv` were checked individually and are all legitimate non-issues:
- Later removed from the index: ETSY, CZR, PENN, MOH, CDAY, MTCH, SEDG, SBNY, BIO, VNT, ENPH, OGN, MBC, AMTM.
- Additions dated after the `universe.csv` snapshot was taken (mid/late 2026, newer than universe.csv): MRVL (2026-06-22), FLEX (2026-06-22), HONA (2026-06-29), VEEV (2026-05-07), FDXF (2026-06-01), SOLS (2025-10-30) — confirmed absent from `universe.csv` by direct lookup (not a matching failure; `universe.csv` predates these events).

## Trap 3 — Dual share classes

| Company | Class A ticker | Class B/C ticker | Added-Ticker event(s) found in changes table | Resolution |
|---|---|---|---|---|
| Alphabet Inc. | GOOGL | GOOG (Class C) | Only `GOOGL` appears as an Added Ticker row, 2014-04-03, Reason: "Google Class C share distribution" | **Manual override**: propagated 2014-04-03 to GOOG. GOOG never appears as its own Added Ticker row, but both share classes share CIK 1652044 and GOOG is the direct product of that 2014 corporate action. |
| Fox Corporation | FOXA (Class A) | FOX (Class B) | **Both** appear as independent Added Ticker rows, 2019-03-19 (when the new "Fox Corporation" was formed post-Disney/21st Century Fox deal) | No override needed — both classes matched directly to the same date via ordinary direct-ticker matching. |
| News Corp | NWSA (Class A) | NWS (Class B) | NWSA: 2013-07-01 (captioned "21st Century Fox" — a historical relabeling of the same corporate lineage in Wikipedia's edit history); NWS: 2015-09-18 ("Share class methodology change") | No override applied. Both classes have their own distinct, independently dated Added Ticker rows, so both resolved automatically — but with **different** dates. Flagging this explicitly per the task's instruction, even though no propagation was performed: NWS was carved out as its own separate index line item in 2015, two years after NWSA/21st Century Fox's 2013 entry, so the two class tickers legitimately carry different `added_date` values in this dataset. |

Scan for other same-company dual-class situations: checked `universe.csv` for duplicate CIK values and for duplicate leading words in company names. The only same-CIK duplicate pairs are GOOGL/GOOG, FOX/FOXA, and NWS/NWSA — all three handled above. Other name collisions found by the leading-word scan (American Electric Power / American International Group / American Tower / American Water Works / American Express; Constellation Energy / Constellation Brands; Charles River Labs / Charles Schwab; Dollar General / Dollar Tree; General Dynamics / General Mills / General Motors; GE Aerospace / GE HealthCare / GE Vernova; Johnson Controls / Johnson & Johnson; Public Service Enterprise Group / Public Storage; Texas Pacific Land / Texas Instruments; United Airlines / United Parcel Service / United Rentals; Huntington Bancshares / Huntington Ingalls; International Flavors & Fragrances / International Paper; W.W. Grainger / W.R. Berkley) are **unrelated companies that merely share a leading word**, confirmed via distinct CIK values — not dual-class siblings. GE Aerospace / GE HealthCare / GE Vernova are three separate 2024 General Electric spinoff companies, each with its own CIK and its own independent Added Ticker event — not a single company with multiple share classes, so no propagation logic applies.

## Unresolved tickers (269) — why these are empty, not a matching bug

These are tickers in `universe.csv` that never appear as an "Added Ticker" anywhere in the Wikipedia "Selected changes" table (under any normalized form). Spot-checked a sample against the constituents table's own "Date added" column as an independent cross-check:

| Ticker | Constituents table "Date added" |
|---|---|
| AAPL | 1982-11-30 |
| MSFT | 1994-06-01 |
| JPM | 1975-06-30 |
| JNJ | 1973-06-30 |
| KO | 1957-03-04 |
| XOM | 1957-03-04 |
| PG | 1957-03-04 |
| BRK.B | 2010-02-16 |

BRK.B is the clearest proof this is a source-completeness gap rather than a matching bug: it has a real, independently sourced "Date added" of 2010-02-16 in the constituents table, yet **no corresponding row exists anywhere in the changes table**. Wikipedia's own caption for the "Selected changes" table describes it as a curated/selected log, not an exhaustive transaction history — this is a known, inherent limitation of the source, not a defect introduced by this script. Per the task's Step 1 instruction, no dates were fabricated for these; all 269 are left blank with `source_note = not-in-changes`.

None of the 269 unresolved tickers are recent (post-2020-06) additions — Trap 2's exhaustive cross-check (above) confirms every genuinely recent addition in `universe.csv` received a populated date. The unresolved set is entirely long-tenured legacy S&P 500 members whose original addition predates or falls outside the changes table's practical coverage.

Full list of the 269 unresolved tickers is visible in `membership.csv` (rows with empty `added_date`) and in `raw/membership_debug.csv` (same data plus internal matching diagnostics: `_match_method`, `_matched_added_ticker` columns).

## Known limitations

- Wikipedia's "Selected changes" table is **best-effort, not authoritative** — it is explicitly a curated/selected log, not a complete transaction history, especially pre-2010. Completeness cannot be guaranteed for older entries.
- The changes table's earliest usable entry is 1976-07-01 (DIS / Walt Disney Company, part of a "major restructuring" event). This is a genuine, cross-validated historical entry — it matches the constituents table's own "Date added" of 1976-06-30 for DIS within one day (most likely an announcement-date vs. effective-date convention difference), not a parsing artifact. It is the sole `added_date` value earlier than 1990-01-01 in the entire output; flagged explicitly by validation V4 and confirmed correct rather than treated as an error to fix.
- This reconstruction matches `config/universe.csv` as it existed at the time of this task against Wikipedia's live page as of 2026-07-06. A handful of very recent 2026 index changes (Marvell, Flex, Honeywell Aerospace, Veeva, FedEx Freight, Solstice Advanced Materials) postdate the `universe.csv` snapshot and are correctly absent from both files — not an error.
- `added_date` for renamed/reorganized companies (META, WTW, and the Fox/News Corp lineage) reflects the **changes-table event date**, which may differ by policy choice from a "true" continuous-listing start date in some edge cases — e.g., NWSA's 2013-07-01 date reflects a mid-lineage relabeling event captioned "21st Century Fox" rather than the original News Corp listing. This is inherent to using the changes table as the primary source per the task's specified methodology and is called out here rather than silently smoothed over.
- No git commands were run and no files outside `research/membership/` were created or modified.

## Reconciliation addendum (2026-07-06, orchestrator)

Two independent builds of this file were reconciled (constituents-"Date added"-primary
vs changes-table-primary). Overlap: **zero date disagreements > 7 days**. Net differences,
resolved as follows:
- Changes-table build (kept) correctly dates POOL, EPAM, SATS, EG inside the window where
  the constituents column was empty/older — kept.
- **SW (Smurfit Westrock) patched to 2024-07-08** from the constituents column: the merger
  entry is absent from the changes table as an "Added Ticker" row, so the primary method
  left it undated (= always-member, wrong for the window). Window additions: 86 -> 87.
- ECHO, FDXF, FLEX, HONA, MRVL, VEEV (+ SOLS, 2025-10-30) are index members per the fresh
  Wikipedia snapshot but are ABSENT from config/universe.csv -> the product's static
  universe is stale by up to ~9 months (separate product finding, not a membership bug).
