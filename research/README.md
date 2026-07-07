# research/ — backtest / IC framework

READ-ONLY analytical framework over the canonical price-archive + EDGAR PIT panel.
It scores the SAME code the dashboard ships (`src/lib/scoring.py`, `config/scoring.yml`)
and writes `research/results/` (CSVs + `REPORT.md`).

Run from the repo root:

```
python -m research.backtest.run_ic                     # price factors only
python -m research.backtest.run_ic --with-fundamentals # + PIT value/quality
```

Needs the sibling repos on the path (`C:/Projects/collectors`, `C:/Projects/data-core`)
and `DATACORE_ROOT` pointing at the `price-archive` checkout. `panel.py` wires the
conventional locations automatically; an explicit env still wins.

---

## Reproducibility ritual (REPRO-REPORT §7)

The 2026-07-07 anchor incident (`_stock_selection_analytics/repro/REPRO-REPORT.md`)
had two root causes: a result whose inputs could not be pinned without transcript
archaeology, and an IC number whose filter convention lived only in prose. These
four rules close both. Rules 2 and 3 are enforced in code; rules 1 and 4 are the
commit / audit discipline around it.

**Rule 1 — results are a function of the commit.**
`research/results/` CSVs + `REPORT.md` are regenerated IMMEDIATELY before a commit
that changes the framework, and land IN THE SAME COMMIT as the code — from a clean
working tree of ALL input repos (this repo, price-archive, collectors, data-core).
Never commit code and results hours apart: the 9-hour gap in the anchor incident is
exactly the window where code and results silently diverge. If `config/universe.csv`
is intentionally uncommitted, that is a real repro-hole — the results are then NOT a
function of committed inputs alone, and the provenance block will say `UNCOMMITTED`.

**Rule 2 — provenance block in `REPORT.md`** (auto, `provenance.py` → `report.py`).
Every report opens with the exact input identities the run is a function of:
- this repo's commit SHA (+ `DIRTY` + tracked-diff hash when the tree is dirty);
- HEAD of price-archive / collectors / data-core;
- **git blob id of `config/universe.csv`** — `git rev-parse HEAD:config/universe.csv`,
  **never** a working-tree sha256 (CRLF normalization makes a worktree hash unstable
  across checkouts). A sha256 is emitted ONLY as an extra tag when the file is
  uncommitted;
- sha256 of `edgar_pit_panel.csv.gz`; `DATACORE_ROOT`; timestamp.

A bit-repro check is then a comparison of this block — not a hunt through logs.

**Rule 3 — the filter label travels with every number.**
The main IC table (`ic_summary.csv` / `REPORT.md`) carries a `membership` column.
That table is computed WITHOUT the membership filter, so every row reads
`unfiltered`; the filtered comparison is the separate pre-inclusion-bias table.
The anchor incident was a filtered number (`+0.023`) compared against an unfiltered
anchor (`+0.041`) — apples vs oranges — because the convention lived only in a
header sentence. Any citation of an IC number in a `*.md` audit MUST carry its
`filtered` / `unfiltered` label too.

**Rule 4 — sanity gates state their convention, not just a value.**
A gate of the form "12-1 raw 63d IC within ±0.01 of the anchor" MUST also fix the
convention it assumes: filter (`filtered`/`unfiltered`), variant (`raw`/`neutral`),
horizon (`21d`/`63d`), and the `universe.csv` blob id. A gate that pins only the
value will fire on a convention mismatch and be misread as a data/code drift — which
is precisely how the anchor incident began.

---

## Layout

- `backtest/` — the framework: `run_ic.py` (CLI/orchestrator), `panel.py`,
  `signals.py`, `metrics.py`, `forward.py`, `composites.py`, `membership.py`,
  `fundamentals_signals.py`, `provenance.py`, `report.py`, `tests/`.
- `fundamentals/` — the EDGAR PIT panel builder (Interface P) + `tests/`.
- `membership/` — point-in-time index membership (Interface M).
- `data/` — `edgar_pit_panel.csv.gz` (built artifact).
- `results/` — generated CSVs + `REPORT.md`.
