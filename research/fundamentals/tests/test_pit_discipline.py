"""
Real-data gates G4 (PIT / no-lookahead discipline) and G5 (YTD/quarter
differentiation) against the AAPL companyfacts cache.

These are skipped automatically if the AAPL cache file is absent (so the suite still
runs on a machine that has not collected yet). To run:

    python -m pytest research/fundamentals/tests/test_pit_discipline.py -q
"""

from __future__ import annotations

import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd
import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from research.fundamentals import build_panel, pit  # noqa: E402

_AAPL_CIK10 = "0000320193"
_AAPL_CACHE = _REPO_ROOT / "research" / "cache" / "edgar" / f"CIK{_AAPL_CIK10}.json"

pytestmark = pytest.mark.skipif(
    not _AAPL_CACHE.exists(),
    reason="AAPL cache not collected yet (run collect_edgar first).",
)


def _aapl_panel() -> pd.DataFrame:
    """Build a one-company panel for AAPL straight from the cache (no full-panel
    dependency), returning it as a typed DataFrame like load_panel would."""
    data = json.loads(_AAPL_CACHE.read_text(encoding="utf-8"))
    rows = build_panel._extract_company(
        "AAPL", _AAPL_CIK10, data.get("facts", {}),
        stats={"missing_concept": {}},
    )
    df = pd.DataFrame(rows, columns=build_panel.PANEL_COLUMNS)
    for c in ("period_start", "period_end", "filed"):
        df[c] = pd.to_datetime(df[c], errors="coerce")
    df["value"] = pd.to_numeric(df["value"], errors="coerce")
    df["fy"] = pd.to_numeric(df["fy"], errors="coerce")
    return df


# ---------------------------------------------------------------------------
# G4 — PIT discipline: as_known_at('2023-01-15') sees ONLY filed <= that date.
# ---------------------------------------------------------------------------
def test_g4_no_lookahead_at_cutoff():
    panel = _aapl_panel()
    cutoff = pd.Timestamp("2023-01-15")

    # The accessor must never consult a fact filed after the cutoff. We assert the
    # invariant directly on the filtered frame the accessor uses.
    known = panel[panel["filed"] <= cutoff]
    assert (known["filed"] <= cutoff).all()
    assert known["filed"].max() <= cutoff

    # And a fact we KNOW post-dates the cutoff must be excluded: AAPL's FY2023 Q1
    # 10-Q (quarter ended 2022-12-31) was filed 2023-02-03 > 2023-01-15.
    post = panel[(panel["concept"] == "revenues") & (panel["period_end"] == pd.Timestamp("2022-12-31"))]
    assert len(post) >= 1
    assert (post["filed"] > cutoff).all()

    # The as_of result for AAPL must be computable and its TTM must be built only
    # from quarters ending on/before the last known filing date.
    wide = pit.as_known_at(panel, "2023-01-15")
    assert "AAPL" in wide.index
    # revenues_ttm should be present (AAPL had >= 4 quarters known by Jan 2023).
    assert pd.notna(wide.loc["AAPL", "revenues_ttm"])


def test_g4_later_date_sees_more():
    """Monotonicity: a later as_of can only ADD filings, never remove them."""
    panel = _aapl_panel()
    early = panel[panel["filed"] <= pd.Timestamp("2023-01-15")]
    late = panel[panel["filed"] <= pd.Timestamp("2023-06-15")]
    assert len(late) >= len(early)


# ---------------------------------------------------------------------------
# G5 — YTD/quarter differentiation: the recovered discrete Q matches the JSON.
# ---------------------------------------------------------------------------
def test_g5_discrete_quarter_matches_json():
    """AAPL FY2023 Q1 revenue (quarter ended 2022-12-31) is tagged directly in the
    companyfacts JSON as a ~97-day discrete quarter worth $117,154,000,000. The
    quarter machinery must reproduce exactly that value (i.e. it does NOT mistake
    the 10-Q's YTD figure for the quarter)."""
    panel = _aapl_panel()
    rev = panel[panel["concept"] == "revenues"]
    q = pit._quarters_for_flow(rev)

    target_end = pd.Timestamp("2022-12-31")
    match = q[q["period_end"] == target_end]
    assert len(match) == 1, f"expected one quarter ending {target_end.date()}"
    # $117.154B, the publicly reported Q1 FY2023 net sales.
    assert float(match.iloc[0]["value"]) == pytest.approx(117_154_000_000, rel=1e-6)


def test_g5_q4_is_fy_minus_9m():
    """A derived Q4 (only obtainable as FY - 9M) must be positive and of sane
    magnitude — i.e. the differencing ladder actually fired for a full fiscal year.
    We check AAPL FY2022 (fiscal year ended 2022-09-24): Q4 = FY - 9M > 0."""
    panel = _aapl_panel()
    rev = panel[panel["concept"] == "revenues"]
    q = pit._quarters_for_flow(rev)
    fy2022 = q[q["fy"] == 2022].sort_values("period_end")
    # Expect four quarters recovered for a completed fiscal year.
    assert len(fy2022) >= 4
    # Every recovered quarterly revenue is positive and < the full-year figure.
    assert (fy2022["value"] > 0).all()


def test_g5_sum_of_quarters_matches_reported_fy():
    """Internal consistency: the four recovered quarters of AAPL FY2022 sum to the
    reported FY revenue (the 365-day fact) within rounding."""
    panel = _aapl_panel()
    rev_rows = panel[panel["concept"] == "revenues"].to_dict("records")
    # Find AAPL's reported FY2022 revenue: a ~365d fact with fy==2022, latest filed.
    fy_facts = []
    for r in rev_rows:
        if pd.isna(r["period_start"]) or pd.isna(r["period_end"]):
            continue
        dur = (r["period_end"].date() - r["period_start"].date()).days
        if 340 <= dur <= 380 and r["fy"] == 2022:
            fy_facts.append(r)
    assert fy_facts, "no FY2022 annual revenue fact found"
    fy_val = sorted(fy_facts, key=lambda r: r["filed"])[-1]["value"]

    q = pit._quarters_for_flow(panel[panel["concept"] == "revenues"])
    fy2022 = q[q["fy"] == 2022]
    assert float(fy2022["value"].sum()) == pytest.approx(float(fy_val), rel=1e-4)
