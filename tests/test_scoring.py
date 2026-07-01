"""
Tests for the factor scoring engine (INIT-22 M1 rework).

Without a backtest there is no ground-truth ranking, so these tests pin the
DIRECTION and INVARIANTS of the engine, not levels — which is exactly what
catches a stray sign flip (the most likely bug) that a "sector mean ~ 0" or
"equal weights -> mean" check would wave through green.

Run:  python -m pytest tests/test_scoring.py -v
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.lib.scoring import build_scores, gaussian_rank, sector_neutralize

N = 12


def _base_universe(n: int = N, sector: str = "X") -> pd.DataFrame:
    """A single-sector universe with every factor CONSTANT (so each is neutral,
    z=0) — a test overrides exactly one factor to isolate its direction."""
    return pd.DataFrame({
        "ticker": [f"S{i:02d}" for i in range(n)],
        "name":   [f"S{i}" for i in range(n)],
        "sector": [sector] * n,
        "cik":    [""] * n,
        "ret_12_1": [0.10] * n, "ret_13w": [0.05] * n, "volatility_26w": [0.25] * n,
        "pe_ratio": [20.0] * n, "ev_ebitda": [10.0] * n, "pb_ratio": [3.0] * n,
        "dividend_yield": [0.02] * n,
        "roe": [0.15] * n, "oper_margin_ttm": [0.20] * n, "fcf_margin_ttm": [0.15] * n,
        "roic": [0.12] * n, "debt_equity": [0.50] * n, "beta": [1.00] * n,
    })


def _mono(n: int = N) -> list[float]:
    return list(np.linspace(1.0, 2.0, n))


# ── Direction golden — the sign-flip catchers ─────────────────────────────────

def test_cheaper_scores_higher_value():
    df = _base_universe()
    df["pe_ratio"] = _mono()                       # S00 cheapest, S11 dearest
    v = build_scores(df).set_index("ticker")["value_score"]
    assert v.loc["S00"] == v.max()
    assert v.loc["S11"] == v.min()


def test_lower_vol_scores_higher_risk():
    df = _base_universe()
    df["volatility_26w"] = _mono()                 # S00 calmest = safest
    r = build_scores(df).set_index("ticker")["risk_score"]
    assert r.loc["S00"] == r.max()
    assert r.loc["S11"] == r.min()


def test_higher_debt_and_beta_score_worse_risk():
    df = _base_universe()
    df["debt_equity"] = _mono()
    df["beta"] = _mono()
    r = build_scores(df).set_index("ticker")["risk_score"]
    assert r.loc["S00"] == r.max()                 # least leveraged / lowest beta
    assert r.loc["S11"] == r.min()


def test_higher_momentum_scores_higher_trend():
    df = _base_universe()
    df["ret_12_1"] = _mono()
    t = build_scores(df).set_index("ticker")["trend_score"]
    assert t.loc["S11"] == t.max()
    assert t.loc["S00"] == t.min()


def test_higher_roe_scores_higher_quality():
    df = _base_universe()
    df["roe"] = _mono()
    q = build_scores(df).set_index("ticker")["quality_score"]
    assert q.loc["S11"] == q.max()
    assert q.loc["S00"] == q.min()


def test_negative_book_value_ranks_bottom_on_value():
    """Yield-form fix: negative book (pb < 0) is a distress signal, not 'cheap'.
    The old 1 - rank(pb) ranked it as the CHEAPEST stock."""
    df = _base_universe()
    df["pb_ratio"] = [2.0] * (N - 1) + [-1.0]       # S11 has negative book value
    v = build_scores(df).set_index("ticker")["value_score"]
    assert v.loc["S11"] == v.min()


# ── Missing-data bias — the NaN-injection test ────────────────────────────────

def test_missing_risk_data_does_not_improve_rank():
    """Zeroing a genuinely-safe stock's risk bucket must LOWER its composite (to
    neutral 0), never raise it — the exact bug the old neutral-0.5 fill created."""
    df = _base_universe()
    df["volatility_26w"] = _mono()                  # S00 safest on risk
    before = build_scores(df).set_index("ticker")
    comp_before = before.loc["S00", "composite_score"]

    df2 = df.copy()
    mask = df2["ticker"] == "S00"
    df2.loc[mask, ["volatility_26w", "debt_equity", "beta"]] = np.nan
    after = build_scores(df2).set_index("ticker")

    assert after.loc["S00", "risk_score"] == 0.0    # neutral, not the safe extreme
    assert after.loc["S00", "composite_score"] <= comp_before + 1e-9


def test_all_trend_inputs_missing_is_neutral_zero_not_half():
    df = _base_universe()
    df.loc[df["ticker"] == "S05", ["ret_12_1", "ret_13w"]] = np.nan
    t = build_scores(df).set_index("ticker")["trend_score"]
    assert t.loc["S05"] == 0.0                      # NOT the old 0.50


# ── Sector neutralization — no sector tilt leaks ──────────────────────────────

def test_sector_neutralization_zeroes_each_sector_mean():
    """Two sectors at very different roe levels; after within-sector
    standardization each sector's quality mean is ~0 (the sector bet is removed)."""
    rows = []
    for sec, lo, hi in [("A", 0.20, 0.35), ("B", 0.02, 0.10)]:
        for i, roe in enumerate(np.linspace(lo, hi, N)):
            rows.append({
                "ticker": f"{sec}{i:02d}", "name": f"{sec}{i}", "sector": sec, "cik": "",
                "ret_12_1": 0.1, "ret_13w": 0.05, "volatility_26w": 0.25,
                "pe_ratio": 20.0, "ev_ebitda": 10.0, "pb_ratio": 3.0, "dividend_yield": 0.02,
                "roe": roe, "oper_margin_ttm": 0.2, "fcf_margin_ttm": 0.15,
                "roic": 0.12, "debt_equity": 0.5, "beta": 1.0,
            })
    scored = build_scores(pd.DataFrame(rows))
    for sec in ("A", "B"):
        m = scored.loc[scored["sector"] == sec, "quality_score"].mean()
        assert abs(m) < 1e-6, f"sector {sec} quality mean = {m}"


# ── Robustness ────────────────────────────────────────────────────────────────

def test_build_scores_is_deterministic():
    df = _base_universe()
    df["pe_ratio"] = _mono()
    df["roe"] = _mono()[::-1]
    pd.testing.assert_frame_equal(build_scores(df), build_scores(df.copy()))


def test_all_nan_subfactor_reweights_not_crashes():
    df = _base_universe()
    df["roic"] = np.nan          # a whole sub-factor column missing
    df["fcf_margin_ttm"] = np.nan
    df["roe"] = _mono()
    q = build_scores(df).set_index("ticker")["quality_score"]
    assert q.notna().all()       # reweighted onto roe/opm, never crashes
    assert q.loc["S11"] == q.max()


def test_constant_factor_is_neutral_not_nan():
    """A perfectly constant sub-factor (zero variance) collapses to neutral 0,
    never NaN or a divide-by-zero."""
    z = gaussian_rank(pd.Series([5.0] * N))
    zn = sector_neutralize(z, pd.Series(["X"] * N))
    assert zn.abs().max() < 1e-9


def test_gaussian_rank_is_monotonic_and_finite():
    z = gaussian_rank(pd.Series([1.0, 2.0, 3.0, 4.0, 5.0]))
    assert list(z) == sorted(z)          # order preserved
    assert np.isfinite(z).all()          # no +/- inf at the tails
