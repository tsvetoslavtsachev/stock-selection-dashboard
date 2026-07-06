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

from src.lib.scoring import (
    _DEFAULT_WEIGHTS,
    _restandardize,
    build_scores,
    gaussian_rank,
    load_weights,
    sector_neutralize,
)

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


def test_negative_book_and_expensive_rank_below_cheap_on_value():
    """Yield-form fix, made DISCRIMINATING: 1/pb must order cheap > expensive >
    negative-book. A no-inversion mutation (raw pb, higher=better) would instead
    put the most EXPENSIVE (pb=100) at the TOP, so this fails without the fix; and
    negative book (distress) must land at the very bottom, never 'cheapest'."""
    df = _base_universe()
    # S00..S09 normal (pb 2), S10 wildly expensive (pb 100), S11 negative book (-1).
    df["pb_ratio"] = [2.0] * (N - 2) + [100.0, -1.0]
    v = build_scores(df).set_index("ticker")["value_score"]
    assert v.loc["S11"] == v.min()          # negative book = worst, not "cheapest"
    assert v.loc["S10"] < v.loc["S00"]      # expensive ranks below cheap (discriminator)


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
        # ~0 up to 4-decimal rounding; a real sector tilt would be O(0.1)+.
        assert abs(m) < 1e-3, f"sector {sec} quality mean = {m}"


def test_sector_neutralize_gives_unit_within_sector_variance():
    """The de-VOL half (dropping `/ std` would still zero each sector's mean and
    pass the test above). Two sectors with very different within-sector spread must
    BOTH come out at ~unit within-sector std after full standardization."""
    secA = np.linspace(0.200, 0.205, N)   # tiny raw spread
    secB = np.linspace(0.020, 0.400, N)   # wide raw spread
    vals = pd.Series(list(secA) + list(secB))
    sector = pd.Series(["A"] * N + ["B"] * N)
    zn = sector_neutralize(gaussian_rank(vals), sector)
    for sec in ("A", "B"):
        s = zn[(sector == sec).to_numpy()].std(ddof=0)
        assert abs(s - 1.0) < 0.05, f"sector {sec} within-sector std = {s}"


def test_small_sector_falls_back_to_universe_standardization():
    """A sector with < 10 members must be standardized against the UNIVERSE, not
    within itself (too few points). Three genuinely high-roe names in a tiny sector
    should keep a clearly positive neutralized mean; within-sector z would force it
    to ~0 and erase the real signal."""
    big = np.linspace(0.05, 0.15, N)      # a normal large sector, mid-range roe
    small = [0.40, 0.42, 0.44]            # 3 names, all far above the universe
    vals = pd.Series(list(big) + small)
    sector = pd.Series(["BIG"] * N + ["SMALL"] * 3)
    zn = sector_neutralize(gaussian_rank(vals), sector)
    small_mean = zn[(sector == "SMALL").to_numpy()].mean()
    assert small_mean > 0.5, f"small-sector mean {small_mean} — was it de-meaned within-sector?"


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


# ── Composite: config-driven weights + bucket re-standardization (Etap D) ─────

def _weights(composite: dict) -> dict:
    return {"composite": composite, "subfactors": _DEFAULT_WEIGHTS["subfactors"]}


def test_composite_weight_change_moves_the_ranking():
    """Wiring proof: the weights genuinely drive the composite. A trend-only
    weighting tops the highest-momentum stock; a risk-only weighting the safest."""
    df = _base_universe()
    df["ret_12_1"] = _mono()             # S11 highest momentum
    df["volatility_26w"] = _mono()       # S00 safest
    top_trend = build_scores(df, _weights(
        {"trend": 1.0, "quality": 0.0, "value": 0.0, "risk": 0.0})).iloc[0]["ticker"]
    top_risk = build_scores(df, _weights(
        {"trend": 0.0, "quality": 0.0, "value": 0.0, "risk": 1.0})).iloc[0]["ticker"]
    assert top_trend == "S11"
    assert top_risk == "S00"
    assert top_trend != top_risk


def test_equal_weight_composite_is_mean_of_restandardized_buckets():
    """With equal weights the composite is exactly the mean of the four buckets
    AFTER each is re-standardized — the check that makes 'equal weight' real."""
    df = _base_universe()
    df["pe_ratio"] = _mono()
    df["roe"] = _mono()[::-1]
    df["ret_12_1"] = _mono()
    scored = build_scores(df)            # equal weights (default)
    buckets = ["trend_score", "quality_score", "value_score", "risk_score"]
    expected = sum(_restandardize(scored[b]) for b in buckets) / 4.0
    assert np.allclose(scored["composite_score"].to_numpy(),
                       expected.to_numpy(), atol=1e-3)


def test_restandardize_is_unit_variance():
    """Direct, NON-tautological pin on the re-standardization: an input with std
    far from 1 must come out at std ~1. (The composite test above computes its
    expected via _restandardize too, so on its own an identity mutation would slip
    through both sides — this catches it.)"""
    s = pd.Series(np.linspace(0.0, 10.0, 50))     # std ~= 2.96, nowhere near 1
    out = _restandardize(s)
    assert abs(out.std(ddof=0) - 1.0) < 1e-9
    assert abs(out.mean()) < 1e-9


def test_restandardize_does_not_amplify_on_coverage_collapse():
    """F1 regression: re-standardizing against the ZERO-padded column (the old bug)
    lets a fundamental-coverage collapse AMPLIFY the scored names — the neutral
    zeros shrink the std, so dividing by it inflates the scored z's.

    Model of the proof: the SAME 60 scored names, once inside a full 500-name
    bucket and once padded out to 500 with neutral zeros (440 names lost fundamental
    coverage). Standardized against the scored sub-population, the scored names must
    keep the SAME re-standardized values in both — no amplification. Against the
    whole zero-padded column they blow up by ~2.7x (the documented failure)."""
    rng = np.random.default_rng(0)
    scored_vals = pd.Series(rng.normal(0.0, 1.0, 60))

    # (a) full coverage: all 60 are the population.
    full = _restandardize(scored_vals, pd.Series([True] * 60))

    # (b) collapsed coverage: same 60 scored values + 440 neutral zeros.
    padded = pd.concat([scored_vals, pd.Series([0.0] * 440)], ignore_index=True)
    mask = pd.Series([True] * 60 + [False] * 440)
    collapsed = _restandardize(padded, mask)

    # The scored names' re-standardized values are invariant to the padding.
    np.testing.assert_allclose(collapsed.iloc[:60].to_numpy(),
                               full.to_numpy(), atol=1e-9)

    # Guard the guard: had we standardized against the whole zero-padded column,
    # the scored names would have been amplified (std shrinks with the zeros). Prove
    # the amplification is real and that the fix removes it (ratio ~1, not ~2.7).
    naive = _restandardize(padded)                 # no mask -> old whole-column behaviour
    amp = float(naive.iloc[:60].std(ddof=0) / collapsed.iloc[:60].std(ddof=0))
    assert amp > 2.0, f"expected the old path to amplify, got {amp}x"


def test_coverage_collapse_does_not_inflate_scored_composite():
    """F1 end-to-end: a genuinely-scored name's composite contribution must not be
    inflated just because most OTHER names lost fundamental coverage in a bucket.

    Two universes with an IDENTICAL trend signal (fully covered in both) differ only
    in quality coverage: one fully covered, one collapsed (most quality NaN -> neutral
    0). The trend-driven ranking gap between the top and bottom momentum name must be
    (near-)equal across the two — the collapsed-coverage quality bucket must not,
    via amplification, distort the surviving buckets' relative weight."""
    n = 60
    df_full = _base_universe(n)
    df_full["ret_12_1"] = list(np.linspace(1.0, 2.0, n))   # clean momentum gradient

    df_collapsed = df_full.copy()
    # Wipe quality inputs for all but 6 names -> ~10% coverage (the collapse).
    q_cols = ["roe", "oper_margin_ttm", "fcf_margin_ttm", "roic"]
    keep = df_collapsed.index[:6]
    df_collapsed.loc[~df_collapsed.index.isin(keep), q_cols] = np.nan

    a = build_scores(df_full).set_index("ticker")["composite_score"]
    b = build_scores(df_collapsed).set_index("ticker")["composite_score"]

    # Trend contribution (top minus bottom momentum) is identical in both universes
    # in the FIXED engine (quality is neutral/constant on both ends). Under the old
    # bug the collapsed quality bucket amplified and shifted the composite spread.
    spread_full = a.max() - a.min()
    spread_collapsed = b.max() - b.min()
    assert abs(spread_full - spread_collapsed) < 0.05 * spread_full, (
        f"coverage collapse moved the trend spread: {spread_full} vs {spread_collapsed}")


def test_deep_invalid_config_falls_back(tmp_path):
    """A structurally-wrong config (mistyped sub-factor key) must fall back to the
    equal-weight default, not pass shallow validation and later KeyError."""
    bad = tmp_path / "scoring.yml"
    bad.write_text(
        "composite: {trend: 0.25, quality: 0.25, value: 0.25, risk: 0.25}\n"
        "subfactors:\n"
        "  trend: {ret_12_1: 0.5, ret_13w: 0.5}\n"
        "  quality: {roe: 0.25, oper_margin_ttm: 0.25, fcf_margin_ttm: 0.25, roic: 0.25}\n"
        "  value: {pe_ratio: 0.25, ev_ebitda: 0.25, pb_ratio: 0.25, dividend_yield: 0.25}\n"
        "  risk: {volatility_26w: 0.34, debt_TYPO: 0.33, beta: 0.33}\n",  # debt_equity mistyped
        encoding="utf-8")
    assert load_weights(bad) == _DEFAULT_WEIGHTS


def test_load_weights_falls_back_when_file_missing(tmp_path):
    assert load_weights(tmp_path / "nope.yml") == _DEFAULT_WEIGHTS
