"""
Tests for the factor scoring engine (INIT-22 M2 rework).

Without a backtest there is no ground-truth ranking, so these tests pin the
DIRECTION and INVARIANTS of the engine, not levels -- which is exactly what
catches a stray sign flip (the most likely bug) that a "sector mean ~ 0" or
"equal weights -> mean" check would wave through green.

M2 changes pinned here: Trend = ret_12_1 only; Quality adds gpa (ROIC unscored);
Value = E/P + EV/EBITDA yield + net_payout_yield (P/B + dividend_yield unscored);
Composite = Trend + Quality + Value with equal-contribution (ERC) weights (Risk is
a separate lens, still scored but out of the composite).

Run:  python -m pytest tests/test_scoring.py -v
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from src.lib.scoring import (
    _COMPOSITE_BUCKETS,
    _DEFAULT_WEIGHTS,
    _erc_weights,
    _restandardize,
    build_scores,
    gaussian_rank,
    load_weights,
    sector_neutralize,
)

N = 12


def _base_universe(n: int = N, sector: str = "X") -> pd.DataFrame:
    """A single-sector universe with every factor CONSTANT (so each is neutral,
    z=0) -- a test overrides exactly one factor to isolate its direction. Carries
    both the SCORED inputs and the retained-but-unscored ones (ret_13w, pb_ratio,
    dividend_yield, roic) so the engine reads exactly what the pipeline feeds it."""
    return pd.DataFrame({
        "ticker": [f"S{i:02d}" for i in range(n)],
        "name":   [f"S{i}" for i in range(n)],
        "sector": [sector] * n,
        "cik":    [""] * n,
        "ret_12_1": [0.10] * n, "ret_13w": [0.05] * n, "volatility_26w": [0.25] * n,
        "pe_ratio": [20.0] * n, "ev_ebitda": [10.0] * n, "pb_ratio": [3.0] * n,
        "dividend_yield": [0.02] * n, "net_payout_yield": [0.03] * n,
        "roe": [0.15] * n, "oper_margin_ttm": [0.20] * n, "fcf_margin_ttm": [0.15] * n,
        "roic": [0.12] * n, "gpa": [0.30] * n, "debt_equity": [0.50] * n, "beta": [1.00] * n,
    })


def _mono(n: int = N) -> list[float]:
    return list(np.linspace(1.0, 2.0, n))


# -- Direction golden -- the sign-flip catchers --------------------------------

def test_cheaper_scores_higher_value():
    df = _base_universe()
    df["pe_ratio"] = _mono()                       # S00 cheapest, S11 dearest
    v = build_scores(df).set_index("ticker")["value_score"]
    assert v.loc["S00"] == v.max()
    assert v.loc["S11"] == v.min()


def test_higher_net_payout_scores_higher_value():
    """net_payout_yield is a YIELD (higher = more cash returned = better): it is NOT
    inverted. The most-returning name must top the value bucket."""
    df = _base_universe()
    df["net_payout_yield"] = _mono()               # S11 returns the most cash
    v = build_scores(df).set_index("ticker")["value_score"]
    assert v.loc["S11"] == v.max()
    assert v.loc["S00"] == v.min()


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


def test_ret_13w_does_not_move_trend_score():
    """M2: ret_13w is no longer scored. Varying it must leave trend_score flat
    (Trend = ret_12_1 only), even though the column is still present."""
    df = _base_universe()
    df["ret_13w"] = _mono()                        # would have tilted trend under M1
    t = build_scores(df).set_index("ticker")["trend_score"]
    assert t.abs().max() < 1e-9                     # all neutral -> ret_13w ignored


def test_higher_roe_scores_higher_quality():
    df = _base_universe()
    df["roe"] = _mono()
    q = build_scores(df).set_index("ticker")["quality_score"]
    assert q.loc["S11"] == q.max()
    assert q.loc["S00"] == q.min()


def test_higher_gpa_scores_higher_quality():
    """GP/A is the new Quality input (Novy-Marx): higher = better."""
    df = _base_universe()
    df["gpa"] = _mono()
    q = build_scores(df).set_index("ticker")["quality_score"]
    assert q.loc["S11"] == q.max()
    assert q.loc["S00"] == q.min()


def test_roic_does_not_move_quality_score():
    """M2: ROIC is no longer scored. Varying it must leave quality_score flat."""
    df = _base_universe()
    df["roic"] = _mono()
    q = build_scores(df).set_index("ticker")["quality_score"]
    assert q.abs().max() < 1e-9


def test_pb_and_dividend_yield_do_not_move_value_score():
    """M2: P/B and dividend_yield are dropped from scoring (UI-only). Varying either
    must NOT move value_score -- the audited P/B defect can no longer leak in."""
    df = _base_universe()
    df["pb_ratio"] = _mono()
    df["dividend_yield"] = _mono()
    v = build_scores(df).set_index("ticker")["value_score"]
    assert v.abs().max() < 1e-9


def test_negative_and_expensive_earnings_rank_below_cheap_on_value():
    """Yield-form fix on E/P (the scored multiple), made DISCRIMINATING: 1/PE must
    order cheap > expensive > negative-earnings. A no-inversion mutation (raw PE,
    higher=better) would put the most EXPENSIVE at the TOP; negative earnings
    (distress) must land at the very bottom, never 'cheapest'."""
    df = _base_universe()
    # S00..S09 normal (pe 15), S10 wildly expensive (pe 500), S11 negative (-20).
    df["pe_ratio"] = [15.0] * (N - 2) + [500.0, -20.0]
    v = build_scores(df).set_index("ticker")["value_score"]
    assert v.loc["S11"] == v.min()          # negative earnings = worst, not "cheapest"
    assert v.loc["S10"] < v.loc["S00"]      # expensive ranks below cheap (discriminator)


# -- Missing-data bias -- the NaN-injection test -------------------------------

def test_missing_risk_data_is_neutral_zero():
    """Zeroing a genuinely-safe stock's risk bucket must fall to neutral 0 -- the
    exact bug the old neutral-0.5 fill created. (Risk is a lens now, out of the
    composite, so we assert on the risk bucket directly.)"""
    df = _base_universe()
    df["volatility_26w"] = _mono()                  # S00 safest on risk
    df2 = df.copy()
    mask = df2["ticker"] == "S00"
    df2.loc[mask, ["volatility_26w", "debt_equity", "beta"]] = np.nan
    after = build_scores(df2).set_index("ticker")
    assert after.loc["S00", "risk_score"] == 0.0    # neutral, not the safe extreme


def test_all_trend_inputs_missing_is_neutral_zero_not_half():
    df = _base_universe()
    df.loc[df["ticker"] == "S05", ["ret_12_1"]] = np.nan
    t = build_scores(df).set_index("ticker")["trend_score"]
    assert t.loc["S05"] == 0.0                      # NOT the old 0.50


# -- Sector neutralization -- no sector tilt leaks -----------------------------

def test_sector_neutralization_zeroes_each_sector_mean():
    """Two sectors at very different roe levels; after within-sector
    standardization each sector's quality mean is ~0 (the sector bet is removed)."""
    rows = []
    for sec, lo, hi in [("A", 0.20, 0.35), ("B", 0.02, 0.10)]:
        for i, roe in enumerate(np.linspace(lo, hi, N)):
            rows.append({
                "ticker": f"{sec}{i:02d}", "name": f"{sec}{i}", "sector": sec, "cik": "",
                "ret_12_1": 0.1, "ret_13w": 0.05, "volatility_26w": 0.25,
                "pe_ratio": 20.0, "ev_ebitda": 10.0, "pb_ratio": 3.0,
                "dividend_yield": 0.02, "net_payout_yield": 0.03,
                "roe": roe, "oper_margin_ttm": 0.2, "fcf_margin_ttm": 0.15,
                "roic": 0.12, "gpa": 0.30, "debt_equity": 0.5, "beta": 1.0,
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
    assert small_mean > 0.5, f"small-sector mean {small_mean} -- was it de-meaned within-sector?"


# -- Robustness ----------------------------------------------------------------

def test_build_scores_is_deterministic():
    df = _base_universe()
    df["pe_ratio"] = _mono()
    df["roe"] = _mono()[::-1]
    pd.testing.assert_frame_equal(build_scores(df), build_scores(df.copy()))


def test_all_nan_subfactor_reweights_not_crashes():
    df = _base_universe()
    df["gpa"] = np.nan           # a whole sub-factor column missing
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


# -- Composite: 3-bucket ERC blend (M2) ----------------------------------------

def _weights(composite: dict) -> dict:
    return {"composite": composite, "subfactors": _DEFAULT_WEIGHTS["subfactors"]}


def test_composite_excludes_risk():
    """M2 headline: Risk is NOT in the composite. Make Risk the ONLY varying bucket;
    the composite (Trend+Quality+Value, all neutral) must stay flat regardless."""
    df = _base_universe()
    df["volatility_26w"] = _mono()         # only risk varies
    scored = build_scores(df).set_index("ticker")
    assert scored["risk_score"].abs().max() > 0.1     # risk IS scored + varies
    assert scored["composite_score"].abs().max() < 1e-6  # composite ignores it


def test_composite_weight_change_moves_the_ranking():
    """Wiring proof: the composite weights genuinely drive the composite. A
    trend-only weighting tops the highest-momentum stock; a value-only weighting the
    cheapest."""
    df = _base_universe()
    df["ret_12_1"] = _mono()             # S11 highest momentum
    df["pe_ratio"] = _mono()             # S00 cheapest
    top_trend = build_scores(df, _weights(
        {"trend": 1.0, "quality": 0.0, "value": 0.0})).iloc[0]["ticker"]
    top_value = build_scores(df, _weights(
        {"trend": 0.0, "quality": 0.0, "value": 1.0})).iloc[0]["ticker"]
    assert top_trend == "S11"
    assert top_value == "S00"
    assert top_trend != top_value


def test_restandardize_is_unit_variance():
    """Direct, NON-tautological pin on the re-standardization: an input with std
    far from 1 must come out at std ~1."""
    s = pd.Series(np.linspace(0.0, 10.0, 50))     # std ~= 2.96, nowhere near 1
    out = _restandardize(s)
    assert abs(out.std(ddof=0) - 1.0) < 1e-9
    assert abs(out.mean()) < 1e-9


def test_restandardize_does_not_amplify_on_coverage_collapse():
    """F1 regression: re-standardizing against the ZERO-padded column (the old bug)
    lets a fundamental-coverage collapse AMPLIFY the scored names -- the neutral
    zeros shrink the std, so dividing by it inflates the scored z's."""
    rng = np.random.default_rng(0)
    scored_vals = pd.Series(rng.normal(0.0, 1.0, 60))

    full = _restandardize(scored_vals, pd.Series([True] * 60))

    padded = pd.concat([scored_vals, pd.Series([0.0] * 440)], ignore_index=True)
    mask = pd.Series([True] * 60 + [False] * 440)
    collapsed = _restandardize(padded, mask)

    np.testing.assert_allclose(collapsed.iloc[:60].to_numpy(),
                               full.to_numpy(), atol=1e-9)

    naive = _restandardize(padded)                 # no mask -> old whole-column behaviour
    amp = float(naive.iloc[:60].std(ddof=0) / collapsed.iloc[:60].std(ddof=0))
    assert amp > 2.0, f"expected the old path to amplify, got {amp}x"


# -- ERC composite solver (ported from research variance_share) ----------------

def _corr_buckets(seed=7, n=800):
    """Three correlated bucket columns with UNEQUAL dispersions -- so an equal-WEIGHT
    blend would NOT have equal variance shares (the whole point of the ERC solve)."""
    rng = np.random.default_rng(seed)
    common = rng.normal(size=n)
    trend = 0.8 * common + 0.6 * rng.normal(size=n)          # moderate vol
    quality = 2.0 * (0.5 * common + 0.9 * rng.normal(size=n))  # high vol
    value = 0.4 * (0.3 * common + 0.95 * rng.normal(size=n))  # low vol
    return pd.DataFrame({"trend": trend, "quality": quality, "value": value})


def _variance_shares(bucket_df: pd.DataFrame, weights: dict) -> dict:
    cols = list(bucket_df.columns)
    w = np.array([weights[c] for c in cols])
    S = bucket_df.cov().values
    Sw = S @ w
    total = float(w @ Sw)
    return {c: float(w[i] * Sw[i] / total) for i, c in enumerate(cols)}


def test_erc_weights_sum_to_one():
    w = _erc_weights(_corr_buckets(), _DEFAULT_WEIGHTS["composite"])
    assert abs(sum(w.values()) - 1.0) < 1e-9


def test_erc_contributions_are_equal_and_sum_to_100pct():
    """The load-bearing assertion (per the mandate): the ERC solution's variance
    contributions sum to 100% AND are equal (spread ~ 0)."""
    df = _corr_buckets()
    w = _erc_weights(df, _DEFAULT_WEIGHTS["composite"])
    contribs = _variance_shares(df, w)
    assert abs(sum(contribs.values()) - 1.0) < 1e-9
    vals = list(contribs.values())
    k = len(vals)
    assert max(vals) - min(vals) < 1e-4               # spread ~ 0
    for v in vals:
        assert abs(v - 1.0 / k) < 1e-4


def test_erc_high_vol_bucket_gets_lower_weight():
    """The high-dispersion bucket ('quality' here) must receive the SMALLEST weight
    so its contribution is pulled down to parity."""
    w = _erc_weights(_corr_buckets(), _DEFAULT_WEIGHTS["composite"])
    assert w["quality"] < w["trend"]
    assert w["quality"] < w["value"]


def test_erc_degenerate_covariance_falls_back_to_nominal():
    """A constant (zero-variance) bucket makes the covariance degenerate; the solver
    must fall back to the nominal config weights renormalized over present buckets,
    not divide by zero."""
    df = pd.DataFrame({"trend": np.linspace(-1, 1, 50),
                       "quality": [0.0] * 50,          # degenerate
                       "value": np.linspace(1, -1, 50)})
    w = _erc_weights(df, _DEFAULT_WEIGHTS["composite"])
    assert abs(sum(w.values()) - 1.0) < 1e-9
    # Fallback = the nominal config composite weights renormalized over the present
    # buckets (the config thirds are 0.3334/0.3333/0.3333, not exactly 1/3).
    nominal = _DEFAULT_WEIGHTS["composite"]
    tot = sum(nominal.values())
    for b in ("trend", "quality", "value"):
        assert abs(w[b] - nominal[b] / tot) < 1e-9


def test_erc_is_deterministic():
    df = _corr_buckets()
    assert _erc_weights(df, _DEFAULT_WEIGHTS["composite"]) == \
        _erc_weights(df.copy(), _DEFAULT_WEIGHTS["composite"])


def test_build_scores_composite_has_equal_bucket_contributions():
    """End-to-end: on a realistic multi-sector universe the shipped composite carries
    ~equal variance shares across Trend/Quality/Value (the ERC promise, in situ)."""
    rng = np.random.default_rng(3)
    n = 220
    rows = []
    sectors = ["Alpha", "Beta", "Gamma", "Delta"]
    for i in range(n):
        rows.append({
            "ticker": f"T{i:03d}", "name": f"T{i}", "sector": sectors[i % 4], "cik": "",
            "ret_12_1": rng.normal(), "ret_13w": rng.normal(),
            "volatility_26w": abs(rng.normal()) + 0.1,
            "pe_ratio": abs(rng.normal()) * 10 + 5, "ev_ebitda": abs(rng.normal()) * 8 + 4,
            "pb_ratio": abs(rng.normal()) * 3 + 1, "dividend_yield": abs(rng.normal()) * 0.02,
            "net_payout_yield": rng.normal() * 0.03,
            "roe": rng.normal() * 0.1, "oper_margin_ttm": rng.normal() * 0.1,
            "fcf_margin_ttm": rng.normal() * 0.1, "roic": rng.normal() * 0.1,
            "gpa": abs(rng.normal()) * 0.3, "debt_equity": abs(rng.normal()),
            "beta": abs(rng.normal()) + 0.5,
        })
    scored = build_scores(pd.DataFrame(rows))
    buckets = pd.DataFrame({b: _restandardize(scored[f"{b}_score"]) for b in _COMPOSITE_BUCKETS})
    w = _erc_weights(buckets, _DEFAULT_WEIGHTS["composite"])
    shares = _variance_shares(buckets, w)
    assert max(shares.values()) - min(shares.values()) < 1e-3


# -- Config schema validation --------------------------------------------------

def test_shipped_config_is_valid_and_loads():
    """The committed config/scoring.yml must pass deep validation (so the pipeline
    uses IT, not the fallback) and match the M2 schema."""
    w = load_weights()
    assert set(w["composite"]) == {"trend", "quality", "value"}
    assert set(w["subfactors"]) == {"trend", "quality", "value", "risk"}
    assert set(w["subfactors"]["value"]) == {"pe_ratio", "ev_ebitda", "net_payout_yield"}
    assert set(w["subfactors"]["quality"]) == {"roe", "oper_margin_ttm", "fcf_margin_ttm", "gpa"}
    assert set(w["subfactors"]["trend"]) == {"ret_12_1"}


def test_deep_invalid_config_falls_back(tmp_path):
    """A structurally-wrong config (mistyped sub-factor key) must fall back to the
    equal-weight default, not pass shallow validation and later KeyError."""
    bad = tmp_path / "scoring.yml"
    bad.write_text(
        "composite: {trend: 0.3334, quality: 0.3333, value: 0.3333}\n"
        "subfactors:\n"
        "  trend: {ret_12_1: 1.0}\n"
        "  quality: {roe: 0.25, oper_margin_ttm: 0.25, fcf_margin_ttm: 0.25, gpa: 0.25}\n"
        "  value: {pe_ratio: 0.34, ev_ebitda: 0.33, net_payout_yield: 0.33}\n"
        "  risk: {volatility_26w: 0.34, debt_TYPO: 0.33, beta: 0.33}\n",  # debt_equity mistyped
        encoding="utf-8")
    assert load_weights(bad) == _DEFAULT_WEIGHTS


def test_config_with_risk_in_composite_is_rejected(tmp_path):
    """A config that leaves the old 4-key composite (risk still in the sum) must NOT
    validate against the M2 schema -- it falls back rather than silently scoring the
    old way."""
    bad = tmp_path / "scoring.yml"
    bad.write_text(
        "composite: {trend: 0.25, quality: 0.25, value: 0.25, risk: 0.25}\n"  # risk in composite
        "subfactors:\n"
        "  trend: {ret_12_1: 1.0}\n"
        "  quality: {roe: 0.25, oper_margin_ttm: 0.25, fcf_margin_ttm: 0.25, gpa: 0.25}\n"
        "  value: {pe_ratio: 0.34, ev_ebitda: 0.33, net_payout_yield: 0.33}\n"
        "  risk: {volatility_26w: 0.34, debt_equity: 0.33, beta: 0.33}\n",
        encoding="utf-8")
    assert load_weights(bad) == _DEFAULT_WEIGHTS


def test_load_weights_falls_back_when_file_missing(tmp_path):
    assert load_weights(tmp_path / "nope.yml") == _DEFAULT_WEIGHTS
