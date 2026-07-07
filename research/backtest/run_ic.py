"""
CLI: BACKTEST / IC pipeline for the Stock Selection Dashboard.

Run from the REPO ROOT:

    python -m research.backtest.run_ic                    # price factors only
    python -m research.backtest.run_ic --with-fundamentals# + PIT value/quality

Pipeline
--------
  1. Load the drift-proof TR-close panel base-first from the canonical archive
     (READ-ONLY) and derive the month-end rebalance calendar.
  2. For each testable rebalance t: compute the product-definition price signals,
     turn each into a raw and a sector-neutral cross-sectional z (product
     primitives), and the 21d / 63d forward returns.
  3. Metrics per factor × variant × horizon: IC series, Newey-West t, block-
     bootstrap CI, quintile spreads, regime splits, IC decay.
  4. Membership (Interface M): if present, also compute filtered IC and report
     the filtered/unfiltered gap; if absent, run unfiltered + flag the report.
  5. Fundamentals (Interface P, --with-fundamentals): if the PIT panel is present,
     add value/quality signals; else skip with a message.
  6. Composite experiments: product / variance-share / literature weights over
     the buckets present, scored on the same slices.
  7. Write CSVs + REPORT.md to research/results/.

Outputs (research/results/):
  ic_summary.csv        -- one row per factor × variant × horizon (the IC table)
  ic_timeseries.csv     -- the monthly IC series (long form)
  quintile_spreads.csv  -- per-rebalance Q1..Q5 + spread per factor
  regime_ic.csv         -- regime IC means
  composite_ic.csv      -- composite scheme IC + Q5-Q1
  membership_bias.csv   -- filtered vs unfiltered (only when membership present)
  REPORT.md             -- the human-facing report (caveats first)
"""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

import numpy as np
import pandas as pd

from research.backtest import composites, forward, metrics, provenance, report, signals
from research.backtest.membership import load_membership
from research.backtest.panel import load_panel, testable_rebalances

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s -- %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("run_ic")

_RESULTS = Path(__file__).resolve().parents[1] / "results"

# Forward horizons: 21d primary (matches the monthly rebalance), 63d secondary.
HORIZONS = (21, 63)
# Buckets and their member signals for the composite (price side always present).
PRICE_BUCKETS = {
    "trend": ("ret_12_1", "ret_13w"),
    "risk": ("volatility_26w", "beta_52w"),
}


# --------------------------------------------------------------------------- #
# Cross-section assembly
# --------------------------------------------------------------------------- #
def _build_slices(panel, market_weekly_ret, fundamentals):
    """For every testable rebalance date, compute raw signals, both score variants
    (raw z / sector-neutral z) per factor, and forward returns per horizon.

    Returns a dict of stacked structures keyed by date:
      raw[t]        -> DataFrame(ticker x all factors) of raw signal values
      score_raw[t][factor]     -> Series (gaussian-rank z)
      score_neut[t][factor]    -> Series (sector-neutral z)
      fwd[t][h]                -> Series forward return
    plus the list of factor names actually present.
    """
    dates = testable_rebalances(panel.close, panel.rebalances)
    logger.info("Testable rebalance slices: %d (%s -> %s)",
                len(dates), dates.min().date() if len(dates) else "n/a",
                dates.max().date() if len(dates) else "n/a")

    factor_names = list(signals.PRICE_FACTORS)
    fund_present = False
    if fundamentals is not None and fundamentals.available:
        from research.backtest.fundamentals_signals import FUNDAMENTAL_SIGNALS
        factor_names = factor_names + list(FUNDAMENTAL_SIGNALS)
        fund_present = True

    raw, score_raw, score_neut, fwd = {}, {}, {}, {}
    for t in dates:
        rw = signals.compute_price_signals(panel.close, t, market_weekly_ret)
        if fund_present:
            frow = fundamentals.signals_at(t, panel.close.loc[t])
            if not frow.empty:
                # Union index (price universe ∪ fundamentals coverage).
                rw = rw.reindex(rw.index.union(frow.index))
                for c in frow.columns:
                    rw[c] = frow[c].reindex(rw.index)
        raw[t] = rw

        sr, sn = {}, {}
        for f in factor_names:
            if f not in rw.columns:
                continue
            direction = signals.FACTOR_DIRECTION.get(f, +1)  # fundamentals are +1
            sr[f] = signals.to_score(rw[f], direction, sectors=None)
            sn[f] = signals.to_score(rw[f], direction, sectors=panel.sectors)
        score_raw[t] = sr
        score_neut[t] = sn

        fwd[t] = {h: forward.forward_return(panel.close, t, h) for h in HORIZONS}

    return {
        "dates": dates, "raw": raw, "score_raw": score_raw,
        "score_neut": score_neut, "fwd": fwd, "factor_names": factor_names,
        "fund_present": fund_present,
    }


def _ic_series(score_by_date: dict, fwd_by_date: dict, factor: str, horizon: int,
               eligible=None) -> pd.Series:
    """Monthly IC series for one factor/horizon. ``eligible`` (optional) is a
    callable t -> set/list of tickers kept (membership filter)."""
    out = {}
    for t, scores in score_by_date.items():
        s = scores.get(factor)
        if s is None:
            continue
        f = fwd_by_date[t][horizon]
        if eligible is not None:
            keep = eligible(t, list(s.dropna().index))
            s = s.reindex(keep)
            f = f.reindex(keep)
        out[t] = metrics.spearman_ic(s, f)
    return pd.Series(out).sort_index()


# --------------------------------------------------------------------------- #
# Main
# --------------------------------------------------------------------------- #
def run(with_fundamentals: bool = False) -> dict:
    _RESULTS.mkdir(parents=True, exist_ok=True)

    panel = load_panel(period="max")
    if panel.close.empty:
        raise RuntimeError("Empty price panel -- archive unreachable or no data.")

    # Market proxy for beta: prefer px_spy_daily. SPY is usually NOT in the S&P
    # constituent universe, so read it separately from the archive and use it as
    # the proxy column; fall back to the equal-weight universe mean if unavailable.
    market_weekly_ret, proxy_label = _market_proxy(panel)

    membership = load_membership()
    fundamentals = None
    if with_fundamentals:
        from research.backtest.fundamentals_signals import load_fundamentals
        fundamentals = load_fundamentals()
        if not fundamentals.available:
            logger.warning("--with-fundamentals set but PIT panel unavailable; "
                           "running price-only.")

    slices = _build_slices(panel, market_weekly_ret, fundamentals)
    dates = slices["dates"]
    if len(dates) == 0:
        raise RuntimeError("No testable rebalance slices (insufficient history).")

    # ---- IC table: factor × variant × horizon --------------------------- #
    ic_rows, ic_ts_records = [], []
    for f in slices["factor_names"]:
        for variant, sd in (("raw", slices["score_raw"]), ("neutral", slices["score_neut"])):
            for h in HORIZONS:
                ic = _ic_series(sd, slices["fwd"], f, h)
                summ = metrics.ic_summary(ic)
                ci = metrics.block_bootstrap_ci(ic)
                # This main IC table is computed WITHOUT the membership filter
                # (eligible=None above) -- the ritual §7 rule 3 filter label. The
                # filtered comparison lives only in the pre-inclusion-bias table.
                ic_rows.append({"factor": f, "variant": variant, "horizon": h,
                                "membership": "unfiltered", "ci": ci, **summ})
                for t, v in ic.items():
                    ic_ts_records.append({"date": t.date(), "factor": f,
                                          "variant": variant, "horizon": h, "ic": v})

    # ---- Quintile / regime / decay: sector-neutral, 21d ----------------- #
    quintile_rows, regime_rows, decay_rows, quintile_spread_frames = [], [], [], {}
    for f in slices["factor_names"]:
        scores21 = {t: slices["score_neut"][t][f] for t in dates
                    if f in slices["score_neut"][t]}
        fwd21 = {t: slices["fwd"][t][21] for t in dates}
        fwd63 = {t: slices["fwd"][t][63] for t in dates}
        spread_df = metrics.quintile_spread_series(scores21, fwd21)
        quintile_spread_frames[f] = spread_df
        qstats = metrics.quintile_stats(spread_df)
        turnover = metrics.top_quintile_turnover(scores21)
        quintile_rows.append({"factor": f, "turnover": turnover, **qstats})

        ic21 = _ic_series(slices["score_neut"], slices["fwd"], f, 21)
        ic63 = _ic_series(slices["score_neut"], slices["fwd"], f, 63)
        reg = metrics.regime_ic_means(ic21)
        regime_rows.append({"factor": f, **reg})
        decay_rows.append({"factor": f,
                           "ic_21": float(ic21.mean()) if len(ic21.dropna()) else float("nan"),
                           "n_21": int(ic21.notna().sum()),
                           "ic_63": float(ic63.mean()) if len(ic63.dropna()) else float("nan"),
                           "n_63": int(ic63.notna().sum())})

    # ---- Membership: filtered vs unfiltered (key factors, 21d neutral) --- #
    bias_rows, avg_excluded = [], float("nan")
    if membership.available:
        excl = [membership.count_excluded(t, list(panel.close.columns)) for t in dates]
        avg_excluded = float(np.mean(excl)) if excl else float("nan")
        key_factors = [f for f in ("ret_12_1", "ret_13w", "volatility_26w", "beta_52w")
                       if f in slices["factor_names"]]
        for f in key_factors:
            ic_unf = _ic_series(slices["score_neut"], slices["fwd"], f, 21)
            ic_fil = _ic_series(slices["score_neut"], slices["fwd"], f, 21,
                                eligible=membership.eligible)
            m_unf = float(ic_unf.mean()) if len(ic_unf.dropna()) else float("nan")
            m_fil = float(ic_fil.mean()) if len(ic_fil.dropna()) else float("nan")
            bias_rows.append({"factor": f,
                              "ic_filtered": m_fil, "n_filtered": int(ic_fil.notna().sum()),
                              "ic_unfiltered": m_unf, "n_unfiltered": int(ic_unf.notna().sum()),
                              "bias": m_unf - m_fil})

    # ---- Composite experiments ------------------------------------------ #
    composite_rows, composite_weights, buckets_present, vs_note = _composites(
        panel, slices)

    # ---- Write CSVs ------------------------------------------------------ #
    _write_csvs(ic_rows, ic_ts_records, quintile_spread_frames, regime_rows,
                composite_rows, bias_rows)

    # ---- Provenance pin (ritual §7 rule 2): the exact input identities this
    # result is a function of, so a bit-repro check is a block comparison. ---- #
    _repo_root = Path(__file__).resolve().parents[2]
    _edgar_panel = Path(__file__).resolve().parents[1] / "data" / "edgar_pit_panel.csv.gz"
    prov = provenance.gather(_repo_root, "config/universe.csv", _edgar_panel)

    # ---- Report ---------------------------------------------------------- #
    median_std = float(np.nanmedian([r["std"] for r in ic_rows if r["std"] == r["std"]]))
    median_ic_for_t2 = float(np.nanmedian(
        [r["ic_for_t2"] for r in ic_rows if r["ic_for_t2"] == r["ic_for_t2"]]))
    ctx = {
        "provenance": prov,
        "panel_start": panel.close.index.min().date(),
        "panel_end": panel.close.index.max().date(),
        "n_bars": len(panel.close.index),
        "n_slices": len(dates),
        "universe_n": panel.close.shape[1],
        "dropped": panel.dropped,
        "market_proxy_label": proxy_label,
        "membership_available": membership.available,
        "avg_excluded": avg_excluded,
        "median_ic_std": median_std,
        "median_ic_for_t2": median_ic_for_t2,
        "with_fundamentals": slices["fund_present"],
        "ic_rows": ic_rows,
        "bias_rows": bias_rows,
        "quintile_rows": quintile_rows,
        "regime_rows": regime_rows,
        "decay_rows": decay_rows,
        "buckets_present": buckets_present,
        "composite_weights": composite_weights,
        "composite_rows": composite_rows,
        "variance_share_note": vs_note,
    }
    report_md = report.build_report(ctx)
    (_RESULTS / "REPORT.md").write_text(report_md, encoding="utf-8")
    logger.info("Wrote REPORT.md and CSVs to %s", _RESULTS)
    return ctx


def _market_proxy(panel):
    """Weekly market return series for beta + a label. Reads px_spy_daily from the
    archive as the proxy (SPY is not a constituent), falling back to the equal-
    weight universe mean if SPY is unreachable."""
    spy_close = _read_spy_close(panel.close.index.min(), panel.close.index.max())
    if spy_close is not None and not spy_close.dropna().empty:
        tmp = panel.close.copy()
        tmp["SPY"] = spy_close.reindex(tmp.index)
        mw, label = signals.market_weekly_returns(tmp, proxy_col="SPY")
        return mw, label
    logger.warning("px_spy_daily unavailable -- using equal-weight universe proxy for beta.")
    return signals.market_weekly_returns(panel.close, proxy_col=None)


def _read_spy_close(start, end):
    """Read the SPY total-return close from the archive (READ-ONLY). Returns a
    Series or None. Separate from the universe read because SPY is the market
    proxy, not a scored constituent."""
    try:
        from collectors.price.consumer import load_ohlcv_base_first  # noqa: PLC0415
    except ImportError:
        return None

    def _fb(missing, period=None):
        return {"Close": pd.DataFrame()}

    ohlcv, src = load_ohlcv_base_first(["SPY"], fetch_fallback=_fb, period="max",
                                       normalize_currency=False)
    close = ohlcv.get("Close", pd.DataFrame())
    if "SPY" not in close.columns:
        return None
    return close["SPY"]


def _composites(panel, slices):
    """Run the three weighting schemes over the buckets present and score their
    composites on the sector-neutral, 21d slices.

    Bucket score at t = the (already sector-neutral z) mean of its member signals'
    neutral scores. Only buckets with >=1 member signal present are used."""
    buckets = dict(PRICE_BUCKETS)
    if slices["fund_present"]:
        from research.backtest.fundamentals_signals import FUNDAMENTAL_BUCKETS
        buckets.update(FUNDAMENTAL_BUCKETS)

    # Bucket score per date = mean of the member neutral z's present.
    bucket_scores_by_date = {}
    for t in slices["dates"]:
        sn = slices["score_neut"][t]
        row = {}
        for b, members in buckets.items():
            cols = [sn[m] for m in members if m in sn]
            if cols:
                # mean across present member z's; all-NaN row -> NaN (mean's default
                # skipna leaves an all-missing row NaN, which is what we want).
                row[b] = pd.concat(cols, axis=1).mean(axis=1)
        if row:
            bucket_scores_by_date[t] = pd.DataFrame(row)

    if not bucket_scores_by_date:
        return [], {}, [], None

    # Buckets present = those appearing in every slice's frame (intersection).
    present = sorted(set.intersection(
        *[set(df.columns) for df in bucket_scores_by_date.values()]))
    if not present:
        return [], {}, [], None

    # Stack all bucket-score rows across dates for the variance-share solve.
    stacked = pd.concat([df[present] for df in bucket_scores_by_date.values()],
                        ignore_index=True).dropna()

    from src.lib.scoring import load_weights  # noqa: PLC0415
    prod_comp = load_weights()["composite"]
    schemes = {
        "product": composites.product_weights(prod_comp, present),
        "variance_share": composites.solve_variance_share(stacked),
        "literature": composites.literature_weights(present),
    }

    # Variance-share correctness note (contributions sum to ~100%, ~equal).
    contribs = composites.variance_contributions(stacked, schemes["variance_share"])
    total = sum(contribs.values())
    spread = (max(contribs.values()) - min(contribs.values())) if contribs else float("nan")
    vs_note = (f"contributions sum to {total * 100:.1f}%, "
               f"max-min share spread {spread * 100:.2f}pp (equal-contribution target).")

    composite_rows = []
    for name, w in schemes.items():
        comp_scores = {t: composites.composite_score(df, w)
                       for t, df in bucket_scores_by_date.items()}
        ic = _composite_ic(comp_scores, slices["fwd"], 21)
        summ = metrics.ic_summary(ic)
        fwd21 = {t: slices["fwd"][t][21] for t in comp_scores}
        spread_df = metrics.quintile_spread_series(comp_scores, fwd21)
        qstats = metrics.quintile_stats(spread_df)
        composite_rows.append({"scheme": name, "n": summ["n"], "mean": summ["mean"],
                               "t": summ["t"], "t_nw": summ["t_nw"],
                               "ann_return": qstats["ann_return"]})
    return composite_rows, schemes, present, vs_note


def _composite_ic(comp_scores: dict, fwd_by_date: dict, horizon: int) -> pd.Series:
    """Monthly IC series for a composite score dict (score already a Series/date)."""
    out = {}
    for t, s in comp_scores.items():
        out[t] = metrics.spearman_ic(s, fwd_by_date[t][horizon])
    return pd.Series(out).sort_index()


def _write_csvs(ic_rows, ic_ts_records, quintile_spread_frames, regime_rows,
                composite_rows, bias_rows):
    """Persist every result table to research/results/ as CSV."""
    # IC summary.
    ic_df = pd.DataFrame([{k: v for k, v in r.items() if k != "ci"} for r in ic_rows])
    ic_df["ci_lo"] = [r["ci"][0] for r in ic_rows]
    ic_df["ci_hi"] = [r["ci"][1] for r in ic_rows]
    ic_df.to_csv(_RESULTS / "ic_summary.csv", index=False)

    pd.DataFrame(ic_ts_records).to_csv(_RESULTS / "ic_timeseries.csv", index=False)

    # Quintile spreads (long).
    q_records = []
    for f, df in quintile_spread_frames.items():
        for t, row in df.iterrows():
            q_records.append({"date": t.date(), "factor": f, **row.to_dict()})
    pd.DataFrame(q_records).to_csv(_RESULTS / "quintile_spreads.csv", index=False)

    # Regime IC (flatten).
    reg_records = []
    for r in regime_rows:
        rec = {"factor": r["factor"]}
        for reg in metrics.REGIMES:
            rec[f"{reg}_mean"] = r[reg]["mean"]
            rec[f"{reg}_n"] = r[reg]["n"]
        reg_records.append(rec)
    pd.DataFrame(reg_records).to_csv(_RESULTS / "regime_ic.csv", index=False)

    pd.DataFrame(composite_rows).to_csv(_RESULTS / "composite_ic.csv", index=False)
    if bias_rows:
        pd.DataFrame(bias_rows).to_csv(_RESULTS / "membership_bias.csv", index=False)


def main() -> None:
    parser = argparse.ArgumentParser(description="Stock Selection backtest / IC framework")
    parser.add_argument("--with-fundamentals", action="store_true",
                        help="Include PIT value/quality signals (needs the EDGAR panel).")
    args = parser.parse_args()
    run(with_fundamentals=args.with_fundamentals)


if __name__ == "__main__":
    main()
