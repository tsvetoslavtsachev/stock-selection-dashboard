"""
REPORT.md generator -- honest caveats up front, then the numbers with n everywhere.

The report is the human-facing output. Its first job is to keep the reader from
over-reading a six-year, survivorship-tainted, single-window study. So it opens
with the CAVEATS block (statistical power, survivorship direction, membership-
filter status, static GICS, one-window warning) BEFORE any IC table, and every
table carries n next to each number.

Pure string assembly from the result dicts the metrics module produced; it does
no computation of its own except formatting and the stat-power sentence.
"""

from __future__ import annotations

from datetime import datetime


def _fmt(x, nd=4):
    if x is None:
        return "n/a"
    try:
        if x != x:  # NaN
            return "n/a"
    except TypeError:
        return str(x)
    return f"{x:.{nd}f}"


def _pct(x, nd=1):
    if x is None or x != x:
        return "n/a"
    return f"{x * 100:.{nd}f}%"


def build_report(ctx: dict) -> str:
    """Assemble the full REPORT.md from the run context ``ctx`` (see run_ic for the
    keys). Returns the markdown string."""
    L = []
    add = L.append

    add("# Stock Selection Dashboard — Backtest / IC Report")
    add("")
    add(f"_Generated {datetime.now().strftime('%Y-%m-%d %H:%M')} · "
        f"framework: research/backtest · READ-ONLY on the canonical price-archive._")
    add("")

    # ---- Honest caveats FIRST ------------------------------------------- #
    add("## Read this first — caveats")
    add("")
    n_slices = ctx["n_slices"]
    med_std = ctx.get("median_ic_std")
    ic_for_t2 = ctx.get("median_ic_for_t2")
    add(f"- **One 6-year window, not eternal truth.** Panel {ctx['panel_start']} → "
        f"{ctx['panel_end']} ({ctx['n_bars']} daily bars). {n_slices} monthly test "
        f"slices. A single regime-heavy window (a 2022 bear inside a 2020s tech "
        f"cycle) can flatter or bury any factor; treat every number as conditional "
        f"on this window.")
    if ic_for_t2 is not None and ic_for_t2 == ic_for_t2:
        add(f"- **Statistical power.** With n={n_slices} monthly ICs and a typical "
            f"IC std of ~{_fmt(med_std, 3)}, a mean |IC| of about "
            f"**{_fmt(ic_for_t2, 3)}** is needed for a plain t=2. Smaller mean ICs "
            f"are not resolvable at this n — a near-zero t is *absence of power*, "
            f"not proof of no signal.")
    add(f"- **Survivorship (dead names absent).** The panel is today's index read "
        f"back through history; delisted/removed names are simply not in the "
        f"archive. This inflates any factor that co-moves with *survival* — "
        f"momentum and low-volatility most (a name that blew up is missing from "
        f"the bad-momentum, high-vol tail), quality mildly, deep-value the least "
        f"(cheap survivors and cheap casualties both missing). Read positive "
        f"trend/low-risk IC as an UPPER bound.")
    # Membership status.
    if ctx["membership_available"]:
        add(f"- **Membership filter: ACTIVE.** On average "
            f"**{_fmt(ctx['avg_excluded'], 1)}** of {ctx['universe_n']} names are "
            f"excluded per slice as not-yet-members (point-in-time index "
            f"membership). Key factors are reported BOTH filtered and unfiltered "
            f"below; the gap is the pre-inclusion-bias measurement.")
    else:
        add("- **Membership filter: UNFILTERED (pre-inclusion bias).** "
            "`research/membership/membership.csv` was absent at run time, so every "
            "slice used TODAY's constituents regardless of when they joined the "
            "index. A name added after a big run inflates momentum/quality IC in "
            "the years before it was actually a member. Re-run with the file for "
            "the filtered/unfiltered comparison.")
    add("- **Static GICS sectors.** Sector-neutralization uses the CURRENT sector "
        "from universe.csv for all history; historical reclassifications are "
        "ignored by convention. A name that changed sector is neutralized against "
        "the wrong peer group in its earlier years (a small, second-order effect).")
    if ctx.get("dropped"):
        add(f"- **Dropped (no base series).** {len(ctx['dropped'])} universe "
            f"tickers were not served from the archive base and were excluded "
            f"(no yfinance fallback in research). First few: "
            f"{', '.join(ctx['dropped'][:8])}.")
    add("")
    add(f"- **Market proxy for beta:** {ctx['market_proxy_label']}.")
    add("")

    # ---- IC tables ------------------------------------------------------- #
    add("## Information coefficient (Spearman, monthly)")
    add("")
    add("Per factor × variant (raw / sector-neutral) × forward horizon. `t` is the "
        "plain t-stat, `t_NW` the Newey-West (lag 3) autocorrelation-adjusted t. "
        "`CI95` is the block-bootstrap (block=3, 2000 resamples, fixed seed) 95% CI "
        "of the mean IC.")
    add("")
    add("| factor | variant | horizon | n | mean IC | std | t | t_NW | %pos | CI95 |")
    add("|---|---|---|---:|---:|---:|---:|---:|---:|---|")
    for r in ctx["ic_rows"]:
        ci = r.get("ci")
        ci_s = f"[{_fmt(ci[0], 3)}, {_fmt(ci[1], 3)}]" if ci else "n/a"
        add(f"| {r['factor']} | {r['variant']} | {r['horizon']}d | {r['n']} | "
            f"{_fmt(r['mean'])} | {_fmt(r['std'], 3)} | {_fmt(r['t'], 2)} | "
            f"{_fmt(r['t_nw'], 2)} | {_pct(r['pct_pos'] / 100 if r['pct_pos'] == r['pct_pos'] else None)} | {ci_s} |")
    add("")

    # ---- Filtered vs unfiltered (only when membership available) --------- #
    if ctx["membership_available"] and ctx.get("bias_rows"):
        add("## Pre-inclusion bias — filtered vs unfiltered (21d, sector-neutral)")
        add("")
        add("The IC difference (unfiltered − filtered) is the measurement of "
            "pre-inclusion bias for each key factor.")
        add("")
        add("| factor | IC filtered | n_f | IC unfiltered | n_u | bias (unf−filt) |")
        add("|---|---:|---:|---:|---:|---:|")
        for r in ctx["bias_rows"]:
            add(f"| {r['factor']} | {_fmt(r['ic_filtered'])} | {r['n_filtered']} | "
                f"{_fmt(r['ic_unfiltered'])} | {r['n_unfiltered']} | "
                f"{_fmt(r['bias'])} |")
        add("")

    # ---- Quintile portfolios -------------------------------------------- #
    add("## Quintile portfolios (equal-weight, monthly rebalance, sector-neutral score, 21d)")
    add("")
    add("Q5 = top quintile by score. Spread = Q5 − Q1 forward return, annualized.")
    add("")
    add("| factor | Q5−Q1 ann.ret | ann.vol | max DD | Q5 turnover/mo | n slices |")
    add("|---|---:|---:|---:|---:|---:|")
    for r in ctx["quintile_rows"]:
        add(f"| {r['factor']} | {_pct(r['ann_return'])} | {_pct(r['ann_vol'])} | "
            f"{_pct(r['max_drawdown'])} | {_pct(r['turnover'])} | {r['n']} |")
    add("")

    # ---- Regime splits --------------------------------------------------- #
    add("## Regime IC means (sector-neutral, 21d)")
    add("")
    add("Windows: late bull (2021-08→2021-12) · bear / rate shock (2022) · "
        "AI bull (2023→now).")
    add("")
    add("| factor | late_bull 21H2 (n) | bear 2022 (n) | AI bull 23+ (n) |")
    add("|---|---:|---:|---:|")
    for r in ctx["regime_rows"]:
        lb = r["late_bull_2021H2"]; be = r["bear_rate_shock_2022"]; ab = r["ai_bull_2023_plus"]
        add(f"| {r['factor']} | {_fmt(lb['mean'])} ({lb['n']}) | "
            f"{_fmt(be['mean'])} ({be['n']}) | {_fmt(ab['mean'])} ({ab['n']}) |")
    add("")

    # ---- IC decay -------------------------------------------------------- #
    add("## IC decay (sector-neutral mean IC: 21d vs 63d)")
    add("")
    add("| factor | mean IC 21d (n) | mean IC 63d (n) |")
    add("|---|---:|---:|")
    for r in ctx["decay_rows"]:
        add(f"| {r['factor']} | {_fmt(r['ic_21'])} ({r['n_21']}) | "
            f"{_fmt(r['ic_63'])} ({r['n_63']}) |")
    add("")

    # ---- Composite experiments ------------------------------------------ #
    add("## Composite weighting experiments (21d, sector-neutral buckets)")
    add("")
    add(f"Buckets present this run: **{', '.join(ctx['buckets_present'])}**"
        + ("" if ctx["with_fundamentals"] else " (price-only — quality/value wait "
           "on the fundamentals panel).") + "")
    add("")
    add("Weights per scheme:")
    add("")
    add("| scheme | " + " | ".join(ctx["buckets_present"]) + " |")
    add("|---|" + "---:|" * len(ctx["buckets_present"]))
    for scheme, w in ctx["composite_weights"].items():
        add(f"| {scheme} | " + " | ".join(_fmt(w.get(b), 3) for b in ctx["buckets_present"]) + " |")
    add("")
    add("Composite IC (sector-neutral, 21d) and Q5−Q1 by scheme:")
    add("")
    add("| scheme | n | mean IC | t | t_NW | Q5−Q1 ann.ret |")
    add("|---|---:|---:|---:|---:|---:|")
    for r in ctx["composite_rows"]:
        add(f"| {r['scheme']} | {r['n']} | {_fmt(r['mean'])} | {_fmt(r['t'], 2)} | "
            f"{_fmt(r['t_nw'], 2)} | {_pct(r['ann_return'])} |")
    add("")
    if ctx.get("variance_share_note"):
        add(f"_Variance-share solution check: {ctx['variance_share_note']}_")
        add("")

    add("---")
    add("")
    add("_Factors, normalization (gaussian_rank, sector_neutralize), and product "
        "composite weights are imported from the live pipeline (src/lib/scoring.py, "
        "config/scoring.yml) — the backtest scores the SAME code the dashboard "
        "ships. Prices are the canonical drift-proof total-return archive._")
    add("")
    return "\n".join(L)
