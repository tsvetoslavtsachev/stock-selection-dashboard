# Stock Selection Dashboard — Backtest / IC Report

_Generated 2026-07-06 23:34 · framework: research/backtest · READ-ONLY on the canonical price-archive._

## Read this first — caveats

- **One 6-year window, not eternal truth.** Panel 2020-06-29 → 2026-07-02 (1510 daily bars). 60 monthly test slices. A single regime-heavy window (a 2022 bear inside a 2020s tech cycle) can flatter or bury any factor; treat every number as conditional on this window.
- **Statistical power.** With n=60 monthly ICs and a typical IC std of ~0.113, a mean |IC| of about **0.030** is needed for a plain t=2. Smaller mean ICs are not resolvable at this n — a near-zero t is *absence of power*, not proof of no signal.
- **Survivorship (dead names absent).** The panel is today's index read back through history; delisted/removed names are simply not in the archive. This inflates any factor that co-moves with *survival* — momentum and low-volatility most (a name that blew up is missing from the bad-momentum, high-vol tail), quality mildly, deep-value the least (cheap survivors and cheap casualties both missing). Read positive trend/low-risk IC as an UPPER bound.
- **Membership filter: ACTIVE.** On average **40.1** of 496 names are excluded per slice as not-yet-members (point-in-time index membership). Key factors are reported BOTH filtered and unfiltered below; the gap is the pre-inclusion-bias measurement.
- **Static GICS sectors.** Sector-neutralization uses the CURRENT sector from universe.csv for all history; historical reclassifications are ignored by convention. A name that changed sector is neutralized against the wrong peer group in its earlier years (a small, second-order effect).
- **Dropped (no base series).** 7 universe tickers were not served from the archive base and were excluded (no yfinance fallback in research). First few: BNY, ECHO, FDXF, FLEX, HONA, MRVL, VEEV.

- **Market proxy for beta:** SPY (px_spy_daily total-return).

## Information coefficient (Spearman, monthly)

Per factor × variant (raw / sector-neutral) × forward horizon. `t` is the plain t-stat, `t_NW` the Newey-West (lag 3) autocorrelation-adjusted t. `CI95` is the block-bootstrap (block=3, 2000 resamples, fixed seed) 95% CI of the mean IC.

| factor | variant | horizon | n | mean IC | std | t | t_NW | %pos | CI95 |
|---|---|---|---:|---:|---:|---:|---:|---:|---|
| ret_12_1 | raw | 21d | 60 | 0.0200 | 0.200 | 0.77 | 1.00 | 61.7% | [-0.021, 0.060] |
| ret_12_1 | raw | 63d | 58 | 0.0409 | 0.155 | 2.01 | 1.50 | 62.1% | [-0.011, 0.091] |
| ret_12_1 | neutral | 21d | 60 | 0.0155 | 0.167 | 0.72 | 0.86 | 60.0% | [-0.022, 0.051] |
| ret_12_1 | neutral | 63d | 58 | 0.0315 | 0.128 | 1.87 | 1.37 | 65.5% | [-0.013, 0.075] |
| ret_13w | raw | 21d | 60 | -0.0049 | 0.181 | -0.21 | -0.26 | 46.7% | [-0.045, 0.032] |
| ret_13w | raw | 63d | 58 | -0.0036 | 0.164 | -0.17 | -0.14 | 55.2% | [-0.050, 0.045] |
| ret_13w | neutral | 21d | 60 | 0.0046 | 0.150 | 0.24 | 0.30 | 56.7% | [-0.028, 0.036] |
| ret_13w | neutral | 63d | 58 | -0.0012 | 0.134 | -0.07 | -0.05 | 51.7% | [-0.042, 0.045] |
| volatility_26w | raw | 21d | 60 | -0.0263 | 0.241 | -0.84 | -0.90 | 40.0% | [-0.085, 0.034] |
| volatility_26w | raw | 63d | 58 | -0.0521 | 0.226 | -1.75 | -1.25 | 36.2% | [-0.134, 0.025] |
| volatility_26w | neutral | 21d | 60 | -0.0149 | 0.192 | -0.60 | -0.63 | 41.7% | [-0.063, 0.034] |
| volatility_26w | neutral | 63d | 58 | -0.0335 | 0.176 | -1.45 | -1.05 | 43.1% | [-0.098, 0.026] |
| beta_52w | raw | 21d | 60 | -0.0362 | 0.277 | -1.01 | -1.09 | 48.3% | [-0.105, 0.038] |
| beta_52w | raw | 63d | 58 | -0.0664 | 0.274 | -1.85 | -1.32 | 39.7% | [-0.163, 0.030] |
| beta_52w | neutral | 21d | 60 | -0.0229 | 0.206 | -0.86 | -0.90 | 48.3% | [-0.075, 0.031] |
| beta_52w | neutral | 63d | 58 | -0.0457 | 0.198 | -1.76 | -1.24 | 43.1% | [-0.120, 0.025] |
| ep | raw | 21d | 60 | 0.0240 | 0.106 | 1.75 | 1.92 | 55.0% | [-0.000, 0.049] |
| ep | raw | 63d | 58 | 0.0314 | 0.099 | 2.41 | 1.81 | 60.3% | [-0.002, 0.065] |
| ep | neutral | 21d | 60 | 0.0117 | 0.091 | 0.99 | 0.86 | 48.3% | [-0.014, 0.037] |
| ep | neutral | 63d | 58 | 0.0159 | 0.093 | 1.31 | 0.86 | 56.9% | [-0.018, 0.049] |
| bp | raw | 21d | 60 | 0.0082 | 0.143 | 0.44 | 0.50 | 48.3% | [-0.024, 0.040] |
| bp | raw | 63d | 58 | 0.0234 | 0.127 | 1.41 | 0.95 | 48.3% | [-0.020, 0.069] |
| bp | neutral | 21d | 60 | 0.0112 | 0.117 | 0.74 | 0.70 | 53.3% | [-0.018, 0.041] |
| bp | neutral | 63d | 58 | 0.0235 | 0.112 | 1.60 | 1.01 | 55.2% | [-0.016, 0.065] |
| fcf_yield | raw | 21d | 60 | 0.0171 | 0.124 | 1.07 | 1.16 | 55.0% | [-0.012, 0.047] |
| fcf_yield | raw | 63d | 58 | 0.0156 | 0.118 | 1.00 | 0.73 | 58.6% | [-0.026, 0.059] |
| fcf_yield | neutral | 21d | 60 | 0.0099 | 0.101 | 0.76 | 0.75 | 53.3% | [-0.015, 0.037] |
| fcf_yield | neutral | 63d | 58 | 0.0089 | 0.104 | 0.66 | 0.45 | 50.0% | [-0.027, 0.050] |
| net_payout_yield | raw | 21d | 60 | 0.0256 | 0.112 | 1.78 | 2.02 | 53.3% | [0.000, 0.052] |
| net_payout_yield | raw | 63d | 58 | 0.0363 | 0.101 | 2.73 | 1.97 | 56.9% | [0.004, 0.073] |
| net_payout_yield | neutral | 21d | 60 | 0.0199 | 0.095 | 1.62 | 1.54 | 53.3% | [-0.005, 0.046] |
| net_payout_yield | neutral | 63d | 58 | 0.0263 | 0.094 | 2.14 | 1.44 | 56.9% | [-0.006, 0.061] |
| gpa | raw | 21d | 60 | 0.0044 | 0.109 | 0.31 | 0.30 | 53.3% | [-0.025, 0.032] |
| gpa | raw | 63d | 58 | -0.0065 | 0.114 | -0.43 | -0.27 | 50.0% | [-0.051, 0.034] |
| gpa | neutral | 21d | 60 | 0.0007 | 0.105 | 0.05 | 0.05 | 50.0% | [-0.027, 0.027] |
| gpa | neutral | 63d | 58 | -0.0111 | 0.102 | -0.82 | -0.52 | 44.8% | [-0.050, 0.027] |
| roe | raw | 21d | 60 | 0.0058 | 0.082 | 0.55 | 0.66 | 50.0% | [-0.014, 0.024] |
| roe | raw | 63d | 58 | -0.0085 | 0.075 | -0.86 | -0.66 | 43.1% | [-0.033, 0.015] |
| roe | neutral | 21d | 60 | -0.0022 | 0.062 | -0.28 | -0.37 | 45.0% | [-0.015, 0.010] |
| roe | neutral | 63d | 58 | -0.0159 | 0.051 | -2.39 | -1.94 | 34.5% | [-0.032, -0.001] |
| oper_margin | raw | 21d | 60 | -0.0117 | 0.102 | -0.89 | -0.98 | 41.7% | [-0.035, 0.014] |
| oper_margin | raw | 63d | 58 | -0.0266 | 0.088 | -2.32 | -1.71 | 41.4% | [-0.056, 0.004] |
| oper_margin | neutral | 21d | 60 | -0.0092 | 0.092 | -0.78 | -0.88 | 43.3% | [-0.030, 0.013] |
| oper_margin | neutral | 63d | 58 | -0.0265 | 0.079 | -2.55 | -1.94 | 32.8% | [-0.052, -0.000] |
| ev_ebitda_yield | raw | 21d | 60 | 0.0237 | 0.129 | 1.43 | 1.44 | 55.0% | [-0.009, 0.056] |
| ev_ebitda_yield | raw | 63d | 58 | 0.0330 | 0.123 | 2.05 | 1.52 | 55.2% | [-0.009, 0.075] |
| ev_ebitda_yield | neutral | 21d | 60 | 0.0172 | 0.108 | 1.24 | 1.13 | 56.7% | [-0.012, 0.045] |
| ev_ebitda_yield | neutral | 63d | 58 | 0.0202 | 0.103 | 1.49 | 1.01 | 51.7% | [-0.016, 0.058] |

## Pre-inclusion bias — filtered vs unfiltered (21d, sector-neutral)

The IC difference (unfiltered − filtered) is the measurement of pre-inclusion bias for each key factor.

| factor | IC filtered | n_f | IC unfiltered | n_u | bias (unf−filt) |
|---|---:|---:|---:|---:|---:|
| ret_12_1 | 0.0057 | 60 | 0.0155 | 60 | 0.0098 |
| ret_13w | -0.0036 | 60 | 0.0046 | 60 | 0.0082 |
| volatility_26w | -0.0055 | 60 | -0.0149 | 60 | -0.0094 |
| beta_52w | -0.0145 | 60 | -0.0229 | 60 | -0.0084 |

## Quintile portfolios (equal-weight, monthly rebalance, sector-neutral score, 21d)

Q5 = top quintile by score. Spread = Q5 − Q1 forward return, annualized.

| factor | Q5−Q1 ann.ret | ann.vol | max DD | Q5 turnover/mo | n slices |
|---|---:|---:|---:|---:|---:|
| ret_12_1 | 6.6% | 15.1% | -20.4% | 22.7% | 60 |
| ret_13w | 4.5% | 12.9% | -19.5% | 45.7% | 60 |
| volatility_26w | -14.9% | 16.9% | -62.2% | 19.6% | 60 |
| beta_52w | -16.7% | 18.1% | -65.6% | 14.0% | 60 |
| ep | -0.6% | 7.8% | -22.2% | 9.3% | 60 |
| bp | 3.5% | 8.2% | -16.8% | 6.6% | 60 |
| fcf_yield | -0.0% | 9.2% | -20.2% | 9.5% | 60 |
| net_payout_yield | -0.4% | 7.9% | -15.1% | 8.1% | 60 |
| gpa | -1.3% | 10.0% | -26.1% | 3.7% | 60 |
| roe | -4.2% | 6.2% | -26.5% | 5.4% | 60 |
| oper_margin | -7.6% | 7.9% | -36.9% | 3.7% | 60 |
| ev_ebitda_yield | 2.7% | 8.9% | -18.7% | 7.7% | 60 |

## Regime IC means (sector-neutral, 21d)

Windows: late bull (2021-08→2021-12) · bear / rate shock (2022) · AI bull (2023→now).

| factor | late_bull 21H2 (n) | bear 2022 (n) | AI bull 23+ (n) |
|---|---:|---:|---:|
| ret_12_1 | -0.0879 (5) | -0.0033 (12) | 0.0326 (41) |
| ret_13w | -0.0851 (5) | -0.0149 (12) | 0.0107 (41) |
| volatility_26w | 0.0257 (5) | 0.0110 (12) | -0.0319 (41) |
| beta_52w | 0.0217 (5) | 0.0239 (12) | -0.0438 (41) |
| ep | 0.0628 (5) | 0.0477 (12) | -0.0023 (41) |
| bp | 0.0784 (5) | 0.0277 (12) | 0.0053 (41) |
| fcf_yield | 0.0737 (5) | 0.0243 (12) | 0.0012 (41) |
| net_payout_yield | 0.0579 (5) | 0.0462 (12) | 0.0111 (41) |
| gpa | -0.0689 (5) | -0.0081 (12) | 0.0034 (41) |
| roe | -0.0159 (5) | 0.0080 (12) | -0.0080 (41) |
| oper_margin | -0.0242 (5) | -0.0020 (12) | -0.0163 (41) |
| ev_ebitda_yield | 0.0795 (5) | 0.0427 (12) | 0.0075 (41) |

## IC decay (sector-neutral mean IC: 21d vs 63d)

| factor | mean IC 21d (n) | mean IC 63d (n) |
|---|---:|---:|
| ret_12_1 | 0.0155 (60) | 0.0315 (58) |
| ret_13w | 0.0046 (60) | -0.0012 (58) |
| volatility_26w | -0.0149 (60) | -0.0335 (58) |
| beta_52w | -0.0229 (60) | -0.0457 (58) |
| ep | 0.0117 (60) | 0.0159 (58) |
| bp | 0.0112 (60) | 0.0235 (58) |
| fcf_yield | 0.0099 (60) | 0.0089 (58) |
| net_payout_yield | 0.0199 (60) | 0.0263 (58) |
| gpa | 0.0007 (60) | -0.0111 (58) |
| roe | -0.0022 (60) | -0.0159 (58) |
| oper_margin | -0.0092 (60) | -0.0265 (58) |
| ev_ebitda_yield | 0.0172 (60) | 0.0202 (58) |

## Composite weighting experiments (21d, sector-neutral buckets)

Buckets present this run: **quality, risk, trend, value**

Weights per scheme:

| scheme | quality | risk | trend | value |
|---|---:|---:|---:|---:|
| product | 0.250 | 0.250 | 0.250 | 0.250 |
| variance_share | 0.242 | 0.214 | 0.269 | 0.275 |
| literature | 0.300 | 0.150 | 0.400 | 0.150 |

Composite IC (sector-neutral, 21d) and Q5−Q1 by scheme:

| scheme | n | mean IC | t | t_NW | Q5−Q1 ann.ret |
|---|---:|---:|---:|---:|---:|
| product | 60 | 0.0013 | 0.06 | 0.07 | -6.7% |
| variance_share | 60 | 0.0040 | 0.21 | 0.24 | -4.8% |
| literature | 60 | 0.0094 | 0.46 | 0.59 | -2.3% |

_Variance-share solution check: contributions sum to 100.0%, max-min share spread 0.00pp (equal-contribution target)._

---

_Factors, normalization (gaussian_rank, sector_neutralize), and product composite weights are imported from the live pipeline (src/lib/scoring.py, config/scoring.yml) — the backtest scores the SAME code the dashboard ships. Prices are the canonical drift-proof total-return archive._
