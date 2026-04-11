"""
Job: compute_factors
====================
Reads raw price and SEC data, computes factor inputs for every ticker, calls
the scoring engine, and writes the ranked output to:

    data/processed/ranks.csv

Factor inputs extracted here
-----------------------------
From data/raw/prices/{symbol}.json  (Alpha Vantage):
    ret_13w     — 13-week price return (adjusted close)
    ret_26w     — 26-week price return
    ret_52w     — 52-week price return
    volatility_26w — annualised weekly return volatility over 26 weeks

From data/raw/sec/{symbol}/companyfacts.json  (SEC EDGAR):
    revenue_growth_ttm  — trailing 12-month revenue YoY growth (decimal)
    oper_margin_ttm     — trailing 12-month operating income / revenue
    fcf_margin_ttm      — trailing 12-month free cash flow / revenue
    ev_ebit             — placeholder (set to NaN; requires market-cap source)

All missing values are filled with 0 before scoring (see scoring.build_scores).

Usage
-----
    python -m src.jobs.compute_factors
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from src.lib.io_utils import DATA_RAW, DATA_PROCESSED, read_universe, read_json
from src.lib.scoring import build_scores

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("compute_factors")

_PRICES_DIR = DATA_RAW / "prices"
_SEC_DIR = DATA_RAW / "sec"
_OUTPUT = DATA_PROCESSED / "ranks.csv"

# ─── Price helpers ────────────────────────────────────────────────────────────

def _load_adjusted_closes(symbol: str) -> pd.Series | None:
    """
    Load weekly adjusted close prices for *symbol* sorted oldest → newest.
    Returns None if the raw file is missing.
    """
    path = _PRICES_DIR / f"{symbol}.json"
    if not path.exists():
        logger.warning("[%s] Price file not found: %s", symbol, path)
        return None

    try:
        raw = read_json(path)
        ts = raw.get("Weekly Adjusted Time Series", {})
        if not ts:
            logger.warning("[%s] Empty time series in price file", symbol)
            return None

        series = pd.Series(
            {pd.Timestamp(date): float(vals["5. adjusted close"]) for date, vals in ts.items()},
            name=symbol,
        ).sort_index()
        return series
    except Exception as exc:
        logger.error("[%s] Error parsing price file: %s", symbol, exc)
        return None


def _price_features(series: pd.Series) -> dict[str, float]:
    """
    Compute momentum and volatility features from a weekly price series.
    Returns a dict with ret_13w, ret_26w, ret_52w, volatility_26w.
    Uses 0.0 as fallback when there are insufficient bars.
    """
    n = len(series)

    def safe_ret(weeks: int) -> float:
        if n < weeks + 1:
            return 0.0
        past = series.iloc[-(weeks + 1)]
        current = series.iloc[-1]
        if past == 0 or np.isnan(past):
            return 0.0
        return (current / past) - 1.0

    ret_13 = safe_ret(13)
    ret_26 = safe_ret(26)
    ret_52 = safe_ret(52)

    # Annualised volatility from 26 weekly log returns
    if n >= 27:
        weekly_rets = np.log(series.iloc[-26:] / series.iloc[-27:-1].values)
        vol = float(weekly_rets.std() * np.sqrt(52))
    else:
        vol = 0.0

    return {
        "ret_13w": ret_13,
        "ret_26w": ret_26,
        "ret_52w": ret_52,
        "volatility_26w": vol,
    }


# ─── SEC / fundamental helpers ────────────────────────────────────────────────

def _get_us_gaap_values(
    facts: dict,
    concept: str,
    form_filter: tuple[str, ...] = ("10-K", "10-Q"),
    unit: str = "USD",
) -> pd.DataFrame | None:
    """
    Extract a time series of XBRL values for a given US-GAAP concept.

    Returns a DataFrame with columns [end, val] sorted by end date,
    or None if the concept is not present.
    """
    try:
        entries = (
            facts.get("facts", {})
                 .get("us-gaap", {})
                 .get(concept, {})
                 .get("units", {})
                 .get(unit, [])
        )
        if not entries:
            return None
        df = pd.DataFrame(entries)
        df = df[df["form"].isin(form_filter)].copy()
        df["end"] = pd.to_datetime(df["end"])
        df = df.sort_values("end").drop_duplicates(subset="end", keep="last")
        return df[["end", "val"]]
    except Exception as exc:
        logger.debug("Error extracting %s: %s", concept, exc)
        return None


def _ttm_sum(df: pd.DataFrame) -> float | None:
    """
    Sum the last 4 quarterly values from a DataFrame with columns [end, val].
    Returns None if there are fewer than 4 rows.
    """
    quarterly = df[df["val"].notna()].tail(4)
    if len(quarterly) < 4:
        return None
    return float(quarterly["val"].sum())


def _sec_features(symbol: str) -> dict[str, float]:
    """
    Compute quality and value factor inputs from SEC companyfacts.
    Returns a dict with revenue_growth_ttm, oper_margin_ttm, fcf_margin_ttm, ev_ebit.
    Falls back to 0.0 for any metric that cannot be computed.
    """
    path = _SEC_DIR / symbol / "companyfacts.json"
    result: dict[str, float] = {
        "revenue_growth_ttm": 0.0,
        "oper_margin_ttm": 0.0,
        "fcf_margin_ttm": 0.0,
        "ev_ebit": 0.0,  # Cannot compute without market cap; scoring engine handles as 0
    }

    if not path.exists():
        logger.warning("[%s] SEC companyfacts file not found", symbol)
        return result

    try:
        facts = read_json(path)
    except Exception as exc:
        logger.error("[%s] Error reading companyfacts: %s", symbol, exc)
        return result

    # --- Revenue (Revenues or RevenueFromContractWithCustomerExcludingAssessedTax) ---
    rev_df = (
        _get_us_gaap_values(facts, "Revenues")
        or _get_us_gaap_values(facts, "RevenueFromContractWithCustomerExcludingAssessedTax")
        or _get_us_gaap_values(facts, "SalesRevenueNet")
    )

    rev_ttm = _ttm_sum(rev_df) if rev_df is not None else None
    rev_prior: float | None = None

    if rev_df is not None and len(rev_df) >= 8:
        prior_4 = rev_df.tail(8).head(4)
        rev_prior = float(prior_4["val"].sum()) if len(prior_4) == 4 else None

    if rev_ttm and rev_prior and rev_prior != 0:
        result["revenue_growth_ttm"] = (rev_ttm - rev_prior) / abs(rev_prior)

    # --- Operating income ---
    opinc_df = (
        _get_us_gaap_values(facts, "OperatingIncomeLoss")
        or _get_us_gaap_values(facts, "IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest")
    )
    opinc_ttm = _ttm_sum(opinc_df) if opinc_df is not None else None

    if opinc_ttm is not None and rev_ttm and rev_ttm != 0:
        result["oper_margin_ttm"] = opinc_ttm / rev_ttm

    # --- Free cash flow = Operating CF - CapEx ---
    ocf_df = _get_us_gaap_values(facts, "NetCashProvidedByUsedInOperatingActivities")
    capex_df = _get_us_gaap_values(facts, "PaymentsToAcquirePropertyPlantAndEquipment")

    ocf_ttm = _ttm_sum(ocf_df) if ocf_df is not None else None
    capex_ttm = _ttm_sum(capex_df) if capex_df is not None else 0.0

    if ocf_ttm is not None and rev_ttm and rev_ttm != 0:
        fcf = ocf_ttm - (capex_ttm or 0.0)
        result["fcf_margin_ttm"] = fcf / rev_ttm

    return result


# ─── Main ─────────────────────────────────────────────────────────────────────

def run() -> pd.DataFrame:
    """
    Build factor inputs for the full universe, score them, and save ranks.csv.

    Returns the scored/ranked DataFrame.
    """
    universe = read_universe(enabled_only=True)
    rows: list[dict] = []

    for _, row in universe.iterrows():
        symbol = row["ticker"]
        logger.info("[%s] Computing factors", symbol)

        record: dict[str, Any] = {
            "ticker": symbol,
            "name":   row.get("name", ""),
            "sector": row.get("sector", ""),
            "cik":    row.get("cik", ""),
        }

        # Price features
        price_series = _load_adjusted_closes(symbol)
        if price_series is not None:
            record.update(_price_features(price_series))
        else:
            record.update({
                "ret_13w": 0.0, "ret_26w": 0.0,
                "ret_52w": 0.0, "volatility_26w": 0.0,
            })

        # SEC fundamental features
        record.update(_sec_features(symbol))

        rows.append(record)

    if not rows:
        logger.error("No factor data produced — universe may be empty or all files missing")
        return pd.DataFrame()

    df = pd.DataFrame(rows)
    scored = build_scores(df)

    DATA_PROCESSED.mkdir(parents=True, exist_ok=True)
    scored.to_csv(_OUTPUT, index=False)
    logger.info("Saved ranks.csv with %d rows → %s", len(scored), _OUTPUT)

    return scored


def main() -> None:
    run()


if __name__ == "__main__":
    main()
