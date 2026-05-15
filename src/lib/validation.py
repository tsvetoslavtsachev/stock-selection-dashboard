"""
Data validation and quality scoring for factor inputs.

The dashboard ranks stocks using third-party market/fundamental data. Those
feeds can contain stale values, impossible ratios, split artefacts, or missing
metrics. This module applies conservative sanity ranges before scoring and adds
per-row quality metadata so the frontend can surface trust signals.
"""

from __future__ import annotations

from dataclasses import dataclass
import logging
import math
from typing import Iterable

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ValidationRule:
    """Numeric sanity range for one factor input."""

    min_value: float | None = None
    max_value: float | None = None
    required: bool = True
    invalidates_score: bool = True
    description: str = ""


# Conservative broad ranges. Values outside these bounds are usually not useful
# for cross-sectional ranking and are more likely feed artefacts or non-comparable
# accounting cases than actionable signals.
VALIDATION_RULES: dict[str, ValidationRule] = {
    # Price/trend inputs
    "ret_13w": ValidationRule(-0.95, 5.0, description="13-week return"),
    "ret_26w": ValidationRule(-0.95, 8.0, description="26-week return"),
    "ret_52w": ValidationRule(-0.95, 10.0, description="52-week return"),
    "volatility_26w": ValidationRule(0.0, 2.0, description="annualised 26-week volatility"),

    # Value inputs
    "pe_ratio": ValidationRule(0.0, 300.0, description="P/E ratio"),
    "pb_ratio": ValidationRule(0.0, 100.0, description="P/B ratio"),
    "ev_ebitda": ValidationRule(0.0, 200.0, description="EV/EBITDA"),
    "ev_ebit": ValidationRule(0.0, 300.0, required=False, description="EV/EBIT"),
    "dividend_yield": ValidationRule(0.0, 0.25, required=False, description="dividend yield"),

    # Quality inputs
    "roe": ValidationRule(-1.0, 2.0, description="return on equity"),
    "roic": ValidationRule(-1.0, 2.0, description="return on invested capital"),
    "revenue_growth_ttm": ValidationRule(-1.0, 5.0, required=False, description="revenue growth"),
    "oper_margin_ttm": ValidationRule(-2.0, 2.0, description="operating margin"),
    "gross_margin_ttm": ValidationRule(-2.0, 2.0, required=False, description="gross margin"),
    "fcf_margin_ttm": ValidationRule(-2.0, 2.0, description="free-cash-flow margin"),

    # Risk inputs
    "debt_equity": ValidationRule(0.0, 10.0, description="debt/equity"),
    "beta": ValidationRule(-1.0, 5.0, description="beta"),

    # Informational fields used in the UI/report
    "eps_ttm": ValidationRule(None, None, required=False, invalidates_score=False, description="EPS TTM"),
    "market_cap": ValidationRule(100_000_000.0, 100_000_000_000_000.0, required=False, invalidates_score=False, description="market cap"),
}

# A missing dividend yield normally means no dividend, not bad data. Convert it
# to zero before ranking and do not penalise the quality score.
ZERO_IF_MISSING = {"dividend_yield"}

QUALITY_FIELD_COUNT = sum(1 for r in VALIDATION_RULES.values() if r.invalidates_score)


def validate_factor_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Apply sanity checks and append data quality metadata.

    Returns a copy of *df* with invalid/outlier values replaced by NaN for scoring
    and these extra columns:
      - data_quality_score: [0, 1], higher is better
      - data_quality_flag_count: number of validation flags
      - data_quality_flags: pipe-delimited flag codes, e.g. "pe_ratio_outlier"
    """

    out = df.copy()
    flags: list[list[str]] = [[] for _ in range(len(out))]

    for col in ZERO_IF_MISSING:
        if col in out.columns:
            out[col] = pd.to_numeric(out[col], errors="coerce").fillna(0.0)

    for col, rule in VALIDATION_RULES.items():
        if col not in out.columns:
            if rule.required:
                _add_flag(flags, f"{col}_missing_column")
            continue

        values = pd.to_numeric(out[col], errors="coerce")
        out[col] = values

        missing = values.isna()
        if rule.required and missing.any():
            _add_flag(flags, f"{col}_missing", missing)

        invalid = pd.Series(False, index=out.index)
        if rule.min_value is not None:
            invalid |= values < rule.min_value
        if rule.max_value is not None:
            invalid |= values > rule.max_value

        # Missing values are tracked separately; only non-missing range failures
        # should be labelled as outliers.
        invalid &= ~missing
        if invalid.any():
            _add_flag(flags, f"{col}_outlier", invalid)
            if rule.invalidates_score:
                out.loc[invalid, col] = np.nan

    flag_counts = [len(row_flags) for row_flags in flags]
    out["data_quality_flag_count"] = flag_counts
    out["data_quality_flags"] = ["|".join(row_flags) for row_flags in flags]
    out["data_quality_score"] = [
        _quality_score(row_flags) for row_flags in flags
    ]

    logger.info(
        "Validation: %d rows | %d rows flagged | avg quality %.3f",
        len(out),
        sum(1 for c in flag_counts if c > 0),
        float(pd.Series(out["data_quality_score"]).mean()) if len(out) else math.nan,
    )
    return out


def build_quality_report(df: pd.DataFrame) -> dict:
    """Create an aggregate JSON-serialisable quality report."""

    if df.empty:
        return {
            "total_rows": 0,
            "flagged_rows": 0,
            "low_quality_rows": 0,
            "avg_quality_score": None,
            "flag_counts": {},
            "missing_counts": {},
            "outlier_counts": {},
            "worst_rows": [],
        }

    flags_series = df.get("data_quality_flags", pd.Series([""] * len(df))).fillna("")
    flag_counts: dict[str, int] = {}
    for item in flags_series:
        for flag in str(item).split("|"):
            if not flag:
                continue
            flag_counts[flag] = flag_counts.get(flag, 0) + 1

    missing_counts = {k: v for k, v in flag_counts.items() if k.endswith("_missing") or k.endswith("_missing_column")}
    outlier_counts = {k: v for k, v in flag_counts.items() if k.endswith("_outlier")}

    quality = pd.to_numeric(df.get("data_quality_score", pd.Series([1.0] * len(df))), errors="coerce").fillna(0.0)
    worst_cols = [c for c in ["ticker", "name", "sector", "composite_score", "data_quality_score", "data_quality_flags"] if c in df.columns]
    worst = (
        df.assign(_quality=quality)
        .sort_values(["_quality", "ticker"], ascending=[True, True])
        .head(15)[worst_cols]
        .to_dict(orient="records")
    )

    return {
        "total_rows": int(len(df)),
        "flagged_rows": int(sum(1 for item in flags_series if str(item).strip())),
        "low_quality_rows": int((quality < 0.75).sum()),
        "avg_quality_score": round(float(quality.mean()), 4),
        "min_quality_score": round(float(quality.min()), 4),
        "flag_counts": dict(sorted(flag_counts.items(), key=lambda kv: (-kv[1], kv[0]))),
        "missing_counts": dict(sorted(missing_counts.items(), key=lambda kv: (-kv[1], kv[0]))),
        "outlier_counts": dict(sorted(outlier_counts.items(), key=lambda kv: (-kv[1], kv[0]))),
        "worst_rows": worst,
    }


def _add_flag(flags: list[list[str]], flag: str, mask: Iterable[bool] | pd.Series | None = None) -> None:
    if mask is None:
        for row_flags in flags:
            row_flags.append(flag)
        return

    for i, is_flagged in enumerate(mask):
        if bool(is_flagged):
            flags[i].append(flag)


def _quality_score(row_flags: list[str]) -> float:
    if not row_flags:
        return 1.0

    score_penalty = 0
    info_penalty = 0
    for flag in row_flags:
        field = flag.rsplit("_", 1)[0]
        rule = VALIDATION_RULES.get(field)
        if rule and not rule.invalidates_score:
            info_penalty += 1
        else:
            score_penalty += 1

    score = 1.0 - (score_penalty / max(QUALITY_FIELD_COUNT, 1)) - (info_penalty * 0.01)
    return round(max(0.0, min(1.0, score)), 4)
