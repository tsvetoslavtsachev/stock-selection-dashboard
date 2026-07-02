"""
Etap 0 — data-recency guard (staleness badge).

Pins the metric that exposes a silently frozen pipeline: recency is measured
from the newest PRICE BAR the ranking is built on, never from publish time
(which is always "now" even when the fetch returned nothing new).
"""

from __future__ import annotations

import datetime

import pandas as pd

from src.jobs.publish_site_data import (
    _MIN_PRICE_COVERAGE,
    _STALE_AFTER_DAYS,
    _data_recency,
)


def _df(dates: list) -> pd.DataFrame:
    return pd.DataFrame({"price_asof": dates})


def test_recency_fresh_when_newest_bar_recent():
    today = datetime.datetime.now(datetime.timezone.utc).date()
    recent = (today - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    old = (today - datetime.timedelta(days=40)).strftime("%Y-%m-%d")

    # Full coverage (no missing bars) so the recency check is isolated from the
    # coverage floor (which the partial-outage test exercises separately).
    asof, age, fresh = _data_recency(_df([old, recent]))

    assert asof == recent          # newest bar across the universe wins
    assert age == 1
    assert fresh is True


def test_recency_flags_stale_when_all_bars_old():
    today = datetime.datetime.now(datetime.timezone.utc).date()
    old = (today - datetime.timedelta(days=_STALE_AFTER_DAYS + 3)).strftime("%Y-%m-%d")

    asof, age, fresh = _data_recency(_df([old, old]))

    assert age == _STALE_AFTER_DAYS + 3
    assert fresh is False          # the silent-staleness catch


def test_recency_missing_column_is_not_fresh():
    """No price_asof stamp at all -> the run is NOT provably fresh (fail loud)."""
    asof, age, fresh = _data_recency(pd.DataFrame({"x": [1, 2]}))
    assert asof is None
    assert age is None
    assert fresh is False


def test_recency_total_outage_is_not_fresh():
    """Every price bar missing (total outage) must report fresh=False with a null
    date — the worst case the badge exists to catch. Reporting fresh=True here was
    the blocker: the safety feature stayed silent exactly when it mattered."""
    asof, age, fresh = _data_recency(_df([None, None]))
    assert asof is None
    assert age is None
    assert fresh is False


def test_recency_partial_outage_below_coverage_floor_is_not_fresh():
    """A recent newest bar is not enough if a large slice of the universe has no
    bar: below the coverage floor the data is flagged not-fresh."""
    today = datetime.datetime.now(datetime.timezone.utc).date()
    recent = (today - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    # 2 fresh bars, 8 missing -> 20% coverage, well below the floor.
    df = _df([recent, recent] + [None] * 8)
    asof, age, fresh = _data_recency(df)
    assert asof == recent          # newest bar is genuinely recent
    assert fresh is False          # ... but coverage 0.2 < floor
    assert _MIN_PRICE_COVERAGE > 0.2
