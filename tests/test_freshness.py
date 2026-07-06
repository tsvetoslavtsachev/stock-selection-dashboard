"""
Etap 0 — data-recency guard (staleness badge).

Pins the metric that exposes a silently frozen pipeline: recency is measured
from the newest PRICE BAR the ranking is built on, never from publish time
(which is always "now" even when the fetch returned nothing new).
"""

from __future__ import annotations

import datetime

import pandas as pd

import json

from src.jobs import publish_site_data
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


# ── F3: staleness threshold travels in the JSON (single source of truth) ──────

def test_publish_emits_stale_after_days_from_the_constant(tmp_path, monkeypatch):
    """The UI recomputes freshness client-side against TODAY, so it needs the
    threshold — which must be the SAME constant the server uses, not a second copy.
    Publishing it means a dead workflow (frozen data_fresh boolean) still trips the
    client-side STALE badge. Pin: market_summary.json.stale_after_days ==
    _STALE_AFTER_DAYS, so the two can never drift."""
    today = datetime.datetime.now(datetime.timezone.utc).date()
    recent = (today - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    ranks = tmp_path / "ranks.csv"
    pd.DataFrame({
        "ticker": ["AAA", "BBB"],
        "name": ["A", "B"],
        "sector": ["Tech", "Tech"],
        "composite_score": [1.0, -1.0],
        "price_asof": [recent, recent],
    }).to_csv(ranks, index=False)

    app_data = tmp_path / "app_data"
    app_data.mkdir()
    monkeypatch.setattr(publish_site_data, "_RANKS_CSV", ranks)
    monkeypatch.setattr(publish_site_data, "APP_DATA", app_data)

    publish_site_data.run()

    summary = json.loads((app_data / "market_summary.json").read_text(encoding="utf-8"))
    assert summary["stale_after_days"] == _STALE_AFTER_DAYS
    assert summary["data_asof"] == recent
