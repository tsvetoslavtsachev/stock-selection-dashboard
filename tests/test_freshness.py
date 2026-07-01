"""
Etap 0 — data-recency guard (staleness badge).

Pins the metric that exposes a silently frozen pipeline: recency is measured
from the newest PRICE BAR the ranking is built on, never from publish time
(which is always "now" even when the fetch returned nothing new).
"""

from __future__ import annotations

import datetime

import pandas as pd

from src.jobs.publish_site_data import _STALE_AFTER_DAYS, _data_recency


def _df(dates: list) -> pd.DataFrame:
    return pd.DataFrame({"price_asof": dates})


def test_recency_fresh_when_newest_bar_recent():
    today = datetime.datetime.utcnow().date()
    recent = (today - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    old = (today - datetime.timedelta(days=40)).strftime("%Y-%m-%d")

    asof, age, fresh = _data_recency(_df([old, recent, None]))

    assert asof == recent          # newest bar across the universe wins
    assert age == 1
    assert fresh is True


def test_recency_flags_stale_when_all_bars_old():
    today = datetime.datetime.utcnow().date()
    old = (today - datetime.timedelta(days=_STALE_AFTER_DAYS + 3)).strftime("%Y-%m-%d")

    asof, age, fresh = _data_recency(_df([old, old]))

    assert age == _STALE_AFTER_DAYS + 3
    assert fresh is False          # the silent-staleness catch


def test_recency_missing_column_is_not_fatal():
    asof, age, fresh = _data_recency(pd.DataFrame({"x": [1, 2]}))
    assert asof is None
    assert age is None
    assert fresh is True


def test_recency_all_nan_bars():
    asof, age, fresh = _data_recency(_df([None, None]))
    assert asof is None
    assert age is None
    assert fresh is True
