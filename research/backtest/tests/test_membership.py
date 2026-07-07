"""
Membership (Interface M) consumer logic tests.
"""

from __future__ import annotations

import pandas as pd

from research.backtest.membership import Membership, load_membership


def _mem():
    added = pd.Series(
        {
            "AAA": pd.NaT,                       # pre-panel member (always in)
            "BBB": pd.Timestamp("2022-06-15"),   # added mid-history
            "CCC": pd.Timestamp("2024-01-01"),   # added late
        },
        name="added_date",
    )
    return Membership(added)


def test_eligible_before_and_after_add():
    m = _mem()
    tickers = ["AAA", "BBB", "CCC"]
    # Before BBB's add: only AAA.
    assert m.eligible(pd.Timestamp("2022-01-31"), tickers) == ["AAA"]
    # After BBB, before CCC: AAA + BBB.
    assert set(m.eligible(pd.Timestamp("2023-06-30"), tickers)) == {"AAA", "BBB"}
    # After CCC: all three.
    assert set(m.eligible(pd.Timestamp("2024-06-30"), tickers)) == {"AAA", "BBB", "CCC"}


def test_added_on_exact_date_is_eligible():
    m = _mem()
    assert "BBB" in m.eligible(pd.Timestamp("2022-06-15"), ["BBB"])


def test_unknown_ticker_excluded_conservatively():
    """A ticker absent from the membership file is treated as not-yet-member."""
    m = _mem()
    assert m.eligible(pd.Timestamp("2023-01-01"), ["AAA", "ZZZ"]) == ["AAA"]


def test_count_excluded():
    m = _mem()
    # At 2022-01-31 only AAA eligible of 3 -> 2 excluded.
    assert m.count_excluded(pd.Timestamp("2022-01-31"), ["AAA", "BBB", "CCC"]) == 2


def test_unavailable_passthrough():
    m = Membership(None)
    assert m.available is False
    assert m.eligible(pd.Timestamp("2022-01-31"), ["X", "Y"]) == ["X", "Y"]
    assert m.count_excluded(pd.Timestamp("2022-01-31"), ["X", "Y"]) == 0


def test_load_missing_file_is_unavailable(tmp_path):
    m = load_membership(tmp_path / "nope.csv")
    assert m.available is False


def test_load_real_file(tmp_path):
    p = tmp_path / "membership.csv"
    p.write_text("ticker,added_date,source_note\n"
                 "AAA,,pre\n"
                 "BBB,2022-06-15,added\n", encoding="utf-8")
    m = load_membership(p)
    assert m.available is True
    assert m.eligible(pd.Timestamp("2022-01-01"), ["AAA", "BBB"]) == ["AAA"]
