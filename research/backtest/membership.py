"""
Interface M -- point-in-time index-membership filter (pre-inclusion-bias control).

The current universe.csv is TODAY's S&P 500. Backtesting today's members over
history is survivorship's cousin -- *pre-inclusion bias*: a name is in the panel
across 2021-2022 only because it was ADDED later (often after a big run), which
inflates momentum/quality IC. The membership file lets a slice keep only names
that were actually in the index AS OF t.

THIS MODULE IS THE CONSUMER ONLY. The file
``research/membership/membership.csv`` is built by a SEPARATE agent. Contract:

    ticker,added_date,source_note
    AAPL,,pre-2020 member
    GEV,2024-04-02,spun off from GE

  * ``ticker``     -- dotted form (BRK.B), matching universe.csv.
  * ``added_date`` -- ISO date the name joined the index; EMPTY means it was a
                      member before the panel starts (2020-06-01), i.e. always in.
  * ``source_note``-- free text provenance.

Rule: at date t a ticker is eligible iff its added_date is empty OR <= t.

If the file is ABSENT: loud warning + the run proceeds UNFILTERED, and the
report is flagged "UNFILTERED (pre-inclusion bias)". When present: the caller
reports BOTH filtered and unfiltered IC for the key factors -- the difference IS
the measurement of the bias.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

_MEMBERSHIP_PATH = Path(__file__).resolve().parents[1] / "membership" / "membership.csv"


class Membership:
    """Point-in-time membership mask. ``available`` is False when the file is
    absent (the caller then runs unfiltered and flags the report)."""

    def __init__(self, added: pd.Series | None):
        # added: ticker -> pd.Timestamp added_date (NaT == pre-panel member).
        self._added = added
        self.available = added is not None

    def eligible(self, t: pd.Timestamp, tickers) -> list:
        """The subset of ``tickers`` that were index members as of ``t``.

        Unknown tickers (in the panel but absent from the membership file) are
        treated CONSERVATIVELY as NOT-yet-members (excluded) so a stale membership
        file cannot silently re-admit pre-inclusion bias; this is logged once by
        the loader as the coverage gap. When the file is unavailable, returns all
        tickers unchanged (unfiltered)."""
        if not self.available:
            return list(tickers)
        t = pd.Timestamp(t)
        out = []
        for tk in tickers:
            if tk not in self._added.index:
                continue  # not in the membership file -> treat as not-yet-member
            added = self._added.loc[tk]
            if pd.isna(added) or added <= t:
                out.append(tk)
        return out

    def count_excluded(self, t: pd.Timestamp, tickers) -> int:
        """How many of ``tickers`` are excluded at ``t`` (0 when unavailable)."""
        if not self.available:
            return 0
        return len(list(tickers)) - len(self.eligible(t, tickers))


def load_membership(path: Path = _MEMBERSHIP_PATH) -> Membership:
    """Load the membership file into a Membership consumer.

    Missing file -> Membership(None) (unavailable): the caller runs UNFILTERED and
    flags the report. A malformed file is treated the same (unavailable) with a
    loud warning -- research must fail visible, never silently half-filter."""
    if not path.exists():
        logger.warning(
            "MEMBERSHIP FILE ABSENT (%s) -- running UNFILTERED. Results carry "
            "PRE-INCLUSION BIAS; the report is flagged accordingly.", path,
        )
        return Membership(None)
    try:
        df = pd.read_csv(path, dtype=str)
        df.columns = [c.strip().lower() for c in df.columns]
        if "ticker" not in df.columns or "added_date" not in df.columns:
            raise ValueError("membership.csv needs columns: ticker, added_date, source_note")
        df["ticker"] = df["ticker"].str.strip().str.upper()
        added = pd.to_datetime(df["added_date"].str.strip().replace("", pd.NA), errors="coerce")
        s = pd.Series(added.values, index=df["ticker"].values, name="added_date")
        s = s[~s.index.duplicated(keep="last")]
        logger.info(
            "Membership loaded: %d tickers (%d pre-panel members, %d dated adds)",
            len(s), int(s.isna().sum()), int(s.notna().sum()),
        )
        return Membership(s)
    except Exception as exc:  # noqa: BLE001 - a bad file must not silently half-filter
        logger.warning("membership.csv unreadable (%s) -- running UNFILTERED.", exc)
        return Membership(None)
