"""
Tests that the ORIGINAL dotted ticker is preserved as the output key.

The Yahoo symbol normalisation (dot → dash) must stay confined to the yfinance
API call. Everything that keys records back to universe.csv — here, the price
CSV filename — must keep the original "BRK.B" form, never the "BRK-B" Yahoo
form, or the join back to the universe would silently break.

Run:  python -m pytest tests/test_fetch_prices.py -v
"""

from __future__ import annotations

import pandas as pd

from src.jobs import fetch_prices


def test_price_csv_filename_keeps_the_dot():
    """BRK.B is stored as BRK.B.csv — the dotted form, not the dash form."""
    path = fetch_prices._target_path("BRK.B")
    assert path.name == "BRK.B.csv"
    assert "BRK-B" not in path.name


def test_plain_ticker_filename_unchanged():
    path = fetch_prices._target_path("AAPL")
    assert path.name == "AAPL.csv"


# ─── INIT-22 P9: base-first daily source + CLOSED yfinance fallback ────────────

def _tz_aware_frame() -> pd.DataFrame:
    idx = pd.to_datetime(["2024-01-02", "2024-01-03"]).tz_localize("America/New_York")
    return pd.DataFrame({"Close": [10.0, 11.0]}, index=idx)


def test_daily_fallback_strips_tz(monkeypatch):
    """The yfinance fallback returns a tz-NAIVE index so the consumer's
    base(naive)+fallback merge cannot raise on a naive/aware comparison — the
    bug that silently degraded the whole strangler to fetch."""
    monkeypatch.setattr(fetch_prices, "get_price_history",
                        lambda sym, period=None, interval="1d": _tz_aware_frame())
    out = fetch_prices._daily_yf_fallback(["AAPL"])
    close = out["Close"]
    assert list(close.columns) == ["AAPL"]
    assert close.index.tz is None


def test_base_first_degrades_to_fetch_without_archive(monkeypatch):
    """No archive reader importable → the strangler routes the WHOLE universe
    through the fallback and stamps every symbol 'fetch' (never 'base')."""
    monkeypatch.setattr(fetch_prices, "_HAVE_BASE", False)
    monkeypatch.setattr(fetch_prices, "get_price_history",
                        lambda sym, period=None, interval="1d": _tz_aware_frame())
    src: dict[str, str] = {}
    close = fetch_prices._base_first_daily_close(["AAPL", "MSFT"], src)
    assert set(close.columns) == {"AAPL", "MSFT"}
    assert src == {"AAPL": "fetch", "MSFT": "fetch"}
