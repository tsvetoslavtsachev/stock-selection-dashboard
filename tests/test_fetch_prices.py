"""
Tests that the ORIGINAL dotted ticker is preserved as the output key.

The Yahoo symbol normalisation (dot → dash) must stay confined to the yfinance
API call. Everything that keys records back to universe.csv — here, the price
CSV filename — must keep the original "BRK.B" form, never the "BRK-B" Yahoo
form, or the join back to the universe would silently break.

Run:  python -m pytest tests/test_fetch_prices.py -v
"""

from __future__ import annotations

from src.jobs import fetch_prices


def test_price_csv_filename_keeps_the_dot():
    """BRK.B is stored as BRK.B.csv — the dotted form, not the dash form."""
    path = fetch_prices._target_path("BRK.B")
    assert path.name == "BRK.B.csv"
    assert "BRK-B" not in path.name


def test_plain_ticker_filename_unchanged():
    path = fetch_prices._target_path("AAPL")
    assert path.name == "AAPL.csv"
