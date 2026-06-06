"""
Tests for the price-fetch retry/backoff logic.

A missing price series for an index constituent is a transient data error, so
``get_price_history`` retries with exponential backoff before giving up. Sleeping
is injected (``_sleep``) so these tests run instantly without real waits, and
yfinance is monkeypatched so they run offline.

Run:  python -m pytest tests/test_yfinance_client.py -v
"""

from __future__ import annotations

import pandas as pd

from src.lib import yfinance_client


def _good_frame() -> pd.DataFrame:
    return pd.DataFrame(
        {"Close": [1.0, 2.0, 3.0]},
        index=pd.to_datetime(["2024-01-01", "2024-01-08", "2024-01-15"]),
    )


def _install_fake_ticker(monkeypatch, responses: list[pd.DataFrame]) -> dict:
    """
    Patch yf.Ticker so successive .history() calls return *responses* in order
    (the last entry repeats). Returns a dict tracking the call count — note the
    counter must persist across the fresh yf.Ticker(symbol) made on each attempt.
    """
    state = {"calls": 0}

    def fake_ticker(symbol):  # noqa: ARG001 — symbol unused in the fake
        class _T:
            def history(self, **_kwargs):
                idx = min(state["calls"], len(responses) - 1)
                state["calls"] += 1
                return responses[idx]

        return _T()

    monkeypatch.setattr(yfinance_client.yf, "Ticker", fake_ticker)
    return state


def test_retries_then_succeeds(monkeypatch):
    """Empty twice, then data on the 3rd attempt → returns data, slept 2s, 4s."""
    empty = pd.DataFrame()
    state = _install_fake_ticker(monkeypatch, [empty, empty, _good_frame()])
    sleeps: list[float] = []

    result = yfinance_client.get_price_history("XYZ", _sleep=sleeps.append)

    assert result is not None
    assert list(result["Close"]) == [1.0, 2.0, 3.0]
    assert state["calls"] == 3            # failed twice, succeeded on the third
    assert sleeps == [2.0, 4.0]           # backoff before attempts 2 and 3


def test_gives_up_and_returns_none(monkeypatch):
    """Always empty → None after 4 attempts (1 + 3 retries), slept 2s, 4s, 8s."""
    state = _install_fake_ticker(monkeypatch, [pd.DataFrame()])
    sleeps: list[float] = []

    result = yfinance_client.get_price_history("DEAD", _sleep=sleeps.append)

    assert result is None
    assert state["calls"] == 4            # max_retries=3 → 4 total attempts
    assert sleeps == [2.0, 4.0, 8.0]      # full exponential backoff


def test_exception_is_retried(monkeypatch):
    """A raised exception is treated like a failure and retried, not propagated."""
    state = {"calls": 0}

    def fake_ticker(symbol):  # noqa: ARG001
        class _T:
            def history(self, **_kwargs):
                state["calls"] += 1
                if state["calls"] < 2:
                    raise RuntimeError("transient yfinance blip")
                return _good_frame()

        return _T()

    monkeypatch.setattr(yfinance_client.yf, "Ticker", fake_ticker)
    sleeps: list[float] = []

    result = yfinance_client.get_price_history("XYZ", _sleep=sleeps.append)

    assert result is not None
    assert state["calls"] == 2
    assert sleeps == [2.0]


def test_no_sleep_after_final_attempt(monkeypatch):
    """With max_retries=0 there is a single attempt and no backoff sleep at all."""
    state = _install_fake_ticker(monkeypatch, [pd.DataFrame()])
    sleeps: list[float] = []

    result = yfinance_client.get_price_history("X", max_retries=0, _sleep=sleeps.append)

    assert result is None
    assert state["calls"] == 1
    assert sleeps == []


# ─── Yahoo symbol normalisation (class shares: dot → dash) ───────────────────

def _capture_ticker_symbols(monkeypatch, frame: pd.DataFrame) -> list[str]:
    """
    Patch yf.Ticker to record exactly what symbol string each construction
    receives, while serving *frame* for .history()/.info. Returns the list the
    test asserts against.
    """
    seen: list[str] = []

    def fake_ticker(symbol):
        seen.append(symbol)

        class _T:
            def history(self, **_kwargs):
                return frame

            @property
            def info(self):
                return {}

        return _T()

    monkeypatch.setattr(yfinance_client.yf, "Ticker", fake_ticker)
    return seen


def test_to_yahoo_symbol_translates_dot_to_dash():
    """The pure helper: class-share dots become dashes; nothing else changes."""
    assert yfinance_client._to_yahoo_symbol("BRK.B") == "BRK-B"
    assert yfinance_client._to_yahoo_symbol("BF.B") == "BF-B"


def test_to_yahoo_symbol_leaves_plain_tickers_untouched():
    """A normal ticker has no dot, so it passes through verbatim."""
    assert yfinance_client._to_yahoo_symbol("AAPL") == "AAPL"


def test_price_fetch_sends_dash_form_to_yahoo(monkeypatch):
    """get_price_history('BRK.B') must hand yfinance the dash form 'BRK-B'."""
    seen = _capture_ticker_symbols(monkeypatch, _good_frame())

    result = yfinance_client.get_price_history("BRK.B", _sleep=lambda _s: None)

    assert result is not None                 # data comes back, not missing_prices
    assert seen == ["BRK-B"]                   # Yahoo only ever saw the dash form


def test_fundamentals_sends_dash_form_to_yahoo(monkeypatch):
    """get_fundamentals('BF.B') must hand yfinance the dash form 'BF-B'."""
    seen = _capture_ticker_symbols(monkeypatch, _good_frame())

    yfinance_client.get_fundamentals("BF.B")

    assert seen == ["BF-B"]


def test_plain_ticker_reaches_yahoo_unchanged(monkeypatch):
    """A control with no dot (AAPL) is passed through without modification."""
    seen = _capture_ticker_symbols(monkeypatch, _good_frame())

    yfinance_client.get_price_history("AAPL", _sleep=lambda _s: None)

    assert seen == ["AAPL"]
