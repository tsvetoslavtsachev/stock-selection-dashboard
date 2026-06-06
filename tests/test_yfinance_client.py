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
