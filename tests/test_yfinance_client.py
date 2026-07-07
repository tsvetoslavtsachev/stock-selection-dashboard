"""
Tests for the price-fetch retry/backoff logic.

A missing price series for an index constituent is a transient data error, so
``get_price_history`` retries with exponential backoff before giving up. Sleeping
is injected (``_sleep``) so these tests run instantly without real waits, and
yfinance is monkeypatched so they run offline.

Run:  python -m pytest tests/test_yfinance_client.py -v
"""

from __future__ import annotations

import numpy as np
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


# --- New M2 fundamentals: GP/A + net payout yield (synthetic) ----------------

class _FakeFundTicker:
    """A stand-in yf.Ticker exposing crafted quarterly statements for the pure
    GP/A and net-payout formulas (no network). Each statement is a DataFrame whose
    INDEX is the tag name and whose columns are the (newest-first) quarters, exactly
    as yfinance returns them."""

    def __init__(self, income=None, balance=None, cashflow=None):
        self.quarterly_income_stmt = income if income is not None else pd.DataFrame()
        self.quarterly_balance_sheet = balance if balance is not None else pd.DataFrame()
        self.quarterly_cashflow = cashflow if cashflow is not None else pd.DataFrame()


def _row(tag: str, values: list[float]) -> pd.DataFrame:
    return pd.DataFrame({f"q{i}": [v] for i, v in enumerate(values)}, index=[tag])


def test_calc_gpa_from_quarterly_gross_profit():
    """GP/A = sum(4 quarterly Gross Profit) / latest Total Assets."""
    income = _row("Gross Profit", [50.0, 40.0, 30.0, 20.0])          # TTM = 140
    balance = _row("Total Assets", [700.0, 690.0])                    # latest = 700
    gpa = yfinance_client._calc_gpa(_FakeFundTicker(income=income, balance=balance), {})
    assert np.isclose(gpa, 140.0 / 700.0)


def test_calc_gpa_falls_back_to_revenue_times_margin():
    """No Gross Profit tag -> info totalRevenue * grossMargins is the TTM proxy."""
    balance = _row("Total Assets", [1000.0])
    info = {"totalRevenue": 400.0, "grossMargins": 0.5}              # GP proxy = 200
    gpa = yfinance_client._calc_gpa(_FakeFundTicker(balance=balance), info)
    assert np.isclose(gpa, 200.0 / 1000.0)


def test_calc_gpa_none_without_assets():
    income = _row("Gross Profit", [50.0, 40.0, 30.0, 20.0])
    assert yfinance_client._calc_gpa(_FakeFundTicker(income=income), {}) is None


def test_net_payout_sums_dividends_and_buybacks_absolute():
    """Both legs are reported NEGATIVE (outflow); the yield uses their absolute sum
    over market cap. (10+10+10+10 div) + (20+20+20+20 buyback) = 120 / 1000 = 12%."""
    cf = pd.concat([
        _row("Cash Dividends Paid", [-10.0, -10.0, -10.0, -10.0]),
        _row("Repurchase Of Capital Stock", [-20.0, -20.0, -20.0, -20.0]),
    ])
    y = yfinance_client._calc_net_payout_yield(_FakeFundTicker(cashflow=cf), 1000.0)
    assert np.isclose(y, 120.0 / 1000.0)


def test_net_payout_missing_one_leg_is_zero_for_that_leg():
    """A non-repurchaser (no buyback tag) is a real 0 for buybacks, not missing:
    the yield is dividends-only, not None."""
    cf = _row("Cash Dividends Paid", [-5.0, -5.0, -5.0, -5.0])       # 20 total
    y = yfinance_client._calc_net_payout_yield(_FakeFundTicker(cashflow=cf), 400.0)
    assert np.isclose(y, 20.0 / 400.0)


def test_net_payout_none_when_both_legs_absent():
    """Neither dividends nor buybacks known -> a true data gap -> None (reweights)."""
    cf = _row("Some Other Line", [1.0, 2.0, 3.0, 4.0])
    assert yfinance_client._calc_net_payout_yield(_FakeFundTicker(cashflow=cf), 1000.0) is None


def test_net_payout_none_without_market_cap():
    cf = _row("Cash Dividends Paid", [-5.0, -5.0, -5.0, -5.0])
    assert yfinance_client._calc_net_payout_yield(_FakeFundTicker(cashflow=cf), None) is None


def test_get_fundamentals_no_longer_returns_ev_ebit(monkeypatch):
    """ev_ebit is removed in M2; gpa + net_payout_yield are the new keys."""
    seen = _capture_ticker_symbols(monkeypatch, _good_frame())
    out = yfinance_client.get_fundamentals("AAPL")
    assert "ev_ebit" not in out
    assert "gpa" in out and "net_payout_yield" in out
    assert seen == ["AAPL"]
