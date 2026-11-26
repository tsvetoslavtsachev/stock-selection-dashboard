"""
G7 (25.11.2026): the panel's latest `filed` must be near the calendar. Synthetic, no cache.

    python -m pytest research/fundamentals/tests/test_g7_freshness.py -q
"""

from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

import pandas as pd

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from research.fundamentals import pit, validate_gates  # noqa: E402


def _panel(*filed):
    return pd.DataFrame({"ticker": ["A"] * len(filed), "filed": pd.to_datetime(list(filed))})


def test_frozen_panel_fails_the_25_11_2026_case():
    # the panel as it stood on 25.11.2026: latest filing 07.10.2026
    fails = validate_gates.g7_freshness(_panel("2026-09-29", "2026-10-07"), today=date(2026, 11, 25))
    assert len(fails) == 1 and fails[0].startswith("STALE_PANEL") and "49 days" in fails[0]


def test_fresh_panel_passes():
    assert validate_gates.g7_freshness(_panel("2026-11-24"), today=date(2026, 11, 25)) == []


def test_boundary_is_inclusive_at_the_limit():
    last = date(2026, 10, 1)
    edge = date.fromordinal(last.toordinal() + validate_gates._FRESH_DAYS)
    over = date.fromordinal(last.toordinal() + validate_gates._FRESH_DAYS + 1)
    assert validate_gates.g7_freshness(_panel("2026-10-01"), today=edge) == []
    assert len(validate_gates.g7_freshness(_panel("2026-10-01"), today=over)) == 1


def test_empty_panel_fails():
    assert validate_gates.g7_freshness(_panel(), today=date(2026, 11, 25)) == ["STALE_PANEL: empty panel"]


def test_g6_all_carries_g7():
    # the verdict G6 returns (and the stamp reflects) includes the freshness failure
    real = pit.load_panel(validate_gates._PANEL_PATH)
    res = validate_gates.g6_all(real, today=date(2099, 1, 1))
    assert any(f.startswith("STALE_PANEL") for f in res["failures"])


# ---------------------------------------------------------------------------
# G7b: a name frozen at an old quarter (XOM, PLD ... on 25.11.2026)
# ---------------------------------------------------------------------------
def _name_panel():
    return pd.DataFrame({
        "ticker": ["XOM", "XOM", "AAPL", "AAPL", "GUEST"],
        "form": ["10-K", "10-Q", "10-Q", "10-Q", "8-K"],
        "filed": pd.to_datetime(["2026-02-18", "2026-05-04", "2026-08-01", "2026-10-30", "2026-01-01"]),
    })


def test_lagging_names_finds_only_the_frozen_name_and_ignores_8k_only_rows():
    lag = validate_gates.lagging_names(_name_panel(), date(2026, 11, 25))
    assert list(lag["ticker"]) == ["XOM"]          # AAPL fresh; GUEST has no 10-Q/10-K at all


def test_g7b_new_unexplained_and_known():
    today = date(2026, 11, 25)
    empty = pd.DataFrame(columns=validate_gates._LAG_COLUMNS)
    assert any(f.startswith("LAGGING_NAME XOM") for f in
               validate_gates.g7b_lagging_names(_name_panel(), empty, today))
    noreason = pd.DataFrame({"ticker": ["XOM"], "last_filed": ["2026-05-04"], "reason": [""]})
    assert any(f.startswith("LAGGING_UNEXPLAINED XOM") for f in
               validate_gates.g7b_lagging_names(_name_panel(), noreason, today))
    known = pd.DataFrame({"ticker": ["XOM"], "last_filed": ["2026-05-04"], "reason": ["companyfacts lag"]})
    assert validate_gates.g7b_lagging_names(_name_panel(), known, today) == []


def test_real_panel_lagging_names_are_all_in_the_baseline():
    real = pit.load_panel(validate_gates._PANEL_PATH)
    base = validate_gates.load_lag_baseline()
    assert (base["reason"].fillna("").str.strip() != "").all()
    assert validate_gates.g7b_lagging_names(real, base, date(2026, 11, 25)) == []


# ---------------------------------------------------------------------------
# CI vs local (Ц., 25.11.2026: "Само предупреждение")
# ---------------------------------------------------------------------------
def _stale_today(monkeypatch):
    """The real panel looked at from 2099: older than 45 days, every name lagging."""
    orig = validate_gates.g6_all
    monkeypatch.setattr(validate_gates, "g6_all", lambda panel, today=None: orig(panel, today=date(2099, 1, 1)))


def test_stale_panel_is_a_warning_in_ci_and_a_failure_locally(monkeypatch, capsys):
    _stale_today(monkeypatch)
    args = ["--panel", str(validate_gates._PANEL_PATH), "--no-stamp"]
    assert validate_gates.main(args + ["--warn-freshness"]) == 0          # CI: warns, does not stop
    out = capsys.readouterr().out
    assert "::warning title=SEC panel freshness::STALE_PANEL" in out and "LAGGING_NAME" in out
    assert validate_gates.main(args) == 1                                   # local ritual: failure
    assert "::warning" not in capsys.readouterr().out


def test_g6_stays_a_hard_stop_in_ci_mode(monkeypatch, capsys):
    import pandas as pd
    monkeypatch.setattr(validate_gates, "load_staleness_baseline",
                        lambda path=validate_gates._STALENESS_BASELINE_PATH:
                        pd.DataFrame(columns=validate_gates._BASELINE_COLUMNS))
    args = ["--panel", str(validate_gates._PANEL_PATH), "--no-stamp", "--warn-freshness"]
    assert validate_gates.main(args) == 1          # an empty baseline makes every stale pair NEW: G6 fails


def test_refreeze_keeps_a_name_declared_ahead_and_fills_it_once_it_lags(tmp_path):
    # 26.11.2026: AVB (merged into EQR on 17.08.2026) is declared before it lags. Mutation
    # case: a writer that keeps only the lagging names would drop it at the 10.12 refreeze.
    p = tmp_path / "lag.csv"
    pd.DataFrame({"ticker": ["AVB", "OLD"], "last_filed": ["", "2026-05-04"],
                  "reason": ["слят в EQR", "healed"]}).to_csv(p, index=False)
    panel = pd.DataFrame({"ticker": ["AVB", "OLD"], "form": ["10-Q", "10-Q"],
                          "filed": pd.to_datetime(["2026-07-30", "2026-10-30"])})
    assert validate_gates.write_lag_baseline(panel, date(2026, 12, 10), p) == 0
    b = validate_gates.load_lag_baseline(p)
    assert list(b["ticker"]) == ["AVB"] and b["reason"][0] == "слят в EQR"   # OLD healed, dropped
    assert validate_gates.g7b_lagging_names(panel, b, date(2026, 12, 10)) == []
    assert validate_gates.write_lag_baseline(panel, date(2026, 12, 18), p) == 1
    b = validate_gates.load_lag_baseline(p)
    assert list(b["ticker"]) == ["AVB"] and b["last_filed"][0] == "2026-07-30" and b["reason"][0] == "слят в EQR"
    assert validate_gates.g7b_lagging_names(panel, b, date(2026, 12, 18)) == []
