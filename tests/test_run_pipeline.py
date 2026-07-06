"""
F2 guard — a zero-fetch price outage must fail the pipeline, not sail past green.

The CLI path (fetch_prices.main) already exits 1 on fetched==0, but CI runs the
pipeline through run_pipeline, which orchestrates the SAME fetch step. Without the
mirrored guard a total price outage produced a fresh, empty/stale ranking on a
green run — the historical "silently frozen pipeline" failure mode.

Run:  python -m pytest tests/test_run_pipeline.py -v
"""

from __future__ import annotations

import pytest

from src.jobs import run_pipeline


def test_step_fetch_raises_on_zero_fetched(monkeypatch):
    """fetched==0 -> RuntimeError out of the orchestrated fetch step (mirror of the
    CLI's `if stats['fetched'] == 0: sys.exit(1)`)."""
    monkeypatch.setattr(
        "src.jobs.fetch_prices.run",
        lambda force=False: {"fetched": 0, "skipped": 0, "errors": 500},
    )
    with pytest.raises(RuntimeError, match="0 tickers"):
        run_pipeline._step_fetch_prices(force=False)


def test_step_fetch_ok_when_some_fetched(monkeypatch):
    """A partial outage (some fetched, some errored) is a warning, not a hard stop —
    only a TOTAL zero-fetch outage refuses to continue."""
    monkeypatch.setattr(
        "src.jobs.fetch_prices.run",
        lambda force=False: {"fetched": 480, "skipped": 0, "errors": 20},
    )
    run_pipeline._step_fetch_prices(force=False)   # must NOT raise


def test_pipeline_run_is_red_on_zero_fetch(monkeypatch):
    """End-to-end: the zero-fetch step failure propagates to run() -> False, which
    main() turns into SystemExit(1) so the CI step (and its commit) fail loud."""
    monkeypatch.setattr(
        "src.jobs.fetch_prices.run",
        lambda force=False: {"fetched": 0, "skipped": 0, "errors": 500},
    )
    ok = run_pipeline.run(steps=["fetch_prices"])
    assert ok is False
