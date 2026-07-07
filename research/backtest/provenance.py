"""
Provenance / reproducibility pin for the backtest REPORT.md (ritual §7 rule 2).

A backtest result is a deterministic function of a handful of inputs: the code in
THIS repo, the point-in-time universe, and the three sibling data repos it reads.
This module snapshots the exact identity of each of those inputs at run time so a
later bit-repro check is a *comparison of this block*, not archaeology through
transcripts (the failure mode the 2026-07-07 anchor incident exposed).

Design rules:
  * config/universe.csv is pinned by its **git blob id**
    (``git rev-parse HEAD:config/universe.csv``) -- NEVER a working-tree sha256.
    CRLF normalization makes a working-tree hash unstable across checkouts; the
    blob id is the only stable identifier. A sha256 is emitted ONLY as an extra
    tag when the file is uncommitted (the real repro-hole flagged in §6).
  * Every field degrades to a sentinel ("UNKNOWN" / None) on any error. The
    report is the human-facing output and must NEVER crash on a missing git,
    absent sibling checkout, or detached/altered tree.

Pure data collection; ``report.build_report`` renders the dict.
"""

from __future__ import annotations

import hashlib
import os
import subprocess
from datetime import datetime
from pathlib import Path

# Conventional sibling input-repo checkouts (mirrors research/backtest/panel.py).
# DATACORE_ROOT overrides the price-archive location; env vars override the rest.
_PRICE_ARCHIVE = os.environ.get("DATACORE_ROOT") or os.environ.get(
    "PRICE_ARCHIVE_ROOT") or "C:/Projects/price-archive"
_COLLECTORS = os.environ.get("COLLECTORS_ROOT") or "C:/Projects/collectors"
_DATA_CORE = os.environ.get("DATA_CORE_ROOT") or "C:/Projects/data-core"


def _git(repo, *args) -> str | None:
    """Run ``git -C repo *args``; return stripped stdout or None on any failure."""
    try:
        out = subprocess.run(
            ["git", "-C", str(repo), *args],
            capture_output=True, text=True, timeout=15,
        )
    except Exception:  # noqa: BLE001 - git absent / path bad -> degrade, never crash
        return None
    if out.returncode != 0:
        return None
    return out.stdout.strip() or None


def _head(repo) -> str:
    return _git(repo, "rev-parse", "HEAD") or "UNKNOWN"


def _sha256(path: Path) -> str | None:
    try:
        h = hashlib.sha256()
        with open(path, "rb") as fh:
            for chunk in iter(lambda: fh.read(1 << 20), b""):
                h.update(chunk)
        return h.hexdigest()
    except Exception:  # noqa: BLE001
        return None


def gather(repo_root: Path, universe_rel: str = "config/universe.csv",
           edgar_panel: Path | None = None) -> dict:
    """Snapshot the input identities this run depends on.

    ``repo_root``   : the dashboards (this) repo checkout.
    ``universe_rel``: path of the PIT universe within ``repo_root`` (blob-pinned).
    ``edgar_panel`` : the EDGAR PIT panel file (sha256-pinned) or None.
    """
    repo_root = Path(repo_root)

    # ---- this repo: HEAD + dirty(tracked) + diff hash ---------------------- #
    dashboards_head = _head(repo_root)
    # ``git diff --quiet HEAD`` returncode: 0 clean, 1 tracked changes present.
    dirty = False
    try:
        rc = subprocess.run(
            ["git", "-C", str(repo_root), "diff", "--quiet", "HEAD"],
            capture_output=True, timeout=15).returncode
        dirty = rc == 1
    except Exception:  # noqa: BLE001
        dirty = False
    diff_hash = None
    if dirty:
        diff = _git(repo_root, "diff", "HEAD")
        if diff is not None:
            diff_hash = hashlib.sha256(diff.encode("utf-8", "replace")).hexdigest()[:12]

    # ---- universe.csv: blob id (primary) + uncommitted tag ---------------- #
    universe_blob = _git(repo_root, "rev-parse", f"HEAD:{universe_rel}") or "UNKNOWN"
    uni_status = _git(repo_root, "status", "--porcelain", "--", universe_rel)
    universe_uncommitted = bool(uni_status)  # non-empty porcelain -> modified/untracked
    universe_worktree_sha256 = None
    if universe_uncommitted:
        universe_worktree_sha256 = _sha256(repo_root / universe_rel)

    # ---- sibling data repos + EDGAR panel --------------------------------- #
    edgar_sha = _sha256(edgar_panel) if edgar_panel is not None else None

    return {
        "generated": datetime.now().strftime("%Y-%m-%d %H:%M"),
        "dashboards_head": dashboards_head,
        "dashboards_dirty": dirty,
        "dashboards_diff_hash": diff_hash,
        "universe_rel": universe_rel,
        "universe_blob": universe_blob,
        "universe_uncommitted": universe_uncommitted,
        "universe_worktree_sha256": universe_worktree_sha256,
        "price_archive_head": _head(_PRICE_ARCHIVE),
        "collectors_head": _head(_COLLECTORS),
        "data_core_head": _head(_DATA_CORE),
        "edgar_panel_sha256": edgar_sha,
        "datacore_root": os.environ.get("DATACORE_ROOT") or _PRICE_ARCHIVE,
    }
