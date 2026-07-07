"""
Provenance pin contract (ritual §7 rule 2).

Guards the shape of the block and the ONE discipline that matters: universe.csv is
pinned by its git blob id, never a working-tree sha256 (a worktree hash is only an
extra tag when the file is uncommitted). Assertions are environment-independent —
they hold whether or not git / sibling checkouts are present (fields degrade to
sentinels), so this passes in CI and on a bare checkout alike.
"""

from __future__ import annotations

import re
from pathlib import Path

from research.backtest import provenance

_REPO_ROOT = Path(__file__).resolve().parents[3]
_HEX40 = re.compile(r"^[0-9a-f]{40}$")
_HEX64 = re.compile(r"^[0-9a-f]{64}$")

_EXPECTED_KEYS = {
    "generated", "dashboards_head", "dashboards_dirty", "dashboards_diff_hash",
    "universe_rel", "universe_blob", "universe_uncommitted",
    "universe_worktree_sha256", "price_archive_head", "collectors_head",
    "data_core_head", "edgar_panel_sha256", "datacore_root",
}


def test_gather_has_the_full_contract():
    prov = provenance.gather(_REPO_ROOT, "config/universe.csv")
    assert set(prov) == _EXPECTED_KEYS
    assert isinstance(prov["universe_blob"], str)
    assert isinstance(prov["dashboards_dirty"], bool)
    assert isinstance(prov["universe_uncommitted"], bool)


def test_universe_pinned_by_git_blob_not_worktree_hash():
    """The primary pin is a git blob id (40-hex) or the UNKNOWN sentinel — a
    committed universe.csv is NEVER identified by a 64-hex worktree sha256."""
    prov = provenance.gather(_REPO_ROOT, "config/universe.csv")
    blob = prov["universe_blob"]
    assert blob == "UNKNOWN" or _HEX40.match(blob), blob
    assert not _HEX64.match(blob), "blob id must not be a sha256 worktree hash"
    # The worktree sha256 is present ONLY as an extra tag on an uncommitted file.
    wt = prov["universe_worktree_sha256"]
    if prov["universe_uncommitted"]:
        assert wt is None or _HEX64.match(wt)
    else:
        assert wt is None
