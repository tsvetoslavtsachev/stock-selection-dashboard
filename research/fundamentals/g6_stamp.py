"""
G6 stamp (ПНЛ5, 13.10.2026): the receipt that the panel on disk passed G6 (+G6d).

validate_gates writes ``research/data/g6_stamp.json`` after checking the canonical
panel: the sha256 of the panel file, the verdict, the date. A consumer that must not
read a stale or unchecked panel (valuation-organ build_screen) calls ``check_stamp`` and
refuses when the panel's sha256 differs from the stamp's or the verdict is not PASS.

The stamp is about the panel's BYTES, so rebuilding the panel without re-running
validate_gates invalidates it, which is the point.
"""

from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
PANEL_PATH = _REPO_ROOT / "research" / "data" / "edgar_pit_panel.csv.gz"
STAMP_PATH = _REPO_ROOT / "research" / "data" / "g6_stamp.json"


def panel_sha256(panel_path: Path = PANEL_PATH) -> str:
    h = hashlib.sha256()
    with open(panel_path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def write_stamp(passed: bool, n_failures: int, panel_path: Path = PANEL_PATH,
                stamp_path: Path = STAMP_PATH) -> dict:
    stamp = {
        "panel_sha256": panel_sha256(panel_path),
        "result": "PASS" if passed else "FAIL",
        "failures": n_failures,
        "date": date.today().isoformat(),
        "gates": "G6+G6d",
    }
    stamp_path.write_text(json.dumps(stamp, indent=2) + "\n", encoding="utf-8")
    return stamp


def check_stamp(panel_path: Path = PANEL_PATH, stamp_path: Path = STAMP_PATH) -> tuple[bool, str]:
    """(ok, message). ok only when the stamp exists, says PASS, and matches the panel bytes."""
    if not Path(stamp_path).exists():
        return False, (f"няма печат на G6 ({stamp_path}); пусни "
                       f"`python -m research.fundamentals.validate_gates` в stock-selection-dashboard")
    try:
        stamp = json.loads(Path(stamp_path).read_text(encoding="utf-8"))
    except (OSError, ValueError) as e:
        return False, f"печатът на G6 не се чете ({e})"
    if stamp.get("result") != "PASS":
        return False, (f"G6 не е PASS в печата от {stamp.get('date')} "
                       f"({stamp.get('failures')} провала); поправи панела и пусни validate_gates")
    if stamp.get("panel_sha256") != panel_sha256(panel_path):
        return False, (f"панелът е променен след печата на G6 от {stamp.get('date')} "
                       f"(sha256 не съвпада); пусни validate_gates върху новия панел")
    return True, f"G6 PASS от {stamp.get('date')}, sha256 съвпада"
