"""
companyfacts + the instance backfill (collect_instance_backfill), one view of a company's facts.

``load_facts(path)`` reads research/cache/edgar/CIK*.json and lays under it the facts of the filings
SEC companyfacts did not carry (research/cache/edgar_backfill/CIK*.json). companyfacts wins: an
accession it carries is never taken from the backfill, so a name that SEC catches up with simply
stops using its backfill. No network, no dependency on the collectors.
"""

from __future__ import annotations

import json
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[2]
BACKFILL_DIR = _REPO_ROOT / "research" / "cache" / "edgar_backfill"


def accns_of(facts: dict) -> set[str]:
    return {r["accn"] for ns in facts.values() for t in ns.values()
            for u in t["units"].values() for r in u if "accn" in r}


def merge_into(facts: dict, backfill: dict) -> dict:
    """companyfacts facts + the backfilled accessions companyfacts does NOT carry."""
    have = accns_of(facts)
    out = {ns: {tag: {**v, "units": {u: list(rs) for u, rs in v["units"].items()}} for tag, v in tags.items()}
           for ns, tags in facts.items()}
    for ns, tags in backfill.get("facts", {}).items():
        for tag, v in tags.items():
            for unit, recs in v["units"].items():
                new = [r for r in recs if r["accn"] not in have]
                if new:
                    out.setdefault(ns, {}).setdefault(tag, {"units": {}})["units"].setdefault(unit, []).extend(new)
    return out


def load_facts(path: Path, backfill_dir: Path | None = None) -> dict:
    """The `facts` of one CIK file with its backfill laid under it (if any)."""
    facts = json.loads(Path(path).read_text(encoding="utf-8")).get("facts", {})
    bp = (backfill_dir or BACKFILL_DIR) / Path(path).name
    if bp.exists():
        facts = merge_into(facts, json.loads(bp.read_text(encoding="utf-8")))
    return facts
