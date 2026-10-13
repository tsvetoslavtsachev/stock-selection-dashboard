"""
Own-tag collector (ПНЛ4г, 13.10.2026).

Why this exists
---------------
The SEC companyfacts API (collect_edgar.py) carries only the standard taxonomies.
A filer that stopped tagging its cash-flow line with us-gaap and moved it to its
OWN extension tag (acn:DepreciationAmortizationAndOther, cop:Payment...) leaves the
panel series stopped at the last us-gaap period. The filing's XBRL instance keeps
the extension facts; this collector reads them, for the names and tags listed in
research/fundamentals/own_tags.csv only (by name, never global). build_panel admits
them only through the overlap proof against the filer's own us-gaap chain.

What it caches
--------------
For every ticker in own_tags.csv and every 10-K / 10-Q (and amendments) filed from
IXBRL_FROM on (or from the row's `from` date, when the proof needs older comparatives:
TSLA; then the pre-inline instance layout is read too), one entry per accession in research/cache/edgar_own/CIK{cik10}.json:

    {"form", "filed", "period", "fy", "fp", "instance", "tags": [local names looked for],
     "facts": {local_name: [[start, end, val], ...]}}   # undimensioned, USD only

Facts on any axis are skipped. The filing list, the Archives client, the
User-Agent and the pace are those of collect_edgar_classes (ПНЛ1).

* Incremental — an accession is refetched only when the table asks for a tag it
                was not parsed for.

CLI
---
    python -m research.fundamentals.collect_own_tags
    python -m research.fundamentals.collect_own_tags --tickers COP URI
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import re
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import NamedTuple

import requests

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from research.fundamentals.collect_edgar_classes import (  # noqa: E402
    ARCHIVES, DELAY, FORMS, IXBRL_FROM, _DEI_META, _XBRLDI, _XBRLI, _cik_map,
)
from src.lib.sec_client import SECClient, _load_user_agent  # noqa: E402

logger = logging.getLogger("collect_own_tags")

TABLE_PATH = Path(__file__).resolve().parent / "own_tags.csv"
CACHE_DIR = _REPO_ROOT / "research" / "cache" / "edgar_own"
_STANDARD_NS = ("http://fasb.org/", "http://xbrl.sec.gov/", "http://www.xbrl.org/", "http://xbrl.org/")


_TERM = r"([+-]?)((?:us-gaap|[A-Za-z0-9_]+):[A-Za-z0-9_]+)"


class OwnSpec(NamedTuple):
    tags: tuple[str, ...]    # own tags (prefixed, e.g. 'acn:Foo'), summed from one filing
    minus: tuple[str, ...]   # us-gaap tags subtracted, from companyfacts, same filing (ACN lease)
    proof: str               # us-gaap tag the proof runs against; "" = the panel's series
    since: str               # filed-from date for the collector; "" = IXBRL_FROM


def load_table(path: Path = TABLE_PATH) -> dict[tuple[str, str], OwnSpec]:
    """(ticker, concept) -> OwnSpec. `tags` is a formula: own tags joined by '+', then
    us-gaap tags to subtract joined by '-' ('acn:A-us-gaap:OperatingLeaseCost')."""
    out = {}
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            formula = re.sub(r"\s+", "", r["tags"])
            parts = re.findall(_TERM, formula)
            if "".join(sign + t for sign, t in parts) != formula or not parts or parts[0][0]:
                raise ValueError(f"own_tags.csv {r['ticker']}: bad formula {r['tags']!r}")
            plus = tuple(t.strip() for sign, t in parts if sign != "-")
            minus = tuple(t.strip() for sign, t in parts if sign == "-")
            if any(t.startswith("us-gaap:") for t in plus) or not all(t.startswith("us-gaap:") for t in minus):
                raise ValueError(f"own_tags.csv {r['ticker']}: own tags add, us-gaap tags subtract")
            proof = (r.get("proof") or "").strip()
            if proof and not proof.startswith("us-gaap:"):
                raise ValueError(f"own_tags.csv {r['ticker']}: the proof runs against a us-gaap tag")
            out[(r["ticker"], r["concept"])] = OwnSpec(plus, minus, proof, (r.get("from") or "").strip())
    return out


def local_name(tag: str) -> str:
    return tag.split(":", 1)[1]


def cache_path(cik10: str) -> Path:
    return CACHE_DIR / f"CIK{cik10}.json"


def own_records(cache: dict, tags: tuple[str, ...]) -> list[dict]:
    """companyfacts-shaped records (start, end, val, accn, filed, form, fy, fp) of
    one own tag, or of a SUM of several taken from the SAME filing for the same
    period (every part present, else the period is dropped)."""
    out = []
    for accn, e in cache.items():
        facts = e.get("facts") or {}
        per = [{(s, en): v for s, en, v in facts.get(local_name(t), [])} for t in tags]
        for key in set(per[0]).intersection(*per[1:]):
            r = {"end": key[1], "val": sum(p[key] for p in per), "accn": accn,
                 "filed": e["filed"], "form": e["form"], "fy": e.get("fy", ""), "fp": e.get("fp", "")}
            if key[0]:
                r["start"] = key[0]
            out.append(r)
    return out


def _filings(client: SECClient, web: requests.Session, cik10: str, since: str) -> list[dict]:
    """All 10-K/10-Q filed since `since`, across the paged submissions (as ПНЛ1)."""
    sub = client.submissions(cik10)
    blocks = [sub["filings"]["recent"]]
    for f in sub["filings"].get("files", []):
        if f.get("filingTo", "9999") < since:
            continue
        r = web.get(f"https://data.sec.gov/submissions/{f['name']}", timeout=30)
        r.raise_for_status()
        time.sleep(DELAY)
        blocks.append(r.json())
    return [{"accn": b["accessionNumber"][i], "form": form, "filed": b["filingDate"][i]}
            for b in blocks for i, form in enumerate(b["form"])
            if form in FORMS and b["filingDate"][i] >= since]


_LINKBASES = ("_cal.xml", "_def.xml", "_lab.xml", "_pre.xml")


def _fetch_instance(web: requests.Session, cik10: str, accn: str) -> tuple[str, bytes] | None:
    """The extracted inline instance (*_htm.xml), else the pre-inline one (tsla-20180630.xml)."""
    base = f"{ARCHIVES}/{int(cik10)}/{accn.replace('-', '')}/"
    r = web.get(base + "index.json", timeout=30)
    time.sleep(DELAY)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    names = [it["name"] for it in r.json()["directory"]["item"]]
    inst = ([n for n in names if n.endswith("_htm.xml")]
            or [n for n in names if n.endswith(".xml") and not n.endswith(_LINKBASES) and n != "FilingSummary.xml"])
    if not inst:
        return None
    x = web.get(base + inst[0], timeout=120)
    time.sleep(DELAY)
    x.raise_for_status()
    return inst[0], x.content


def parse_instance(xml_bytes: bytes, wanted: set[str]) -> dict:
    """Undimensioned USD facts of the wanted extension tags (local names) plus the
    dei period meta out of one XBRL instance document."""
    root = ET.fromstring(xml_bytes)
    ctx: dict[str, tuple] = {}
    for c in root.iter(f"{_XBRLI}context"):
        dimensional = any(True for _ in c.iter(f"{_XBRLDI}explicitMember")) or \
            any(True for _ in c.iter(f"{_XBRLDI}typedMember"))
        p = c.find(f"{_XBRLI}period")
        start = p.findtext(f"{_XBRLI}startDate") if p is not None else None
        end = p.findtext(f"{_XBRLI}endDate") if p is not None else None
        ctx[c.get("id")] = (dimensional, start, end)
    usd = set()
    for u in root.iter(f"{_XBRLI}unit"):
        ms = [(m.text or "").strip() for m in u.iter(f"{_XBRLI}measure")]
        if ms and len(ms) == 1 and ms[0].split(":")[-1] == "USD":
            usd.add(u.get("id"))

    facts: dict[str, list] = {}
    meta: dict[str, str] = {}
    seen: set[tuple] = set()
    for el in root:
        tag = el.tag
        if not tag.startswith("{") or el.get("contextRef") is None:
            continue
        ns, name = tag[1:].split("}", 1)
        if name in _DEI_META and "/dei/" in ns:
            meta.setdefault(name, (el.text or "").strip())
            continue
        if name not in wanted or ns.startswith(_STANDARD_NS):
            continue
        if el.get("{http://www.w3.org/2001/XMLSchema-instance}nil") == "true" or el.get("unitRef") not in usd:
            continue
        dimensional, start, end = ctx.get(el.get("contextRef"), (True, None, None))
        if dimensional or not end:
            continue
        try:
            val = float((el.text or "").strip())
        except ValueError:
            continue
        key = (name, start, end)
        if key in seen:
            continue
        seen.add(key)
        facts.setdefault(name, []).append([start or "", end, val])
    return {"facts": facts, "period": meta.get("DocumentPeriodEndDate", ""),
            "fy": meta.get("DocumentFiscalYearFocus", ""), "fp": meta.get("DocumentFiscalPeriodFocus", "")}


def collect(tickers: list[str] | None = None) -> dict:
    table = load_table()
    cmap = _cik_map()
    want: dict[str, set[str]] = {}
    since: dict[str, str] = {}
    for (ticker, _concept), spec in table.items():
        if tickers and ticker not in tickers:
            continue
        want.setdefault(cmap[ticker], set()).update(local_name(t) for t in spec.tags)
        since[cmap[ticker]] = min(since.get(cmap[ticker], IXBRL_FROM), spec.since or IXBRL_FROM)
    client = SECClient()
    web = requests.Session()
    web.headers.update({"User-Agent": _load_user_agent(), "Accept-Encoding": "gzip, deflate"})
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    stats = {"ciks": len(want), "fetched": 0, "cached": 0, "no_instance": 0, "failed": []}
    for cik10, wanted in sorted(want.items()):
        p = cache_path(cik10)
        cache = json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
        try:
            filings = _filings(client, web, cik10, since[cik10])
        except Exception as exc:  # noqa: BLE001 — one bad name never aborts the batch
            stats["failed"].append((cik10, "submissions", repr(exc)[:120]))
            continue
        for f in filings:
            old = cache.get(f["accn"])
            if old is not None and (old.get("instance") is None or wanted <= set(old.get("tags", []))):
                stats["cached"] += 1
                continue
            try:
                got = _fetch_instance(web, cik10, f["accn"])
            except Exception as exc:  # noqa: BLE001
                stats["failed"].append((cik10, f["accn"], repr(exc)[:120]))
                continue
            if got is None:
                stats["no_instance"] += 1
                cache[f["accn"]] = {**f, "instance": None}
                continue
            name, xml_bytes = got
            cache[f["accn"]] = {**f, "instance": name, "tags": sorted(wanted),
                                **parse_instance(xml_bytes, wanted)}
            stats["fetched"] += 1
        p.write_text(json.dumps(cache, indent=0, sort_keys=True), encoding="utf-8")
        logger.info("CIK %s: %d filings in cache", cik10, len(cache))
    return stats


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--tickers", nargs="*", help="subset (default: every ticker in own_tags.csv)")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stats = collect(args.tickers)
    logger.info("done: %s", {k: (len(v) if isinstance(v, list) else v) for k, v in stats.items()})
    for f in stats["failed"]:
        logger.warning("FAILED %s", f)
    return 1 if stats["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
