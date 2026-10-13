"""
Debt facts from the filing's own XBRL instance (ПНЛ2, 13.10.2026).

Why this exists
---------------
companyfacts keeps only undimensioned facts of the standard taxonomies. A filer
whose balance-sheet debt lines are split by segment (F: Company excluding Ford
Credit / Ford Credit; CAT: ME&T / Financial Products), by recourse (AES) or by
related party (TMUS), or carry a custom tag, therefore has no current debt there
at all. This collector reads those lines from the instance in EDGAR Archives, the
way collect_edgar_classes reads per-class share counts. build_panel turns them into
total_debt with debt.FILING_SPECS (the industrial member, or the sum over the axis).

What it caches
--------------
For every filing-route ticker (debt.filing_spec: FILING_SPECS, plus the filers
with a custom face debt line) and every 10-K / 10-Q (and amendments) filed
from IXBRL_FROM on, one entry per accession in
research/cache/edgar_debt/CIK{cik10}.json:

    {"form", "filed", "period", "fy", "fp", "instance",
     "facts": [[local_name, instant, [[axis, member]], val], ...]}

Only debt lines (debt.is_debt_line), instants, and contexts with at most ONE
dimension are kept; names are local (prefixes dropped).

CLI
---
    python -m research.fundamentals.collect_debt_filing
    python -m research.fundamentals.collect_debt_filing --tickers F GM
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import xml.etree.ElementTree as ET
from pathlib import Path

import requests

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from research.fundamentals.collect_edgar_classes import (  # noqa: E402
    _XBRLDI, _XBRLI, _cik_map, _fetch_instance, _filings)
from research.fundamentals.debt import filing_spec, is_debt_line, read_face  # noqa: E402
from src.lib.sec_client import SECClient, _load_user_agent  # noqa: E402

logger = logging.getLogger("collect_debt_filing")

_CACHE_DIR = _REPO_ROOT / "research" / "cache" / "edgar_debt"
_DEI_META = ("DocumentPeriodEndDate", "DocumentFiscalYearFocus", "DocumentFiscalPeriodFocus")


def cache_path(cik10: str) -> Path:
    return _CACHE_DIR / f"CIK{cik10}.json"


def _local(qname: str) -> str:
    return qname.split(":")[-1]


def parse_instance(xml_bytes: bytes) -> dict:
    """Debt-line instants with at most one dimension, plus the dei period meta."""
    root = ET.fromstring(xml_bytes)
    ctx: dict[str, tuple] = {}
    for c in root.iter(f"{_XBRLI}context"):
        dims = [[_local(m.get("dimension", "")), _local((m.text or "").strip())]
                for m in c.iter(f"{_XBRLDI}explicitMember")]
        typed = any(True for _ in c.iter(f"{_XBRLDI}typedMember"))
        p = c.find(f"{_XBRLI}period")
        inst = p.findtext(f"{_XBRLI}instant") if p is not None else None
        ctx[c.get("id")] = (dims, typed, inst)
    facts, meta, seen = [], {}, set()
    for el in root:
        tag = el.tag
        if not tag.startswith("{") or el.get("contextRef") is None:
            continue
        ns, local = tag[1:].split("}", 1)
        if local in _DEI_META and "/dei/" in ns:
            meta.setdefault(local, (el.text or "").strip())
            continue
        if not is_debt_line(local) or el.get("{http://www.w3.org/2001/XMLSchema-instance}nil") == "true":
            continue
        dims, typed, inst = ctx.get(el.get("contextRef"), ([], False, None))
        if inst is None or typed or len(dims) > 1:
            continue
        try:
            val = float((el.text or "").strip())
        except ValueError:
            continue
        key = (local, inst, tuple(map(tuple, dims)))
        if key in seen:
            continue
        seen.add(key)
        facts.append([local, inst, dims, val])
    return {"facts": facts, "period": meta.get("DocumentPeriodEndDate", ""),
            "fy": meta.get("DocumentFiscalYearFocus", ""), "fp": meta.get("DocumentFiscalPeriodFocus", "")}


def collect(tickers: list[str] | None = None) -> dict:
    cmap = _cik_map()
    face = read_face()
    tickers = tickers or sorted(t for t in cmap if filing_spec(t, face.get(t)) is not None)
    client = SECClient()
    web = requests.Session()
    web.headers.update({"User-Agent": _load_user_agent(), "Accept-Encoding": "gzip, deflate"})
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    stats = {"tickers": len(tickers), "fetched": 0, "cached": 0, "no_instance": 0, "failed": []}
    for t in tickers:
        cik10 = cmap[t]
        p = cache_path(cik10)
        cache = json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
        try:
            filings = _filings(client, web, cik10)
        except Exception as exc:  # noqa: BLE001 — one bad name never aborts the batch
            stats["failed"].append((t, "submissions", repr(exc)[:120]))
            continue
        for f in filings:
            if f["accn"] in cache:
                stats["cached"] += 1
                continue
            try:
                got = _fetch_instance(web, cik10, f["accn"])
            except Exception as exc:  # noqa: BLE001
                stats["failed"].append((t, f["accn"], repr(exc)[:120]))
                continue
            if got is None:
                stats["no_instance"] += 1
                cache[f["accn"]] = {**f, "instance": None, "facts": []}
                continue
            name, xml_bytes = got
            cache[f["accn"]] = {**f, "instance": name, **parse_instance(xml_bytes)}
            stats["fetched"] += 1
        p.write_text(json.dumps(cache, indent=0, sort_keys=True), encoding="utf-8")
        logger.info("%s: %d filings in cache", t, len(cache))
    return stats


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--tickers", nargs="*", help="subset (default: every filing-route ticker, debt.filing_spec)")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stats = collect(args.tickers)
    logger.info("done: %s", {k: (len(v) if isinstance(v, list) else v) for k, v in stats.items()})
    for f in stats["failed"]:
        logger.warning("FAILED %s", f)
    return 1 if stats["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
