"""
Per-class share-count collector (ПНЛ1, 12.10.2026).

Why this exists
---------------
The SEC companyfacts API (collect_edgar.py) DISCARDS dimensional facts. A
multi-class issuer tags its cover-page share count PER CLASS
(dei:EntityCommonStockSharesOutstanding on us-gaap:StatementClassOfStockAxis),
so for META, V, MA, CMCSA, NKE, BRK.B and ~30 more names companyfacts carries
either no count at all or a stale undimensioned one from 2009-2018. The filing's
own XBRL instance in EDGAR Archives keeps the dimensions; this collector reads
it.

What it caches
--------------
For every ticker in research/fundamentals/share_classes.csv (the coefficient
table) and every 10-K / 10-Q (and amendments) filed from IXBRL_FROM on, one entry
per accession in research/cache/edgar_classes/CIK{cik10}.json:

    {"form", "filed", "period", "fy", "fp", "instance",
     "ecso": [[member, instant, val], ...],   # cover page, per class
     "eps":  [[member, start, end, val], ...],# EarningsPerShareDiluted
     "ni":   [[start, end, val], ...],        # NetIncomeLoss, undimensioned
     "pl", "nci": the same shape            # ProfitLoss / NCI share, undimensioned
     "wdil": [[member, start, end, val], ...],# WeightedAverageNumberOfDilutedSharesOutstanding
     "epsb": [[member, start, end, val], ...],# EarningsPerShareBasic
     "wbas": [[member, start, end, val], ...]}# WeightedAverageNumberOfSharesOutstandingBasic

member is the StatementClassOfStockAxis member ("" when undimensioned). Facts on
any OTHER axis are skipped (segments, co-registrants), so the cache holds only
what build_panel needs. The summing into one economic count happens in
build_panel, against the coefficient table; the cache is raw evidence.

* Incremental — an accession already in the cache is never refetched.
* Rate limit  — ~0.12 s between requests, inside SEC's 10 req/s cap.
* www.sec.gov — SECClient pins Host: data.sec.gov (its endpoints), which makes
                www.sec.gov answer 404 NoSuchKey; Archives go through a separate
                session with the same User-Agent.

CLI
---
    python -m research.fundamentals.collect_edgar_classes
    python -m research.fundamentals.collect_edgar_classes --tickers META V
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import time
import xml.etree.ElementTree as ET
from pathlib import Path

import requests

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from src.lib.sec_client import SECClient, _load_user_agent  # noqa: E402

logger = logging.getLogger("collect_edgar_classes")

_UNIVERSE_PATH = _REPO_ROOT / "config" / "universe.csv"
_TABLE_PATH = Path(__file__).resolve().parent / "share_classes.csv"
_CACHE_DIR = _REPO_ROOT / "research" / "cache" / "edgar_classes"

ARCHIVES = "https://www.sec.gov/Archives/edgar/data"
FORMS = ("10-K", "10-Q", "10-K/A", "10-Q/A", "10-KT")
# Inline XBRL (and with it the extracted *_htm.xml instance) is universal for
# large accelerated filers from fiscal periods ending after 2019-06-15; the
# screen's 6-year P/E history starts in 2020. Older filings are not fetched.
IXBRL_FROM = "2019-06-01"
DELAY = 0.12

_XBRLI = "{http://www.xbrl.org/2003/instance}"
_XBRLDI = "{http://xbrl.org/2006/xbrldi}"
_CLASS_AXIS = "us-gaap:StatementClassOfStockAxis"
_WANT = {
    ("http://xbrl.sec.gov/dei/", "EntityCommonStockSharesOutstanding"): "ecso",
    ("http://fasb.org/us-gaap/", "EarningsPerShareDiluted"): "eps",
    ("http://fasb.org/us-gaap/", "NetIncomeLoss"): "ni",
    ("http://fasb.org/us-gaap/", "WeightedAverageNumberOfDilutedSharesOutstanding"): "wdil",
    ("http://fasb.org/us-gaap/", "EarningsPerShareBasic"): "epsb",
    ("http://fasb.org/us-gaap/", "WeightedAverageNumberOfSharesOutstandingBasic"): "wbas",
    # issuers that tag the bottom line as ProfitLoss (MA, IBKR, SPG, FOX, EL, ABNB):
    # the guard in build_panel uses ProfitLoss minus the NCI when NetIncomeLoss is absent
    ("http://fasb.org/us-gaap/", "ProfitLoss"): "pl",
    ("http://fasb.org/us-gaap/", "NetIncomeLossAttributableToNoncontrollingInterest"): "nci",
}
# Cache entries written before a kind was added lack its key and are refetched.
_SCHEMA_KEYS = ("pl", "nci")
_DEI_META = ("DocumentPeriodEndDate", "DocumentFiscalYearFocus", "DocumentFiscalPeriodFocus")


def _cik_map() -> dict[str, str]:
    with open(_UNIVERSE_PATH, newline="", encoding="utf-8") as f:
        return {r["symbol"]: r["cik"].strip().zfill(10) for r in csv.DictReader(f)}


def table_tickers(path: Path = _TABLE_PATH) -> list[str]:
    with open(path, newline="", encoding="utf-8") as f:
        return sorted({r["ticker"] for r in csv.DictReader(f)})


def cache_path(cik10: str) -> Path:
    return _CACHE_DIR / f"CIK{cik10}.json"


def parse_instance(xml_bytes: bytes) -> dict:
    """Pull the wanted concepts (class axis only) plus the dei period meta
    out of one XBRL instance document."""
    root = ET.fromstring(xml_bytes)
    ctx: dict[str, tuple] = {}
    for c in root.iter(f"{_XBRLI}context"):
        dims = [(m.get("dimension"), (m.text or "").strip())
                for m in c.iter(f"{_XBRLDI}explicitMember")]
        typed = any(True for _ in c.iter(f"{_XBRLDI}typedMember"))
        p = c.find(f"{_XBRLI}period")
        inst = p.findtext(f"{_XBRLI}instant") if p is not None else None
        start = p.findtext(f"{_XBRLI}startDate") if p is not None else None
        end = p.findtext(f"{_XBRLI}endDate") if p is not None else None
        ctx[c.get("id")] = (dims, typed, inst, start, end)

    out = {k: [] for k in _WANT.values()}
    meta: dict[str, str] = {}
    seen: set[tuple] = set()
    for el in root:
        tag = el.tag
        if not tag.startswith("{") or el.get("contextRef") is None:
            continue
        ns, local = tag[1:].split("}", 1)
        if local in _DEI_META and "/dei/" in ns:
            meta.setdefault(local, (el.text or "").strip())
            continue
        kind = next((k for (pfx, name), k in _WANT.items()
                     if local == name and ns.startswith(pfx)), None)
        if kind is None or el.get("{http://www.w3.org/2001/XMLSchema-instance}nil") == "true":
            continue
        dims, typed, inst, start, end = ctx.get(el.get("contextRef"), ([], False, None, None, None))
        if typed or any(d != _CLASS_AXIS for d, _ in dims) or len(dims) > 1:
            continue  # another axis (segment, co-registrant) — not a class count
        member = dims[0][1] if dims else ""
        try:
            val = float((el.text or "").strip())
        except ValueError:
            continue
        if kind == "ecso":
            rec = [member, inst, val]
        elif kind in ("ni", "pl", "nci"):
            if member:
                continue
            rec = [start, end, val]
        else:
            rec = [member, start, end, val]
        key = (kind, *rec[:-1])
        if key in seen:
            continue
        seen.add(key)
        out[kind].append(rec)
    out["period"] = meta.get("DocumentPeriodEndDate", "")
    out["fy"] = meta.get("DocumentFiscalYearFocus", "")
    out["fp"] = meta.get("DocumentFiscalPeriodFocus", "")
    return out


def _filings(client: SECClient, web: requests.Session, cik10: str) -> list[dict]:
    """All 10-K/10-Q filed since IXBRL_FROM, across the paged submissions."""
    sub = client.submissions(cik10)
    blocks = [sub["filings"]["recent"]]
    for f in sub["filings"].get("files", []):
        if f.get("filingTo", "9999") < IXBRL_FROM:
            continue
        r = web.get(f"https://data.sec.gov/submissions/{f['name']}", timeout=30)
        r.raise_for_status()
        time.sleep(DELAY)
        blocks.append(r.json())
    out = []
    for b in blocks:
        for i, form in enumerate(b["form"]):
            if form in FORMS and b["filingDate"][i] >= IXBRL_FROM:
                out.append({"accn": b["accessionNumber"][i], "form": form,
                            "filed": b["filingDate"][i]})
    return out


def _fetch_instance(web: requests.Session, cik10: str, accn: str) -> tuple[str, bytes] | None:
    base = f"{ARCHIVES}/{int(cik10)}/{accn.replace('-', '')}/"
    r = web.get(base + "index.json", timeout=30)
    time.sleep(DELAY)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    names = [it["name"] for it in r.json()["directory"]["item"]]
    inst = [n for n in names if n.endswith("_htm.xml")]
    if not inst:
        return None  # pre-inline-XBRL layout; not needed for the 2019+ window
    x = web.get(base + inst[0], timeout=120)
    time.sleep(DELAY)
    x.raise_for_status()
    return inst[0], x.content


def collect(tickers: list[str] | None = None) -> dict:
    cmap = _cik_map()
    tickers = tickers or table_tickers()
    ciks = sorted({cmap[t] for t in tickers})
    client = SECClient()
    web = requests.Session()
    web.headers.update({"User-Agent": _load_user_agent(), "Accept-Encoding": "gzip, deflate"})
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)
    stats = {"ciks": len(ciks), "fetched": 0, "cached": 0, "no_instance": 0, "failed": []}
    for cik10 in ciks:
        p = cache_path(cik10)
        cache = json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
        try:
            filings = _filings(client, web, cik10)
        except Exception as exc:  # noqa: BLE001 — one bad name never aborts the batch
            stats["failed"].append((cik10, "submissions", repr(exc)[:120]))
            continue
        for f in filings:
            old = cache.get(f["accn"])
            if old is not None and (old.get("instance") is None or all(k in old for k in _SCHEMA_KEYS)):
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
            cache[f["accn"]] = {**f, "instance": name, **parse_instance(xml_bytes)}
            stats["fetched"] += 1
        p.write_text(json.dumps(cache, indent=0, sort_keys=True), encoding="utf-8")
        logger.info("CIK %s: %d filings in cache", cik10, len(cache))
    return stats


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--tickers", nargs="*", help="subset (default: every ticker in share_classes.csv)")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stats = collect(args.tickers)
    logger.info("done: %s", {k: (len(v) if isinstance(v, list) else v) for k, v in stats.items()})
    for f in stats["failed"]:
        logger.warning("FAILED %s", f)
    return 1 if stats["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
