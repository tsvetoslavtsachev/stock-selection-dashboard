"""
Instance backfill for names SEC companyfacts has not caught up with (25.11.2026).

Why this exists
---------------
On 25.11.2026 a full ``collect_edgar --force`` still returned no Q2 and Q3 facts for 17 names
(C, NEE, DTE, SO, PLD, MAA, LNT, DLR, ETR, FRT, REG, XOM, DUK, AEP, EVRG, SRE, PPL) although
``submissions`` lists their 10-Q and each filing carries an XBRL instance (a known trap, see
validate_gates G7b). The panel froze those names at Q1 2026.

What it does
------------
For every universe CIK it compares the filings ``submissions`` lists (10-K/10-Q and amendments)
with the accessions companyfacts carries. For each filing that companyfacts lacks, it downloads the
filing's extracted instance (``*_htm.xml``) and keeps EVERY undimensioned numeric fact of the
us-gaap and dei taxonomies in the shape companyfacts uses
(``{start, end, val, accn, fy, fp, form, filed}`` under ``facts[ns][tag]["units"][unit]``), in
research/cache/edgar_backfill/CIK{cik10}.json. ``facts_overlay.load_facts`` merges it under the
companyfacts facts, so every chain, proof and derivation runs unchanged. A backfilled accession that
companyfacts later carries is ignored by the merge (companyfacts wins), so nothing double counts.

The instance is the filing itself: no estimate, no second source. test_instance_backfill proves
the parser on a filing companyfacts DOES carry (same facts, same values).

Not kept: dimensioned facts (companyfacts does not carry them either) and extension tags (the own-tag
collector's job).

CLI
---
    python -m research.fundamentals.collect_instance_backfill
    python -m research.fundamentals.collect_instance_backfill --tickers XOM PLD
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

from research.fundamentals.collect_edgar import SEC_STATUSES  # noqa: E402
from research.fundamentals.facts_overlay import BACKFILL_DIR as CACHE_DIR, accns_of  # noqa: E402
from research.fundamentals.collect_edgar_classes import (  # noqa: E402
    ARCHIVES, DELAY, FORMS, _XBRLDI, _XBRLI, _DEI_META,
)
from research.fundamentals.collect_own_tags import _fetch_instance  # noqa: E402
from src.lib.sec_client import SECClient, _load_user_agent  # noqa: E402

logger = logging.getLogger("collect_instance_backfill")

_COMPANYFACTS_DIR = _REPO_ROOT / "research" / "cache" / "edgar"
_UNIVERSE_PATH = _REPO_ROOT / "config" / "universe.csv"
_FROM = "2026-01-01"          # only filings the companyfacts cache could miss: the current year
_NS = (("http://fasb.org/us-gaap/", "us-gaap"), ("http://xbrl.sec.gov/dei/", "dei"))


def _unit_name(u: ET.Element) -> str | None:
    """companyfacts unit names: 'USD', 'shares', 'USD/shares', 'pure'. Anything else: None."""
    div = u.find(f"{_XBRLI}divide")
    if div is not None:
        n = [(m.text or "").strip().split(":")[-1] for m in div.find(f"{_XBRLI}unitNumerator").iter(f"{_XBRLI}measure")]
        d = [(m.text or "").strip().split(":")[-1] for m in div.find(f"{_XBRLI}unitDenominator").iter(f"{_XBRLI}measure")]
        return f"{n[0]}/{d[0]}" if len(n) == 1 and len(d) == 1 else None
    ms = [(m.text or "").strip().split(":")[-1] for m in u.iter(f"{_XBRLI}measure")]
    return ms[0] if len(ms) == 1 and ms[0] in ("USD", "shares", "pure") else None


def parse_instance(xml_bytes: bytes, accn: str, form: str, filed: str) -> dict:
    """Undimensioned numeric us-gaap/dei facts of one instance, companyfacts-shaped:
    {ns: {tag: {"units": {unit: [record, ...]}}}}. dei carries the fiscal focus used for fy/fp."""
    root = ET.fromstring(xml_bytes)
    ctx: dict[str, tuple] = {}
    for c in root.iter(f"{_XBRLI}context"):
        dimensional = any(True for _ in c.iter(f"{_XBRLDI}explicitMember")) or \
            any(True for _ in c.iter(f"{_XBRLDI}typedMember"))
        p = c.find(f"{_XBRLI}period")
        inst = p.findtext(f"{_XBRLI}instant") if p is not None else None
        start = p.findtext(f"{_XBRLI}startDate") if p is not None else None
        end = p.findtext(f"{_XBRLI}endDate") if p is not None else None
        ctx[c.get("id")] = (dimensional, start, inst or end)
    units = {u.get("id"): _unit_name(u) for u in root.iter(f"{_XBRLI}unit")}

    meta: dict[str, str] = {}
    for el in root:
        if el.tag.startswith("{") and el.get("contextRef") is not None:
            ns, local = el.tag[1:].split("}", 1)
            if local in _DEI_META and "/dei/" in ns:
                meta.setdefault(local, (el.text or "").strip())
    try:
        fy = int(meta.get("DocumentFiscalYearFocus", ""))
    except ValueError:
        fy = None
    fp = meta.get("DocumentFiscalPeriodFocus", "")

    # The same fact can sit twice in one instance at different precision (AAPL UnrecognizedTaxBenefits:
    # 23 200 000 000 at decimals -8 and 23 242 000 000 at -6). companyfacts keeps the more precise
    # one; so do we (higher `decimals`, "INF" the highest; the first one on a tie).
    best: dict[tuple, tuple[int, dict]] = {}
    for el in root:
        if not el.tag.startswith("{") or el.get("contextRef") is None:
            continue
        ns_uri, local = el.tag[1:].split("}", 1)
        ns = next((short for pre, short in _NS if ns_uri.startswith(pre)), None)
        unit = units.get(el.get("unitRef"))
        if ns is None or unit is None or el.get("{http://www.w3.org/2001/XMLSchema-instance}nil") == "true":
            continue
        dimensional, start, end = ctx.get(el.get("contextRef"), (True, None, None))
        if dimensional or not end:
            continue
        try:
            fval = float((el.text or "").strip())
        except ValueError:
            continue
        dec = el.get("decimals")
        prec = 10 ** 6 if dec == "INF" else (int(dec) if dec and dec.lstrip("-").isdigit() else -(10 ** 6))
        key = (ns, local, unit, start or "", end)
        if key in best and best[key][0] >= prec:
            continue
        rec = {"end": end, "val": int(fval) if fval == int(fval) and abs(fval) < 1e15 else fval,
               "accn": accn, "fy": fy, "fp": fp, "form": form, "filed": filed}
        if start:
            rec = {"start": start, **rec}
        best[key] = (prec, rec)
    out: dict = {}
    for (ns, local, unit, _s, _e), (_p, rec) in best.items():
        out.setdefault(ns, {}).setdefault(local, {"units": {}})["units"].setdefault(unit, []).append(rec)
    return out


def _companyfacts_accns(cik10: str) -> set[str]:
    p = _COMPANYFACTS_DIR / f"CIK{cik10}.json"
    return accns_of(json.loads(p.read_text(encoding="utf-8")).get("facts", {})) if p.exists() else set()


def _listed_filings(client: SECClient, cik10: str) -> list[dict]:
    r = client.submissions(cik10)["filings"]["recent"]
    return [{"accn": r["accessionNumber"][i], "form": f, "filed": r["filingDate"][i]}
            for i, f in enumerate(r["form"]) if f in FORMS and r["filingDate"][i] >= _FROM]


def _cache_path(cik10: str) -> Path:
    return CACHE_DIR / f"CIK{cik10}.json"


def collect(tickers: list[str] | None = None) -> dict:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    with open(_UNIVERSE_PATH, encoding="utf-8", newline="") as f:
        uni = [r for r in csv.DictReader(f) if (r.get("enabled") or "").strip() in SEC_STATUSES]
    if tickers:
        uni = [r for r in uni if r["symbol"] in set(tickers)]
    client = SECClient()
    web = requests.Session()
    web.headers.update({"User-Agent": _load_user_agent(), "Accept-Encoding": "gzip, deflate"})
    stats = {"ciks": 0, "fetched": 0, "cached": 0, "no_instance": 0, "failed": [], "names": []}
    for row in uni:
        cik10 = row["cik"].strip().zfill(10)
        stats["ciks"] += 1
        try:
            have = _companyfacts_accns(cik10)
            missing = [f for f in _listed_filings(client, cik10) if f["accn"] not in have]
            if not missing:
                continue
            path = _cache_path(cik10)
            cache = json.loads(path.read_text(encoding="utf-8")) if path.exists() else {"accns": {}, "facts": {}}
            for f in missing:
                if f["accn"] in cache["accns"]:
                    stats["cached"] += 1
                    continue
                got = _fetch_instance(web, cik10, f["accn"])
                if got is None:
                    stats["no_instance"] += 1
                    logger.warning("%s %s: no instance", row["symbol"], f["accn"])
                    continue
                facts = parse_instance(got[1], f["accn"], f["form"], f["filed"])
                for ns, tags in facts.items():
                    for tag, v in tags.items():
                        for unit, recs in v["units"].items():
                            cache["facts"].setdefault(ns, {}).setdefault(tag, {"units": {}})["units"] \
                                .setdefault(unit, []).extend(recs)
                cache["accns"][f["accn"]] = {"form": f["form"], "filed": f["filed"]}
                stats["fetched"] += 1
                logger.info("%s %s %s %s: %d tags", row["symbol"], f["form"], f["filed"], f["accn"],
                            sum(len(t) for t in facts.values()))
            tmp = path.with_suffix(".json.tmp")
            tmp.write_text(json.dumps(cache), encoding="utf-8")
            tmp.replace(path)
            stats["names"].append(row["symbol"])
        except Exception as exc:  # noqa: BLE001
            logger.error("%s failed: %s", row["symbol"], repr(exc)[:200])
            stats["failed"].append(row["symbol"])
    logger.info("DONE: %s", {k: v for k, v in stats.items() if k != "names"} | {"names": len(stats["names"])})
    return stats


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="Backfill facts of filings companyfacts lacks, from the filing's instance.")
    ap.add_argument("--tickers", nargs="*", help="subset (default: the whole universe)")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    stats = collect(args.tickers)
    return 1 if stats["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
