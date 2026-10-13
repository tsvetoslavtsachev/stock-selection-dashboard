"""
Balance-sheet face debt lines per filer (ПНЛ2, 13.10.2026).

Why this exists
---------------
total_debt is the FINANCIAL debt (Ц., СТОП А of ПНЛ2): long-term debt with its
current portion + short-term borrowings + commercial paper + finance leases, at
balance-sheet carrying value, operating leases out. The us-gaap tags for those
pieces overlap differently per filer: PEP shows ONE line "short-term debt
obligations" (ShortTermBorrowings 10.60) that already holds its commercial paper
(6.10) and current maturities (1.60), which it tags only in the debt note; AAPL
shows commercial paper and the current term debt as separate lines. companyfacts
cannot tell the two apart. The filing's presentation linkbase can: it lists the
lines of the balance sheet itself. build_panel adds the debt lines a filer shows
on the face and never a tag that is only a note component of one of them.

What it writes
--------------
research/fundamentals/debt_face.csv, one row per ticker, from the filing of its
latest balance sheet in the companyfacts cache:

    ticker, accn, filed, role, face, parts

parts = the debt lines that are components of a face total (totalLabel), kept as
evidence and never summed; face = the other debt-like concepts of the balance-sheet
role, ";"-joined, as
"prefix_LocalName" (us-gaap_ShortTermBorrowings, tmus_LongTermDebtToAffiliates ...).
A non-us-gaap or dimensioned face line never reaches companyfacts; such a filer is
read from its filing instead (see build_panel._DEBT_FROM_FILING).

* Rate limit — ~0.12 s between requests, inside SEC's 10 req/s cap.
* An existing row whose accn equals the cache's latest is not refetched.

CLI
---
    python -m research.fundamentals.collect_debt_face
    python -m research.fundamentals.collect_debt_face --tickers PEP AAPL
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

import requests

_REPO_ROOT = Path(__file__).resolve().parents[2]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from research.fundamentals.build_panel import _CACHE_DIR, load_universe  # noqa: E402
from research.fundamentals.debt import FACE_PATH, is_debt_line  # noqa: E402
from src.lib.sec_client import _load_user_agent  # noqa: E402

logger = logging.getLogger("collect_debt_face")

ARCHIVES = "https://www.sec.gov/Archives/edgar/data"
DELAY = 0.12
FORMS = ("10-K", "10-Q", "10-K/A", "10-Q/A", "10-KT", "20-F", "40-F")

_LINK = "{http://www.xbrl.org/2003/linkbase}"
_XL = "{http://www.w3.org/1999/xlink}"
_BS_ROLE = re.compile(r"(balancesheet|financialposition|financialcondition|statementofcondition)", re.I)
def latest_balance(cik10: str) -> tuple[str, str] | None:
    """(accn, filed) of the latest balance sheet (us-gaap:Assets) in the cache."""
    p = _CACHE_DIR / f"CIK{cik10}.json"
    if not p.exists():
        return None
    g = json.loads(p.read_text(encoding="utf-8")).get("facts", {}).get("us-gaap", {})
    recs = [r for r in g.get("Assets", {}).get("units", {}).get("USD", [])
            if r.get("form") in FORMS and r.get("accn")]
    if not recs:
        return None
    last = max(recs, key=lambda r: (r["filed"], r["end"]))
    return last["accn"], last["filed"]


def parse_face(pre_xml: bytes) -> tuple[str, list[str], list[str]] | None:
    """(role, debt lines, component lines) of the balance-sheet presentation role.

    A debt line presented with a totalLabel is the sum of the debt lines under the
    same parent (KMI: LongTermDebtNoncurrent 29.8 = kmi_DebtInstrumentCarrying-
    AmountOfLongTermPortion 29.7 + kmi_DebtFairValueAdjustments 0.1; DTE, CNP the
    same shape). Those components are returned apart and never summed again."""
    root = ET.fromstring(pre_xml)
    for pl in root.iter(_LINK + "presentationLink"):
        role = pl.get(_XL + "role", "")
        low = re.sub(r"[^a-z]", "", role.lower())  # KIM, GNRC: "...-balance-sheets-..."
        if not _BS_ROLE.search(low) or "parenth" in low or "detail" in low:
            continue
        loc = {lc.get(_XL + "label"): lc.get(_XL + "href", "").split("#")[-1] for lc in pl.iter(_LINK + "loc")}
        kids: dict[str, list[tuple[float, str, str]]] = {}
        for arc in pl.iter(_LINK + "presentationArc"):
            ch = loc.get(arc.get(_XL + "to"), "")
            kids.setdefault(loc.get(arc.get(_XL + "from"), ""), []).append(
                (float(arc.get("order") or 0), ch, (arc.get("preferredLabel") or "").rsplit("/", 1)[-1]))
        lines: list[str] = []
        parts: list[str] = []
        for parent, ch in kids.items():
            debt = [(o, c, lab) for o, c, lab in sorted(ch) if is_debt_line(c)]
            if any(lab == "totalLabel" for _o, _c, lab in debt):
                parts += [c for _o, c, lab in debt if lab != "totalLabel"]
        for c in loc.values():
            if is_debt_line(c) and c not in lines and c not in parts:
                lines.append(c)
        return role.rsplit("/", 1)[-1], lines, sorted(set(parts))
    return None


def _get(web: requests.Session, url: str) -> requests.Response:
    r = web.get(url, timeout=60)
    time.sleep(DELAY)
    r.raise_for_status()
    return r


def fetch_face(web: requests.Session, cik10: str, accn: str) -> tuple[str, list[str]] | None:
    base = f"{ARCHIVES}/{int(cik10)}/{accn.replace('-', '')}/"
    names = [it["name"] for it in _get(web, base + "index.json").json()["directory"]["item"]]
    # the presentation linkbase is its own *_pre.xml, or embedded in the
    # extension schema (*.xsd) — ORCL, MSFT, PCAR file it that way
    pre = [n for n in names if n.endswith("_pre.xml")] or [n for n in names if n.endswith(".xsd")]
    if not pre:
        return None
    return parse_face(_get(web, base + pre[0]).content)


def read_face(path: Path = FACE_PATH) -> dict[str, dict]:
    if not path.exists():
        return {}
    with open(path, newline="", encoding="utf-8") as f:
        return {r["ticker"]: r for r in csv.DictReader(f)}


def collect(tickers: list[str] | None = None, refresh: bool = False) -> dict:
    universe = dict(load_universe())
    tickers = tickers or sorted(universe)
    rows = read_face()
    web = requests.Session()
    web.headers.update({"User-Agent": _load_user_agent(), "Accept-Encoding": "gzip, deflate"})
    stats = {"tickers": len(tickers), "fetched": 0, "cached": 0, "no_role": [], "failed": []}
    for t in tickers:
        lb = latest_balance(universe[t])
        if lb is None:
            stats["failed"].append((t, "no balance sheet in cache"))
            continue
        accn, filed = lb
        if not refresh and rows.get(t, {}).get("accn") == accn and rows[t].get("role"):
            stats["cached"] += 1
            continue
        try:
            got = fetch_face(web, universe[t], accn)
        except Exception as exc:  # noqa: BLE001 — one bad name never aborts the batch
            stats["failed"].append((t, repr(exc)[:120]))
            continue
        if got is None:
            stats["no_role"].append(t)
            got = ("", [], [])
        rows[t] = {"ticker": t, "accn": accn, "filed": filed, "role": got[0], "face": ";".join(got[1]),
                   "parts": ";".join(got[2])}
        stats["fetched"] += 1
    with open(FACE_PATH, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["ticker", "accn", "filed", "role", "face", "parts"])
        w.writeheader()
        for t in sorted(rows):
            w.writerow(rows[t])
    return stats


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--tickers", nargs="*", help="subset (default: the whole SEC universe)")
    ap.add_argument("--refresh", action="store_true", help="refetch rows already at the latest accn")
    args = ap.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    stats = collect(args.tickers, refresh=args.refresh)
    logger.info("done: %s", {k: (len(v) if isinstance(v, list) else v) for k, v in stats.items()})
    for f in stats["failed"]:
        logger.warning("FAILED %s", f)
    if stats["no_role"]:
        logger.warning("no balance-sheet role: %s", " ".join(stats["no_role"]))
    return 1 if stats["failed"] else 0


if __name__ == "__main__":
    raise SystemExit(main())
