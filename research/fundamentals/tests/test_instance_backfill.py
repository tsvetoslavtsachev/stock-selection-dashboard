"""
Instance backfill (25.11.2026): the parser keeps undimensioned us-gaap/dei numeric facts in the
companyfacts shape, and the merge never lets a backfilled accession shadow companyfacts. Synthetic.
The live fidelity check (instance vs companyfacts on AAPL, JPM, MSFT, CAT: 463 of 463, 787 of 788,
340 of 340, 483 of 483 facts equal; 2 AAPL UnrecognizedTaxBenefits values differ in the 4th digit,
a non-panel tag) is in the session note.

    python -m pytest research/fundamentals/tests/test_instance_backfill.py -q
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from research.fundamentals import collect_instance_backfill as B  # noqa: E402
from research.fundamentals import facts_overlay as O  # noqa: E402

_XML = b"""<?xml version="1.0"?>
<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance" xmlns:xbrldi="http://xbrl.org/2006/xbrldi"
  xmlns:us-gaap="http://fasb.org/us-gaap/2026" xmlns:dei="http://xbrl.sec.gov/dei/2026" xmlns:xom="http://xom.com/20260630">
 <xbrli:context id="d"><xbrli:entity/><xbrli:period><xbrli:startDate>2026-04-01</xbrli:startDate><xbrli:endDate>2026-06-30</xbrli:endDate></xbrli:period></xbrli:context>
 <xbrli:context id="i"><xbrli:entity/><xbrli:period><xbrli:instant>2026-06-30</xbrli:instant></xbrli:period></xbrli:context>
 <xbrli:context id="seg"><xbrli:entity><xbrli:segment><xbrldi:explicitMember dimension="us-gaap:StatementBusinessSegmentsAxis">x:A</xbrldi:explicitMember></xbrli:segment></xbrli:entity>
   <xbrli:period><xbrli:startDate>2026-04-01</xbrli:startDate><xbrli:endDate>2026-06-30</xbrli:endDate></xbrli:period></xbrli:context>
 <xbrli:unit id="usd"><xbrli:measure>iso4217:USD</xbrli:measure></xbrli:unit>
 <xbrli:unit id="sh"><xbrli:measure>xbrli:shares</xbrli:measure></xbrli:unit>
 <xbrli:unit id="eps"><xbrli:divide><xbrli:unitNumerator><xbrli:measure>iso4217:USD</xbrli:measure></xbrli:unitNumerator>
   <xbrli:unitDenominator><xbrli:measure>xbrli:shares</xbrli:measure></xbrli:unitDenominator></xbrli:divide></xbrli:unit>
 <dei:DocumentFiscalYearFocus contextRef="d">2026</dei:DocumentFiscalYearFocus>
 <dei:DocumentFiscalPeriodFocus contextRef="d">Q2</dei:DocumentFiscalPeriodFocus>
 <us-gaap:NetIncomeLoss contextRef="d" unitRef="usd">7000000000</us-gaap:NetIncomeLoss>
 <us-gaap:NetIncomeLoss contextRef="seg" unitRef="usd">1</us-gaap:NetIncomeLoss>
 <us-gaap:Assets contextRef="i" unitRef="usd">400000000000</us-gaap:Assets>
 <us-gaap:EarningsPerShareBasic contextRef="d" unitRef="eps">1.65</us-gaap:EarningsPerShareBasic>
 <dei:EntityCommonStockSharesOutstanding contextRef="i" unitRef="sh">4300000000</dei:EntityCommonStockSharesOutstanding>
 <xom:OwnLine contextRef="d" unitRef="usd">5</xom:OwnLine>
</xbrli:xbrl>"""


def test_parser_keeps_undimensioned_us_gaap_and_dei_in_companyfacts_shape():
    out = B.parse_instance(_XML, "0000034088-26-000093", "10-Q", "2026-08-03")
    ni = out["us-gaap"]["NetIncomeLoss"]["units"]["USD"]
    assert ni == [{"start": "2026-04-01", "end": "2026-06-30", "val": 7000000000, "accn": "0000034088-26-000093",
                   "fy": 2026, "fp": "Q2", "form": "10-Q", "filed": "2026-08-03"}]   # the segment fact is dropped
    assert out["us-gaap"]["Assets"]["units"]["USD"][0] == {          # instant: no start
        "end": "2026-06-30", "val": 400000000000, "accn": "0000034088-26-000093", "fy": 2026, "fp": "Q2",
        "form": "10-Q", "filed": "2026-08-03"}
    assert out["us-gaap"]["EarningsPerShareBasic"]["units"]["USD/shares"][0]["val"] == 1.65
    assert out["dei"]["EntityCommonStockSharesOutstanding"]["units"]["shares"][0]["val"] == 4300000000
    assert "xom" not in out and all(ns in ("us-gaap", "dei") for ns in out)   # extension tags are not kept


def _facts(accn, val):
    return {"us-gaap": {"Assets": {"units": {"USD": [{"end": "2026-06-30", "val": val, "accn": accn}]}}}}


def test_merge_adds_a_missing_accession_and_never_shadows_companyfacts():
    cf = _facts("A1", 1)
    merged = O.merge_into(cf, {"facts": _facts("A2", 2)})                         # A2 is not in companyfacts: taken
    assert [r["accn"] for r in merged["us-gaap"]["Assets"]["units"]["USD"]] == ["A1", "A2"]
    merged = O.merge_into(cf, {"facts": _facts("A1", 999)})                       # SEC caught up with A1: backfill ignored
    assert merged["us-gaap"]["Assets"]["units"]["USD"] == [{"end": "2026-06-30", "val": 1, "accn": "A1"}]
    assert cf["us-gaap"]["Assets"]["units"]["USD"][0]["val"] == 1      # the input is not mutated


def test_load_facts_without_a_backfill_file_is_companyfacts(tmp_path):
    p = tmp_path / "CIK0000000001.json"
    p.write_text(json.dumps({"facts": _facts("A1", 1)}), encoding="utf-8")
    assert O.load_facts(p, backfill_dir=tmp_path / "none") == _facts("A1", 1)
    (tmp_path / "bf").mkdir()
    (tmp_path / "bf" / p.name).write_text(json.dumps({"facts": _facts("A2", 2)}), encoding="utf-8")
    got = O.load_facts(p, backfill_dir=tmp_path / "bf")
    assert [r["accn"] for r in got["us-gaap"]["Assets"]["units"]["USD"]] == ["A1", "A2"]


def test_duplicate_fact_keeps_the_more_precise_decimals():
    # AAPL 10-K 2026: the same UnrecognizedTaxBenefits at decimals -8 and -6 (the second is the real number)
    xml = _XML.replace(b'<xom:OwnLine', b'''<us-gaap:UnrecognizedTaxBenefits contextRef="i" unitRef="usd" decimals="-8">23200000000</us-gaap:UnrecognizedTaxBenefits>
 <us-gaap:UnrecognizedTaxBenefits contextRef="i" unitRef="usd" decimals="-6">23242000000</us-gaap:UnrecognizedTaxBenefits>
 <us-gaap:UnrecognizedTaxBenefits contextRef="i" unitRef="usd" decimals="-8">23200000000</us-gaap:UnrecognizedTaxBenefits>
 <xom:OwnLine''')
    out = B.parse_instance(xml, "A", "10-K", "2026-11-03")
    assert [r["val"] for r in out["us-gaap"]["UnrecognizedTaxBenefits"]["units"]["USD"]] == [23242000000]
    # the order does not matter and INF wins
    xml2 = _XML.replace(b'<xom:OwnLine', b'''<us-gaap:Foo contextRef="i" unitRef="usd" decimals="-6">5</us-gaap:Foo>
 <us-gaap:Foo contextRef="i" unitRef="usd" decimals="INF">7</us-gaap:Foo>
 <xom:OwnLine''')
    assert B.parse_instance(xml2, "A", "10-K", "2026-11-03")["us-gaap"]["Foo"]["units"]["USD"][0]["val"] == 7
