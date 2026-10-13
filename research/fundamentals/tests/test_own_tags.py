"""
Synthetic tests for the own (extension) tags read by name (ПНЛ4г): own_tags.csv,
collect_own_tags.parse_instance / own_records and build_panel's overlap-proven
continuation. No cache needed.

    python -m pytest research/fundamentals/tests/test_own_tags.py -q
"""

from __future__ import annotations

import sys
from pathlib import Path

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from research.fundamentals import build_panel, collect_own_tags  # noqa: E402

_CAPEX = "PaymentsToAcquirePropertyPlantAndEquipment"


def _f(end, start, val, filed):
    return {"start": start, "end": end, "val": val, "filed": filed, "accn": "a0",
            "form": "10-Q", "fy": 2024, "fp": "Q1"}


_CHAIN = [_f("2024-03-31", "2024-01-01", 100.0, "2024-04-30"),
          _f("2024-06-30", "2024-04-01", 110.0, "2024-07-30")]


def _facts(chain=_CHAIN):
    return {"us-gaap": {_CAPEX: {"units": {"USD": list(chain)}}}}


def _entry(filed, facts, form="10-Q"):
    return {"form": form, "filed": filed, "instance": "x_htm.xml", "fy": "2025", "fp": "Q2",
            "tags": sorted(facts), "facts": facts}


def _own_cache(q2_2024=110.0, tag="FooCapex"):
    # the filer moved the line to its own tag: the 2025 filings re-file the 2024
    # comparatives under it and carry on
    return {
        "b1": _entry("2025-04-30", {tag: [["2024-01-01", "2024-03-31", 100.0],
                                          ["2025-01-01", "2025-03-31", 115.0]]}),
        "b2": _entry("2025-07-30", {tag: [["2024-04-01", "2024-06-30", q2_2024],
                                          ["2025-04-01", "2025-06-30", 120.0]]}),
    }


def _capex(rows):
    return sorted((r[4], r[5], r[10]) for r in rows if r[2] == "capex")


def _extract(own_specs, own_cache, chain=_CHAIN):
    stats = {"missing_concept": {}}
    rows = build_panel._extract_company("ZZZ", "0000000001", _facts(chain), stats,
                                        own_specs=own_specs, own_cache=own_cache)
    return _capex(rows), stats


def test_own_tag_continues_the_series_only_forward_when_proven():
    rows, stats = _extract({"capex": ("zzz:FooCapex",)}, _own_cache())
    assert rows == [("2024-01-01", "2024-03-31", 100.0), ("2024-04-01", "2024-06-30", 110.0),
                    ("2025-01-01", "2025-03-31", 115.0), ("2025-04-01", "2025-06-30", 120.0)]
    assert stats["own_admitted"] == ["ZZZ.capex:zzz:FooCapex"]


def test_own_tag_without_proof_never_enters():
    # one shared period 1% off: a different line, the gap stays
    rows, stats = _extract({"capex": ("zzz:FooCapex",)}, _own_cache(q2_2024=111.1))
    assert rows == _capex(build_panel._emit_rows("ZZZ", "1", "capex", "USD", _CHAIN))
    assert stats["own_refused"] == ["ZZZ.capex:zzz:FooCapex"]


def test_own_tag_needs_two_shared_periods():
    rows, stats = _extract({"capex": ("zzz:FooCapex",)}, _own_cache(), chain=_CHAIN[1:])
    assert [r[1] for r in rows] == ["2024-06-30"] and "own_admitted" not in stats


def test_own_tag_without_a_chain_never_enters():
    rows, _stats = _extract({"capex": ("zzz:FooCapex",)}, _own_cache(), chain=[])
    assert rows == []


def test_own_tag_is_read_by_name_only():
    # the cache carries the tag, but the ticker is not in own_tags.csv: nothing enters
    rows, stats = _extract(None, _own_cache())
    assert [r[1] for r in rows] == ["2024-03-31", "2024-06-30"] and "own_admitted" not in stats


def test_own_sum_takes_every_part_from_the_same_filing():
    cache = {
        "b1": _entry("2025-04-30", {"A": [["2024-01-01", "2024-03-31", 60.0]],
                                    "B": [["2024-01-01", "2024-03-31", 40.0],
                                          ["2024-04-01", "2024-06-30", 50.0]]}),   # A missing: dropped
        "b2": _entry("2025-07-30", {"A": [["2024-04-01", "2024-06-30", 60.0]]}),  # B in another filing
    }
    recs = collect_own_tags.own_records(cache, ("x:A", "x:B"))
    assert [(r["start"], r["end"], r["val"], r["accn"]) for r in recs] == [
        ("2024-01-01", "2024-03-31", 100.0, "b1")]


_XML = b"""<?xml version="1.0"?>
<xbrli:xbrl xmlns:xbrli="http://www.xbrl.org/2003/instance" xmlns:xbrldi="http://xbrl.org/2006/xbrldi"
  xmlns:iso4217="http://www.xbrl.org/2003/iso4217" xmlns:dei="http://xbrl.sec.gov/dei/2024"
  xmlns:us-gaap="http://fasb.org/us-gaap/2024" xmlns:zzz="http://www.zzz.com/20250630">
 <xbrli:context id="c1"><xbrli:entity><xbrli:identifier scheme="x">1</xbrli:identifier></xbrli:entity>
  <xbrli:period><xbrli:startDate>2025-01-01</xbrli:startDate><xbrli:endDate>2025-06-30</xbrli:endDate></xbrli:period></xbrli:context>
 <xbrli:context id="c2"><xbrli:entity><xbrli:identifier scheme="x">1</xbrli:identifier>
  <xbrli:segment><xbrldi:explicitMember dimension="us-gaap:StatementBusinessSegmentsAxis">zzz:AMember</xbrldi:explicitMember></xbrli:segment></xbrli:entity>
  <xbrli:period><xbrli:startDate>2025-01-01</xbrli:startDate><xbrli:endDate>2025-06-30</xbrli:endDate></xbrli:period></xbrli:context>
 <xbrli:unit id="usd"><xbrli:measure>iso4217:USD</xbrli:measure></xbrli:unit>
 <xbrli:unit id="eur"><xbrli:measure>iso4217:EUR</xbrli:measure></xbrli:unit>
 <dei:DocumentPeriodEndDate contextRef="c1">2025-06-30</dei:DocumentPeriodEndDate>
 <zzz:FooCapex contextRef="c1" unitRef="usd" decimals="-6">120000000</zzz:FooCapex>
 <zzz:FooCapex contextRef="c2" unitRef="usd" decimals="-6">70000000</zzz:FooCapex>
 <zzz:BarCapex contextRef="c1" unitRef="eur" decimals="-6">5000000</zzz:BarCapex>
 <us-gaap:FooCapex contextRef="c1" unitRef="usd" decimals="-6">1</us-gaap:FooCapex>
</xbrli:xbrl>"""


def test_parse_instance_keeps_only_undimensioned_usd_extension_facts():
    got = collect_own_tags.parse_instance(_XML, {"FooCapex", "BarCapex"})
    assert got["facts"] == {"FooCapex": [["2025-01-01", "2025-06-30", 120000000.0]]}
    assert got["period"] == "2025-06-30"


def test_own_tags_table_is_by_name_and_well_formed():
    table = collect_own_tags.load_table()
    assert len(table) == 12
    for (ticker, concept), tags in table.items():
        assert concept in build_panel._CONCEPTS, (ticker, concept)
        for t in tags:
            prefix, name = t.split(":", 1)
            assert prefix and name and prefix not in ("us-gaap", "dei"), (ticker, t)
