"""
УБР3 (29.09.2026): enabled=2 е ГОСТ, име извън S&P 500 (LYFT), което органът чете, без да го
пуска в S&P конвейера.

Договорът на последната колона на config/universe.csv:
    1 = член на S&P 500: живият дневен конвейер, публикуваното ранжиране и консенсусните ingest-и
    2 = гост: само SEC (collect_edgar, build_panel) и проверката на кохортите на data-core
    0 = изключен нарочно

Гейтът пази двете посоки: гост не влиза в каноничното множество (503 имена, същото число пазят
validate_gates и build_membership), а SEC събирачите не го пропускат. Пазачът е доказан с мутация
вътре в самия тест: LYFT със статус 1 в копие ТРЯБВА да го накара да падне.
"""

from __future__ import annotations

import csv
from pathlib import Path

from research.fundamentals import build_panel, collect_edgar
from src.lib.io_utils import UNIVERSE_PATH, read_universe

_CANON_N = 503
_GUEST = "LYFT"


def _rows(path: Path) -> list[dict]:
    with open(path, encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def canon_violations(path: Path) -> list[str]:
    """Празен списък = договорът на колоната е спазен за този universe.csv."""
    rows = _rows(path)
    bad = []
    statuses = {r["enabled"].strip() for r in rows}
    if not statuses <= {"0", "1", "2"}:
        bad.append("непознат статус: %s" % sorted(statuses - {"0", "1", "2"}))
    canon = [r["symbol"].strip() for r in rows if r["enabled"].strip() == "1"]
    if len(canon) != _CANON_N:
        bad.append("каноничното множество е %d, а не %d" % (len(canon), _CANON_N))
    if _GUEST in canon:
        bad.append("%s е със статус 1: гост в каноничното множество" % _GUEST)
    for r in rows:
        if r["enabled"].strip() == "2" and not (r.get("cik") or "").strip():
            bad.append("гост без CIK: %s (SEC събирачът би го пропуснал тихо)" % r["symbol"])
    return bad


def test_real_universe_keeps_the_column_contract():
    assert canon_violations(UNIVERSE_PATH) == []


def test_guest_is_status_2_with_the_sec_cik():
    lyft = {r["symbol"].strip(): r for r in _rows(UNIVERSE_PATH)}[_GUEST]
    assert lyft["enabled"].strip() == "2"
    assert lyft["cik"].strip() == "1759509"


def test_live_pipeline_never_sees_a_guest():
    tickers = set(read_universe(enabled_only=True)["ticker"])
    assert _GUEST not in tickers
    assert len(tickers) == _CANON_N
    assert _GUEST in set(read_universe(enabled_only=False)["ticker"])


def test_sec_loaders_take_canon_and_guest_but_not_switched_off(tmp_path):
    p = tmp_path / "universe.csv"
    p.write_text("symbol,cik,name,sector,industry,enabled\n"
                 "AAA,1,A,Health Care,x,1\n"
                 "BBB,2,B,Health Care,x,2\n"
                 "CCC,3,C,Health Care,x,0\n"
                 "DDD,4,D,Health Care,x,\n", encoding="utf-8")
    assert [r["symbol"] for r in collect_edgar.load_universe(p)] == ["AAA", "BBB", "DDD"]
    assert [t for t, _cik in build_panel.load_universe(p)] == ["AAA", "BBB", "DDD"]
    assert collect_edgar.SEC_STATUSES == build_panel.SEC_STATUSES == ("1", "2", "")


def test_real_sec_loaders_carry_the_guest_with_a_padded_cik():
    assert (_GUEST, "0001759509") in build_panel.load_universe()
    assert _GUEST in {r["symbol"] for r in collect_edgar.load_universe()}


def test_coverage_gate_counts_the_canon_only():
    from research.fundamentals import validate_gates

    canon = validate_gates._canon_symbols()
    assert _GUEST not in canon
    assert len(canon) == validate_gates._universe_size() == _CANON_N


def test_guard_fires_when_the_guest_is_promoted_or_loses_its_cik(tmp_path):
    src = UNIVERSE_PATH.read_bytes().decode("utf-8")
    promoted = tmp_path / "promoted.csv"
    promoted.write_bytes(src.replace("LYFT,1759509,Lyft,Industrials,,2",
                                     "LYFT,1759509,Lyft,Industrials,,1").encode("utf-8"))
    assert canon_violations(promoted), "гост със статус 1 трябва да пада"
    no_cik = tmp_path / "no_cik.csv"
    no_cik.write_bytes(src.replace("LYFT,1759509,Lyft,Industrials,,2",
                                   "LYFT,,Lyft,Industrials,,2").encode("utf-8"))
    assert any("без CIK" in v for v in canon_violations(no_cik))
