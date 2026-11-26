"""
The net_income NCI choice is locked per name (ПНЛ-NCI, 26.11.2026): build_panel's
nci_lock.csv, the check file and the gate G6n. The mutation: new quarters that flip the rule
must leave the name's history alone and make G6n report the mismatch. No cache needed.

    python -m pytest research/fundamentals/tests/test_nci_lock.py -q
"""

from __future__ import annotations

import csv
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parents[3]
if str(_REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(_REPO_ROOT))

from research.fundamentals import build_panel, validate_gates  # noqa: E402

NI, PL = build_panel._NI_TAG, build_panel._NI_CONSOLIDATED_TAG


def _f(start, end, val, filed):
    return {"start": start, "end": end, "val": val, "filed": filed, "accn": f"a{filed}",
            "form": "10-Q", "fy": 2026, "fp": "Q1"}


def _facts(ni, pl):
    return {"us-gaap": {NI: {"units": {"USD": ni}}, PL: {"units": {"USD": pl}}}}


_QUARTERS = [("2024-01-01", "2024-03-31", "2024-05-01"), ("2024-04-01", "2024-06-30", "2024-08-01"),
             ("2024-07-01", "2024-09-30", "2024-11-01"), ("2024-10-01", "2024-12-31", "2025-02-20"),
             ("2025-01-01", "2025-03-31", "2025-05-01"), ("2025-04-01", "2025-06-30", "2025-08-01"),
             ("2025-07-01", "2025-09-30", "2025-11-01"), ("2025-10-01", "2025-12-31", "2026-02-20")]
_NEW = [("2026-01-01", "2026-03-31", "2026-05-01"), ("2026-04-01", "2026-06-30", "2026-08-01"),
        ("2026-07-01", "2026-09-30", "2026-11-01"), ("2026-10-01", "2026-12-31", "2027-02-20"),
        ("2027-01-01", "2027-03-31", "2027-05-01")]
# SO shape: an old year that only ProfitLoss carries (2012 FY, from the 2015 10-K)
_OLD_PL_ONLY = _f("2012-01-01", "2012-12-31", 2415.0, "2015-03-02")
_LOCK = [{"tag": NI, "from": "2026-01-01", "rule_seen": NI, "reason": "test"}]


def _base():
    """8 shared quarters with a material NCI (ProfitLoss 5% above): the rule says NetIncomeLoss."""
    ni = [_f(s, e, 100.0, d) for s, e, d in _QUARTERS]
    pl = [_f(s, e, 105.0, d) for s, e, d in _QUARTERS] + [_OLD_PL_ONLY]
    return ni, pl


def _mutated():
    """+5 new quarters where the two tags agree: 5 of the latest 8 at 0%, the rule flips."""
    ni, pl = _base()
    return (ni + [_f(s, e, 100.0, d) for s, e, d in _NEW],
            pl + [_f(s, e, 100.0, d) for s, e, d in _NEW])


def _history(recs, before="2026-01-01"):
    return sorted((r.get("start", ""), r["end"], r["filed"], r["val"]) for r in recs if r["filed"] < before)


def test_mutation_flips_the_rule():
    # the mutation has teeth: the rule alone would now admit ProfitLoss as-is
    assert build_panel.nci_rule(_facts(*_base()))["rule"] == NI
    assert build_panel.nci_rule(_facts(*_mutated()))["rule"] == PL


def test_without_the_lock_the_new_quarters_rewrite_history():
    # the old behaviour (no lock): the 2012 ProfitLoss year enters the history
    before, _ = build_panel._extract_net_income(_facts(*_base()))
    after, _ = build_panel._extract_net_income(_facts(*_mutated()))
    assert _history(before) != _history(after)
    assert ("2012-01-01", "2012-12-31", "2015-03-02", 2415.0) in _history(after)


def test_locked_history_survives_the_flip_and_g6n_reports_it(tmp_path):
    before, _ = build_panel._extract_net_income(_facts(*_base()), _LOCK)
    after, refused = build_panel._extract_net_income(_facts(*_mutated()), _LOCK)
    assert _history(after) == _history(before)
    assert ("2012-01-01", "2012-12-31", "2015-03-02", 2415.0) not in _history(after)
    assert refused == 1

    assert build_panel.nci_check_row("SO", _facts(*_base()), _LOCK)["status"] == "ok"
    row = build_panel.nci_check_row("SO", _facts(*_mutated()), _LOCK)
    assert (row["status"], row["rule"], row["locked_tag"]) == ("MISMATCH", PL, NI)

    path = tmp_path / "nci_lock_check.csv"
    build_panel.write_nci_check([row], path)
    fails = validate_gates.g6n_nci_lock(path)
    assert len(fails) == 1 and "SO" in fails[0]
    assert "the rule now says ProfitLoss" in fails[0]


def test_a_new_row_moves_only_what_is_filed_from_its_date():
    lock = _LOCK + [{"tag": PL, "from": "2027-06-01", "rule_seen": PL, "reason": "Ц.: test"}]
    ni, pl = _mutated()
    late = _f("2019-01-01", "2019-12-31", 900.0, "2027-07-01")      # filed after the new row
    recs, _ = build_panel._extract_net_income(_facts(ni, pl + [late]), lock)
    hist = _history(recs, before="2027-06-01")
    assert ("2012-01-01", "2012-12-31", "2015-03-02", 2415.0) not in hist
    assert any(r["filed"] == "2027-07-01" and r["val"] == 900.0 for r in recs)
    assert build_panel.nci_check_row("SO", _facts(ni, pl), lock)["status"] == "ok"


def test_override_keeps_the_old_tag_and_quiets_the_gate():
    # Ц. keeps NetIncomeLoss although the rule says ProfitLoss: rule_seen records it
    lock = _LOCK + [{"tag": NI, "from": "2026-11-27", "rule_seen": PL, "reason": "Ц.: test"}]
    recs, _ = build_panel._extract_net_income(_facts(*_mutated()), lock)
    assert all(r["filed"] != "2015-03-02" for r in recs)
    assert build_panel.nci_check_row("SO", _facts(*_mutated()), lock)["status"] == "ok"


def _write_lock(path, rows):
    with open(path, "w", encoding="utf-8", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=build_panel.NCI_LOCK_COLUMNS)
        w.writeheader()
        w.writerows(rows)


def test_lock_rows_need_a_reason_and_a_known_tag(tmp_path):
    p = tmp_path / "nci_lock.csv"
    good = {"ticker": "SO", "tag": PL, "from": "2026-11-26", "rule_seen": PL, "reason": "r"}
    _write_lock(p, [{**good, "from": "2027-01-01", "tag": NI}, good])
    assert [r["from"] for r in build_panel.load_nci_lock(p)["SO"]] == ["2026-11-26", "2027-01-01"]
    for bad in ({**good, "reason": " "}, {**good, "tag": "Revenues"}, {**good, "from": "26.11.2026"}):
        _write_lock(p, [bad])
        with pytest.raises(ValueError):
            build_panel.load_nci_lock(p)


def test_g6n_unlocked_name_fails_and_absent_file_is_skipped(tmp_path):
    ni, pl = _base()
    path = tmp_path / "nci_lock_check.csv"
    build_panel.write_nci_check([build_panel.nci_check_row("NEW", _facts(ni, pl), None),
                                 build_panel.nci_check_row("SO", _facts(ni, pl), _LOCK)], path)
    fails = validate_gates.g6n_nci_lock(path)
    assert len(fails) == 1 and "NEW" in fails[0] and "--lock-unlocked" in fails[0]
    assert validate_gates.g6n_nci_lock(tmp_path / "absent.csv") == []


def test_committed_lock_loads_and_every_row_has_a_reason():
    lock = build_panel.load_nci_lock()
    assert len(lock) > 300
    for tk in ("SO", "EVRG", "DLR"):
        assert lock[tk][-1]["tag"] == PL and "fa61681" in lock[tk][-1]["reason"]
