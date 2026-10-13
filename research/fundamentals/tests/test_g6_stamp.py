"""G6 stamp: write/check round-trip, tampered panel, FAIL verdict, missing stamp (ПНЛ5)."""
import gzip
import json

from research.fundamentals import g6_stamp


def _panel(tmp_path, body=b"a,b\n1,2\n"):
    p = tmp_path / "panel.csv.gz"
    with gzip.open(p, "wb") as fh:
        fh.write(body)
    return p


def test_roundtrip_passes(tmp_path):
    p, s = _panel(tmp_path), tmp_path / "stamp.json"
    g6_stamp.write_stamp(True, 0, panel_path=p, stamp_path=s)
    ok, msg = g6_stamp.check_stamp(p, s)
    assert ok, msg


def test_swapped_panel_refused(tmp_path):
    p, s = _panel(tmp_path), tmp_path / "stamp.json"
    g6_stamp.write_stamp(True, 0, panel_path=p, stamp_path=s)
    _panel(tmp_path, b"a,b\n9,9\n")          # same path, different bytes
    ok, msg = g6_stamp.check_stamp(p, s)
    assert not ok and "sha256" in msg


def test_fail_verdict_refused(tmp_path):
    p, s = _panel(tmp_path), tmp_path / "stamp.json"
    g6_stamp.write_stamp(False, 3, panel_path=p, stamp_path=s)
    ok, msg = g6_stamp.check_stamp(p, s)
    assert not ok and "не е PASS" in msg
    assert json.loads(s.read_text(encoding="utf-8"))["result"] == "FAIL"


def test_missing_stamp_refused(tmp_path):
    ok, msg = g6_stamp.check_stamp(_panel(tmp_path), tmp_path / "nope.json")
    assert not ok and "няма печат" in msg


def test_real_panel_is_stamped_pass():
    """The committed stamp matches the committed panel (rebuild => rerun validate_gates)."""
    ok, msg = g6_stamp.check_stamp()
    assert ok, msg
