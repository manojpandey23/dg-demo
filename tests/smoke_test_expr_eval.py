"""Smoke test for decorator-based expr_eval + file_tracker."""
import datetime as dt
import tempfile
import time
from pathlib import Path
from unittest.mock import patch

from framework.utils.expr_eval import evaluate_expr, get_expr_registry
from framework.utils.file_tracker import FileTracker

# Use a fixed date so assertions are deterministic
FIXED_TODAY = dt.date(2026, 3, 21)

print("=== Registry ===")
registry = get_expr_registry()
print(f"  Registered functions: {sorted(registry.keys())}")
assert "rdd" in registry
assert "today" in registry

print("\n=== Path Expression Tests ===")
with patch("framework.utils.expr_eval.dt") as mock_dt:
    mock_dt.date.today.return_value = FIXED_TODAY

    assert evaluate_expr("path/{rdd()}/") == "path/20260321/"
    print("  {rdd()}              => path/20260321/")

    assert evaluate_expr("path/{rdd('yyyy_mm_dd')}/") == "path/2026_03_21/"
    print("  {rdd('yyyy_mm_dd')} => path/2026_03_21/")

    assert evaluate_expr("path/{rdd('yyyy/mm/dd')}/") == "path/2026/03/21/"
    print("  {rdd('yyyy/mm/dd')} => path/2026/03/21/")

    assert evaluate_expr("path/{rdd('yy')}/") == "path/26/"
    print("  {rdd('yy')}         => path/26/")

print("\n=== Pattern Expression Tests ===")
with patch("framework.utils.expr_eval.dt") as mock_dt:
    mock_dt.date.today.return_value = FIXED_TODAY

    assert evaluate_expr("file_{today()}.csv") == "file_20260321.csv"
    print("  {today()}             => file_20260321.csv")

    assert evaluate_expr("file_{today()}_*.csv") == "file_20260321_*.csv"
    print("  {today()}_*           => file_20260321_*.csv")

    assert evaluate_expr("file_{today(format='yyyymmdd')}_*.csv") == "file_20260321_*.csv"
    print("  {today(format=...)}   => file_20260321_*.csv")

    assert evaluate_expr("file_{today(format='yyyy_mm_dd')}_*.csv") == "file_2026_03_21_*.csv"
    print("  {today(format=yyyy_mm_dd)} => file_2026_03_21_*.csv")

print("\n=== No-placeholder passthrough ===")
assert evaluate_expr("/data/static/files") == "/data/static/files"
print("  Static path unchanged ✅")

print("\n=== Invalid expression ===")
try:
    evaluate_expr("{unknown_fn()}")
    assert False, "Should have raised"
except ValueError as e:
    print(f"  Unknown function raises: {e.__class__.__name__} ✅")

print("\n=== File Tracker Tests ===")
with tempfile.TemporaryDirectory() as d:
    dp = Path(d)
    (dp / "data_20260321_001.csv").write_text("a,b\n1,2")
    (dp / "data_20260321_002.csv").write_text("c,d\n3,4")
    (dp / "readme.txt").write_text("ignore me")

    known = FileTracker.deserialize_cursor(None)
    new_files, state = FileTracker.detect_new_or_modified(dp, "data_*_*.csv", known)
    assert len(new_files) == 2
    print(f"  First scan:  {len(new_files)} new file(s) ✅")

    cursor = FileTracker.serialize_cursor(state)
    known2 = FileTracker.deserialize_cursor(cursor)
    assert len(known2) == 2
    print(f"  Cursor tracks {len(known2)} file(s) ✅")

    new_files2, _ = FileTracker.detect_new_or_modified(dp, "data_*_*.csv", known2)
    assert len(new_files2) == 0
    print(f"  Second scan: {len(new_files2)} new file(s) ✅")

    time.sleep(0.05)
    (dp / "data_20260321_001.csv").write_text("a,b\n1,2,MODIFIED")

    new_files3, _ = FileTracker.detect_new_or_modified(dp, "data_*_*.csv", known2)
    assert len(new_files3) == 1
    print(f"  After modify: {len(new_files3)} new file(s) ✅")
    print(f"    modified: {new_files3[0].name}")

print("\n✅ All smoke tests passed!")

