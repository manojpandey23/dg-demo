"""Smoke test for expr_eval + file_tracker."""
import datetime as dt
import tempfile
import time
from pathlib import Path

from framework.utils.expr_eval import evaluate_path_expr, evaluate_pattern_expr
from framework.utils.file_tracker import FileTracker

ref = dt.date(2026, 3, 21)

print("=== Path Expression Tests ===")
assert evaluate_path_expr("path/{rdd}/", ref) == "path/20260321/"
print("  {rdd}              => path/20260321/")

assert evaluate_path_expr("path/{rdd('yyyy_mm_dd')}/", ref) == "path/2026_03_21/"
print("  {rdd('yyyy_mm_dd')} => path/2026_03_21/")

assert evaluate_path_expr("path/{rdd('yyyy/mm/dd')}/", ref) == "path/2026/03/21/"
print("  {rdd('yyyy/mm/dd')} => path/2026/03/21/")

assert evaluate_path_expr("path/{rdd('yy')}/", ref) == "path/26/"
print("  {rdd('yy')}         => path/26/")

print("\n=== Pattern Expression Tests ===")
assert evaluate_pattern_expr("file_{today}.csv", ref) == "file_20260321.csv"
print("  {today}             => file_20260321.csv")

assert evaluate_pattern_expr("file_{today}_*.csv", ref) == "file_20260321_*.csv"
print("  {today}_*           => file_20260321_*.csv")

assert evaluate_pattern_expr("file_{today(format='yyyymmdd')}_*.csv", ref) == "file_20260321_*.csv"
print("  {today(format=...)} => file_20260321_*.csv")

print("\n=== File Tracker Tests ===")
with tempfile.TemporaryDirectory() as d:
    dp = Path(d)
    (dp / "data_20260321_001.csv").write_text("a,b\n1,2")
    (dp / "data_20260321_002.csv").write_text("c,d\n3,4")
    (dp / "readme.txt").write_text("ignore me")

    known = FileTracker.deserialize_cursor(None)
    new_files, state = FileTracker.detect_new_or_modified(dp, "data_*_*.csv", known)
    assert len(new_files) == 2, f"Expected 2, got {len(new_files)}"
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

