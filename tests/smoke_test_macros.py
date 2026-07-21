"""Verify .macro + .resource discovery and merging matches the original monolith."""
from pathlib import Path
from framework.builder.config_discovery import discover_files, load_and_merge

config_dir = Path("demo/configs")

resource_paths, macro_paths = discover_files(config_dir)

print("=== Discovered Files ===")
print(f"  .resource: {[p.name for p in resource_paths]}")
print(f"  .macro:    {[p.name for p in macro_paths]}")

merged = load_and_merge(resource_paths, macro_paths)

print("\n=== Merged Counts ===")
print(f"  Resources: {len(merged['resources'])}")
print(f"  Assets:    {len(merged['assets'])}")
print(f"  Jobs:      {len(merged['jobs'])}")
print(f"  Sensors:   {len(merged['sensors'])}")

# Verify totals match the original monolith
assert len(merged["resources"]) == 6, f"Expected 6 resources, got {len(merged['resources'])}"
assert len(merged["assets"]) == 5, f"Expected 5 assets, got {len(merged['assets'])}"
assert len(merged["jobs"]) == 2, f"Expected 2 jobs, got {len(merged['jobs'])}"
assert len(merged["sensors"]) == 2, f"Expected 2 sensors, got {len(merged['sensors'])}"

print("\n=== Asset Names ===")
for a in merged["assets"]:
    print(f"  - {a['name']} ({a['type']})")

print("\n=== Job Names ===")
for j in merged["jobs"]:
    print(f"  - {j['name']}")

print("\n=== Sensor Names ===")
for s in merged["sensors"]:
    print(f"  - {s['name']} ({s['type']})")

print("\n=== Resource Names ===")
for r in merged["resources"]:
    print(f"  - {r['name']} ({r['type']})")

# Verify all expected names are present
asset_names = {a["name"] for a in merged["assets"]}
expected_assets = {
    "cash_balance_api", "cash_balance_api_raw", "cash_balance_stage",
    "cash_balance_file", "cash_balance_file_raw", "cash_balance_stage",
}
# cash_balance_stage appears in api.macro only
assert "cash_balance_api" in asset_names
assert "cash_balance_api_raw" in asset_names
assert "cash_balance_stage" in asset_names
assert "cash_balance_file" in asset_names
assert "cash_balance_file_raw" in asset_names

print("\n✅ Multi-file split matches the original monolith!")

