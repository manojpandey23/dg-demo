"""Smoke test: config_discovery + ref_resolver + core_loader integration."""
import tempfile
from pathlib import Path

from framework.builder.config_discovery import discover_files, load_and_merge
from framework.builder.ref_resolver import resolve_refs

# =============================================================
# 1. Test config_discovery
# =============================================================
print("=== Config Discovery ===")

with tempfile.TemporaryDirectory() as d:
    dp = Path(d)

    # Create .resource files
    (dp / "main.resource").write_text("""
resources:
  - name: api_resource
    type: api
    config:
      base_url: "http://localhost:8000"
      timeout: 10
  - name: pg_resource
    type: postgres
    config:
      host: localhost
      port: 5432
""")

    (dp / "vault.resource").write_text("""
resources:
  - name: vault_main
    type: vault
    config:
      url: "https://vault.example.com"
""")

    # Create .macro files
    (dp / "api_pipeline.macro").write_text("""
assets:
  - name: cash_balance_api
    type: api
    source:
      endpoint: /cash_balance
      method: GET

  - name: cash_balance_raw
    type: database
    depends_on: [cash_balance_api]
    source:
      resource: pg_resource
      table: price.cash_balance_raw

jobs:
  - name: api_ingestion_job
    flow:
      definition: cash_balance_api >> cash_balance_raw
""")

    (dp / "file_pipeline.macro").write_text("""
assets:
  - name: file_ingest
    type: file
    source:
      file_path: /data/incoming

sensors:
  - name: file_sensor
    type: file_drop
    trigger:
      type: job
      target: api_ingestion_job
    config:
      file_path: /data/incoming
      file_pattern: "*.csv"
""")

    # Also put some .yaml files — these should NOT be discovered
    (dp / "ignored.yaml").write_text("key: value")

    # Discover
    resource_paths, macro_paths = discover_files(dp)
    print(f"  .resource files: {[p.name for p in resource_paths]}")
    print(f"  .macro files:    {[p.name for p in macro_paths]}")
    assert len(resource_paths) == 2
    assert len(macro_paths) == 2

    # Merge
    merged = load_and_merge(resource_paths, macro_paths)
    print(f"  Merged resources: {len(merged['resources'])}")
    print(f"  Merged assets:    {len(merged['assets'])}")
    print(f"  Merged jobs:      {len(merged['jobs'])}")
    print(f"  Merged sensors:   {len(merged['sensors'])}")
    assert len(merged["resources"]) == 3
    assert len(merged["assets"]) == 3
    assert len(merged["jobs"]) == 1
    assert len(merged["sensors"]) == 1

print("  ✅ Discovery + merge passed\n")

# =============================================================
# 2. Test ref_resolver
# =============================================================
print("=== Ref Resolver ===")

config = {
    "resources": [
        {
            "name": "api_resource",
            "type": "api",
            "config": {
                "base_url": "http://localhost:8000",
                "timeout": 10,
            },
        },
        {
            "name": "pg_resource",
            "type": "postgres",
            "config": {
                "host": "db.example.com",
                "port": 5432,
            },
        },
    ],
    "assets": [
        {
            "name": "raw_api",
            "type": "api",
            "source": {
                "endpoint": "/prices",
            },
            "columns": [
                {
                    "name": "account_cd",
                    "dtype": "string",
                    "expr": 'ref("account_cd")',  # runtime expr — should be SKIPPED
                },
            ],
        },
        {
            "name": "raw_db",
            "type": "database",
            "source": {
                # ref to the pg_resource host
                "host": 'ref("resources[?name==\'pg_resource\'].config.host | [0]")',
                # ref to asset name
                "upstream": 'ref("assets[?name==\'raw_api\'].source.endpoint | [0]")',
            },
            "columns": [
                {
                    "name": "amt",
                    "dtype": "float",
                    "expr": 'ref("amt")',  # runtime expr — should be SKIPPED
                },
            ],
        },
    ],
}

resolved = resolve_refs(config)

# Check cross-section ref resolved
assert resolved["assets"][1]["source"]["host"] == "db.example.com", \
    f"Expected 'db.example.com', got {resolved['assets'][1]['source']['host']}"
print(f"  resources→asset ref: {resolved['assets'][1]['source']['host']} ✅")

assert resolved["assets"][1]["source"]["upstream"] == "/prices", \
    f"Expected '/prices', got {resolved['assets'][1]['source']['upstream']}"
print(f"  asset→asset ref:     {resolved['assets'][1]['source']['upstream']} ✅")

# Check expr fields are NOT resolved (runtime refs)
assert resolved["assets"][0]["columns"][0]["expr"] == 'ref("account_cd")'
assert resolved["assets"][1]["columns"][0]["expr"] == 'ref("amt")'
print("  expr fields skipped: ✅")

# Test circular ref detection
try:
    circular = {
        "a": 'ref("b")',
        "b": 'ref("a")',
    }
    resolve_refs(circular)
    assert False, "Should have raised"
except ValueError as e:
    print(f"  Circular ref detected: ✅")

# Test unresolvable ref
try:
    resolve_refs({"x": 'ref("nonexistent.path")'})
    assert False, "Should have raised"
except ValueError as e:
    print(f"  Unresolvable ref detected: ✅")

# Test non-ref strings pass through
plain = resolve_refs({"a": "hello", "b": 42, "c": ["x", "y"]})
assert plain == {"a": "hello", "b": 42, "c": ["x", "y"]}
print("  Non-ref passthrough: ✅")

print("\n=== Duplicate Detection ===")
with tempfile.TemporaryDirectory() as d:
    dp = Path(d)
    (dp / "a.macro").write_text("""
assets:
  - name: dup_asset
    type: api
""")
    (dp / "b.macro").write_text("""
assets:
  - name: dup_asset
    type: database
""")
    rp, mp = discover_files(dp)
    try:
        load_and_merge(rp, mp)
        assert False, "Should have raised"
    except ValueError as e:
        print(f"  Duplicate asset name detected: ✅")

print("\n✅ All smoke tests passed!")

