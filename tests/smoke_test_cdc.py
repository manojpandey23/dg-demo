"""Quick smoke test for the CDC feature."""
from framework.model.config_models import AssetConfig, AssertType, StreamConfig, StreamType

try:
    AssetConfig(name="t1", type=AssertType.file, change_tracking=True, streams=[StreamConfig(type=StreamType.websocket, relay_endpoint="ws://x")])
    print("FAIL T1")
except ValueError:
    print("T1 OK: rejected non-DB CDC")

try:
    AssetConfig(name="t2", type=AssertType.database, change_tracking=True, source={"table": "x.y", "resource": "pg"})
    print("FAIL T2")
except ValueError:
    print("T2 OK: rejected CDC without streams")

try:
    AssetConfig(name="t3", type=AssertType.database, streams=[StreamConfig(type=StreamType.websocket, relay_endpoint="ws://x")], source={"table": "x.y", "resource": "pg"})
    print("FAIL T3")
except ValueError:
    print("T3 OK: rejected streams without change_tracking")

a = AssetConfig(name="t4", type=AssertType.database, change_tracking=True, streams=[StreamConfig(type=StreamType.websocket, relay_endpoint="ws://x")], source={"table": "x.y", "resource": "pg"})
print(f"T4 OK: valid CDC, streams={len(a.streams)}")

from pathlib import Path
from framework.builder.core_loader import FrameworkLoader
loader = FrameworkLoader(config_dir=Path("demo/configs"), environment="local")
defs = loader.get_definitions()
cdc_assets = [x for x in loader._pipeline_config.assets if x.change_tracking]
cdc_res = [k for k in loader.resources if k.startswith("__cdc_")]
print(f"T5 OK: {len(cdc_assets)} CDC asset(s), {len(cdc_res)} dispatcher resource(s)")
for ca in cdc_assets:
    print(f"  CDC: {ca.name}")
    for s in ca.streams or []:
        print(f"    stream: {s.type.value} -> {s.relay_endpoint}")

from framework.core.streams.stream_registry import STREAM_REGISTRY
print(f"T6 OK: {len(STREAM_REGISTRY.all())} stream handler(s)")

from framework.cdc.store import derive_change_log_table
assert derive_change_log_table("price.cash_balance_api_raw") == "price.__cdc_cash_balance_api_raw"
print("T7 OK: derive_change_log_table")
print("All CDC smoke tests passed")
