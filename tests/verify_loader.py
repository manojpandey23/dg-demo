"""Verify the new auto-discovery FrameworkLoader path."""
from pathlib import Path
from framework.builder.core_loader import FrameworkLoader

config_dir = Path("src/test_domain/configs")

# --- New path: just config_dir, auto-discovers .resource + .macro ---
loader = FrameworkLoader(config_dir=config_dir, environment="local")
defs = loader.get_definitions()

assert defs is not None
assert loader.resources is not None
assert loader._pipeline_config is not None

print(f"Resources: {len(loader.resources)}")
print(f"Assets:    {len(loader._pipeline_config.assets)}")
print(f"Jobs:      {len(loader._pipeline_config.jobs)}")
print(f"Sensors:   {len(loader._pipeline_config.sensors)}")

assert len(loader.resources) == 7   # 6 from .resource + noop_io_manager
assert len(loader._pipeline_config.assets) == 5
assert len(loader._pipeline_config.jobs) == 2
assert len(loader._pipeline_config.sensors) == 2

# Verify asset names
names = {a.name for a in loader._pipeline_config.assets}
assert "cash_balance_api" in names
assert "cash_balance_file" in names
assert "cash_balance_stage" in names
print(f"Assets:    {sorted(names)}")

# --- Legacy path still works ---
legacy = FrameworkLoader(
    config_dir=config_dir,
    resources_yaml="resources_backup.yaml",
    environment="local",
)
legacy_defs = legacy.get_definitions("framework_pipeline_backup.yaml")
assert legacy_defs is not None
assert len(legacy.resources) > 0
print("\nLegacy path: ✅")

print("\n✅ All paths verified!")

