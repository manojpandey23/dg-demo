"""Verify legacy FrameworkLoader path still works with backup YAML."""
from pathlib import Path
from framework.builder.core_loader import FrameworkLoader

config_dir = Path("demo/configs")

loader = FrameworkLoader(
    config_dir=config_dir,
    resources_yaml="resources_backup.yaml",
    environment="local",
)

assets, jobs, sensors, asset_checks = loader.load_from_file("framework_pipeline_backup.yaml")

print(f"Assets:       {len(assets)}")
print(f"Jobs:         {len(jobs)}")
print(f"Sensors:      {len(sensors)}")
print(f"Asset checks: {len(asset_checks)}")
print(f"Resources:    {len(loader.resources)}")

assert len(assets) > 0, "Expected at least 1 asset"
assert len(jobs) > 0, "Expected at least 1 job"
assert len(sensors) > 0, "Expected at least 1 sensor"
assert len(loader.resources) > 0, "Expected at least 1 resource"

print("\n✅ Legacy loader path works!")

