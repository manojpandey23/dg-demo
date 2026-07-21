"""Verify load_all() builds the full Dagster Definitions from .macro + .resource files."""
from pathlib import Path
from framework.builder.core_loader import FrameworkLoader

config_dir = Path("demo/configs")

loader = FrameworkLoader(config_dir=config_dir, environment="local")
defs = loader.load_all()

assert defs is not None, "defs is None"
assert loader.resources is not None, "resources is None"

print(f"Resources: {len(loader.resources)}")
for name in sorted(loader.resources.keys()):
    print(f"  - {name}")

assert len(loader.resources) == 7  # 6 from .resource + noop_io_manager

print("\n✅ load_all() produced a valid dg.Definitions!")
