from pathlib import Path

import dagster as dg

from framework.builder.core_loader import FrameworkLoader

config_dir = Path(__file__).resolve().parents[0] / "configs"

try:
    loader = FrameworkLoader(config_dir=config_dir, environment="local")
    defs = loader.get_definitions()

    dg.get_dagster_logger().info("✅ Framework pipeline loaded successfully")
    dg.get_dagster_logger().info(loader.describe_resources())

except Exception as e:
    dg.get_dagster_logger().error(f"❌ Failed to load framework config: {e}")
    import traceback

    traceback.print_exc()
    defs = dg.Definitions()
