from pathlib import Path

from framework.builder.core_loader import FrameworkLoader

config_dir = Path(__file__).resolve().parents[0] / "configs"

loader = FrameworkLoader(config_dir=config_dir, environment="local")
defs = loader.get_definitions()
