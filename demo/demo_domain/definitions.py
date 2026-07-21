"""Demo domain — self-contained pipeline for docker compose demo."""

import os
from pathlib import Path

from framework.builder.core_loader import FrameworkLoader

config_dir = Path(__file__).resolve().parent / "configs"

environment = os.getenv("ENVIRONMENT", "local")
loader = FrameworkLoader(config_dir=config_dir, environment=environment)
defs = loader.get_definitions()
