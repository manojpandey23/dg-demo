"""Demo definitions — loads all demo pipelines from configs/.

Each .macro file in configs/ defines an independent pipeline.
The framework merges them into a single Dagster deployment so
you can explore all capabilities from one UI.
"""

import os
from pathlib import Path

from framework import FrameworkLoader

config_dir = Path(__file__).resolve().parent / "configs"
environment = os.getenv("ENVIRONMENT", "local")

loader = FrameworkLoader(config_dir=config_dir, environment=environment)
defs = loader.get_definitions()
