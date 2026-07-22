"""Config-driven Dagster pipeline framework.

Build data pipelines from YAML configuration files. Define assets, jobs,
sensors, and validations in .macro and .resource files — the framework
compiles them into a fully functional Dagster deployment.

Usage::

    from pathlib import Path
    from framework import FrameworkLoader

    loader = FrameworkLoader(config_dir=Path("configs"))
    defs = loader.get_definitions()
"""

from framework.builder.core_loader import FrameworkLoader
from framework.builder.resources_builder import ResourceBuilder
from framework.utils.expr_eval import expr_function

__version__ = "0.1.0"
__all__ = ["FrameworkLoader", "ResourceBuilder", "expr_function", "__version__"]
