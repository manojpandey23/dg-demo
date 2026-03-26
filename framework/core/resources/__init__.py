# price_domain/framework/core/resources/__init__.py
import importlib
import pkgutil
from pathlib import Path

from framework.core.resources.resource_registry import (
    RESOURCE_REGISTRY,
    resource_handler,
)

package_dir = Path(__file__).parent

for module in pkgutil.iter_modules([str(package_dir)]):
    if not module.ispkg and module.name not in {"resource_registry"}:
        importlib.import_module(f"{__name__}.{module.name}")