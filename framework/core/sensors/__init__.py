# price_domain/framework/core/sensors/__init__.py
import importlib
import pkgutil
from pathlib import Path

from framework.core.sensors.sensor_registry import (
    SENSOR_REGISTRY,
    sensor_handler,
)

# Register expression functions (rdd, today, …) at import time
import framework.utils.expr_eval  # noqa: F401

package_dir = Path(__file__).parent

for module in pkgutil.iter_modules([str(package_dir)]):
    if not module.ispkg and module.name not in {"sensor_registry"}:
        importlib.import_module(f"{__name__}.{module.name}")