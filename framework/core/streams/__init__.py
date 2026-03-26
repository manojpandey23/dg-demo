"""
Stream module — auto-discovers and registers stream handler implementations.
"""

import importlib
import pkgutil
from pathlib import Path

from framework.core.streams.stream_registry import (
    STREAM_REGISTRY,
    EventDispatcher,
    stream_handler,
)

# Auto-import all stream_*.py modules so decorators execute
package_dir = Path(__file__).parent

for module in pkgutil.iter_modules([str(package_dir)]):
    if not module.ispkg and module.name not in {"stream_registry"}:
        importlib.import_module(f"{__name__}.{module.name}")

