# price_domain/framework/core/asserts/__init__.py
import importlib
import pkgutil
from pathlib import Path

from framework.core.asserts.assert_registry import (
    ASSERT_REGISTRY,
    assert_handler,
)

# Automatically import all assert modules so decorators execute
package_dir = Path(__file__).parent

for module in pkgutil.iter_modules([str(package_dir)]):
    if not module.ispkg and module.name != "assert_registry":
        importlib.import_module(
            f"{__name__}.{module.name}"
        )