"""
Validation package initialization.

This module ensures that ALL validation rules are imported
so that decorator-based registration runs and the
ValidationRegistry is populated.

⚠️ Do NOT remove this file.
"""

import importlib
import pkgutil

# Import registry so it exists before rules load
from framework.validation.engine import ValidationRegistry  # noqa: F401

# -------------------------------------------------------------------
# Dynamically import all rule modules under validation.rules
# -------------------------------------------------------------------

def _load_validation_rules():
    """
    Import all modules in validation.rules so decorators execute.
    """
    package_name = __name__ + ".rules"

    try:
        package = importlib.import_module(package_name)
    except ImportError as e:
        raise RuntimeError(
            f"Failed to import validation rules package '{package_name}'"
        ) from e

    for _, module_name, is_pkg in pkgutil.iter_modules(package.__path__):
        if is_pkg:
            continue

        full_module_name = f"{package_name}.{module_name}"
        importlib.import_module(full_module_name)


# ✅ Load rules at import time
_load_validation_rules()

# Optional: expose registry for debugging
__all__ = ["ValidationRegistry"]