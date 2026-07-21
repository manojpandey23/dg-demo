"""Database backend registry with auto-discovery."""

from __future__ import annotations

import importlib
import pkgutil
from pathlib import Path

from framework.backends.base import DatabaseBackend
from framework.backends.registry import (
    BACKEND_REGISTRY,
    BackendRegistry,
    backend_handler,
)

__all__ = [
    "DatabaseBackend",
    "BackendRegistry",
    "BACKEND_REGISTRY",
    "backend_handler",
    "get_backend_for_resource",
]


def get_backend_for_resource(resource_type: str) -> DatabaseBackend:
    """Instantiate a backend from a resource type string."""
    backend_cls = BackendRegistry.get(resource_type)
    return backend_cls()


_package_dir = Path(__file__).parent
for _module in pkgutil.iter_modules([str(_package_dir)]):
    if not _module.ispkg and _module.name not in {"base", "registry"}:
        try:
            importlib.import_module(f"{__name__}.{_module.name}")
        except ImportError:
            pass
