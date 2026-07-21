"""Backend registry — decorator-based registration for database backends."""

from __future__ import annotations

from typing import Dict, Type

from framework.backends.base import DatabaseBackend


class BackendRegistry:
    _handlers: Dict[str, Type[DatabaseBackend]] = {}

    @classmethod
    def register(cls, backend_type: str, backend_class: Type[DatabaseBackend]) -> None:
        if backend_type in cls._handlers:
            raise ValueError(
                f"Duplicate backend registered for type '{backend_type}'"
            )
        cls._handlers[backend_type] = backend_class

    @classmethod
    def get(cls, backend_type: str) -> Type[DatabaseBackend]:
        if backend_type not in cls._handlers:
            raise ValueError(
                f"No backend registered for type '{backend_type}'. "
                f"Available: {list(cls._handlers.keys())}"
            )
        return cls._handlers[backend_type]

    @classmethod
    def all(cls) -> Dict[str, Type[DatabaseBackend]]:
        return dict(cls._handlers)


BACKEND_REGISTRY = BackendRegistry


def backend_handler(backend_type: str):
    """Decorator to register a DatabaseBackend implementation."""

    def decorator(cls: Type[DatabaseBackend]) -> Type[DatabaseBackend]:
        BackendRegistry.register(backend_type, cls)
        return cls

    return decorator
