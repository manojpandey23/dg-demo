# price_domain/framework/core/resources/registry.py
from typing import Callable, Dict
from framework.model.resource_models import ResourceType
import dagster as dg


class ResourceRegistry:
    _handlers: Dict[ResourceType, Callable[..., dg.ResourceDefinition]] = {}

    @classmethod
    def register(cls, resource_type: ResourceType, handler: Callable):
        if resource_type in cls._handlers:
            raise ValueError(
                f"Duplicate resource handler for type '{resource_type}'"
            )
        cls._handlers[resource_type] = handler

    @classmethod
    def get(cls, resource_type: ResourceType) -> Callable:
        if resource_type not in cls._handlers:
            raise ValueError(
                f"No resource handler registered for type '{resource_type}'"
            )
        return cls._handlers[resource_type]

    @classmethod
    def all(cls):
        return dict(cls._handlers)

RESOURCE_REGISTRY = ResourceRegistry()

def resource_handler(resource_type: ResourceType):
    def decorator(fn):
        ResourceRegistry.register(resource_type, fn)
        return fn
    return decorator
