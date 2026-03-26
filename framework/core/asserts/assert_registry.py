
from typing import Callable, Dict
from framework.model.config_models import AssertType


class AssertRegistry:
    _handlers: Dict[AssertType, Callable] = {}

    @classmethod
    def register(cls, assert_type: AssertType, handler: Callable):
        if assert_type in cls._handlers:
            raise ValueError(
                f"Duplicate assert handler for type '{assert_type}'"
            )
        cls._handlers[assert_type] = handler

    @classmethod
    def get(cls, assert_type: AssertType) -> Callable:
        if assert_type not in cls._handlers:
            raise ValueError(
                f"No assert handler registered for type '{assert_type}'"
            )
        return cls._handlers[assert_type]

    @classmethod
    def all(cls) -> Dict[AssertType, Callable]:
        return dict(cls._handlers)

ASSERT_REGISTRY = AssertRegistry()


def assert_handler(assert_type: AssertType):
    def decorator(fn):
        AssertRegistry.register(assert_type, fn)
        return fn
    return decorator
