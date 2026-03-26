
from typing import Callable, Dict
from framework.model.config_models import SensorType


class SensorRegistry:
    _handlers: Dict[SensorType, Callable] = {}

    @classmethod
    def register(cls, sensor_type: SensorType, handler: Callable):
        if sensor_type in cls._handlers:
            raise ValueError(
                f"Duplicate sensor handler for type '{sensor_type}'"
            )
        cls._handlers[sensor_type] = handler

    @classmethod
    def get(cls, sensor_type: SensorType) -> Callable:
        if sensor_type not in cls._handlers:
            raise ValueError(
                f"No sensor handler registered for type '{sensor_type}'"
            )
        return cls._handlers[sensor_type]

    @classmethod
    def all(cls) -> Dict[SensorType, Callable]:
        return dict(cls._handlers)

SENSOR_REGISTRY = SensorRegistry()


def sensor_handler(sensor_type: SensorType):
    def decorator(fn):
        SensorRegistry.register(sensor_type, fn)
        return fn
    return decorator