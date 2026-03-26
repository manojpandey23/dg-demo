# price_domain/framework/core/sensor_builder.py
from typing import Callable, Optional, List

from framework.core.sensors.sensor_registry import SENSOR_REGISTRY
from framework.model.config_models import SensorConfig, SensorType


class SensorBuilder:
    """Factory for building Dagster sensors from configuration"""

    @staticmethod
    def build_sensors(configs: List[SensorConfig]) -> List:
        sensors = []
        for config in configs:
            sensor = SensorBuilder.build_sensor(config)
            if sensor:
                sensors.append(sensor)
        return sensors

    @staticmethod
    def build_sensor(config: SensorConfig) -> Optional[Callable]:
        try:
            sensor_type = SensorType(config.type)
        except ValueError:
            raise ValueError(f"Unknown sensor type: {config.type}")

        handler = SENSOR_REGISTRY.get(sensor_type)
        return handler(config)
