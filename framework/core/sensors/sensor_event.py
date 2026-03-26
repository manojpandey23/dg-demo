# price_domain/framework/core/sensors/sensor_event.py
import dagster as dg
import time

from framework.core.sensors.sensor_registry import sensor_handler
from framework.model.config_models import SensorConfig, SensorType


@sensor_handler(SensorType.event)
def handle_event_sensor(config: SensorConfig):
    target_asset = config.trigger.target

    if not target_asset:
        raise ValueError(
            f"Sensor '{config.name}': trigger.target (asset name) is required"
        )

    @dg.asset_sensor(
        asset_key=dg.AssetKey(target_asset),
        name=config.name,
        description=config.description,
        default_status=dg.DefaultSensorStatus.RUNNING,
    )
    def event_sensor(context, asset_event):
        if asset_event.asset_materialization is None:
            return dg.SkipReason("No asset materialization")

        yield dg.RunRequest(
            run_key=f"event-{int(time.time())}"
        )

    return event_sensor