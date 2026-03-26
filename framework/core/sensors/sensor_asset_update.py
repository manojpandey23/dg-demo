# price_domain/framework/core/sensors/sensor_asset_update.py
import dagster as dg

from framework.core.sensors.sensor_registry import sensor_handler
from framework.model.config_models import SensorConfig, SensorType


@sensor_handler(SensorType.asset_update)
def handle_asset_update_sensor(config: SensorConfig):
    target_asset = config.trigger.target

    if not target_asset:
        raise ValueError(
            f"Sensor '{config.name}': trigger.target is required"
        )

    @dg.asset_sensor(
        asset_key=dg.AssetKey(target_asset),
        name=config.name,
        description=config.description,
        minimum_interval_seconds=config.trigger.minimum_interval_seconds or 1,
        default_status=dg.DefaultSensorStatus.RUNNING,
    )
    def asset_update_sensor(context, asset_event):
        if asset_event.asset_materialization is None:
            return dg.SkipReason("No materialization")

        return dg.SkipReason("Update processed")

    return asset_update_sensor
