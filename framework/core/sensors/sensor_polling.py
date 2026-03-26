# price_domain/framework/core/sensors/sensor_polling.py
import dagster as dg
import time

from framework.core.sensors.sensor_registry import sensor_handler
from framework.model.config_models import SensorConfig, SensorType



@sensor_handler(SensorType.polling)
def handle_polling_sensor(config: SensorConfig):
    trigger = config.trigger
    job_name = trigger.target
    interval = trigger.minimum_interval_seconds or 10

    if not job_name:
        raise ValueError(
            f"Sensor '{config.name}': trigger.target (job name) is required"
        )

    @dg.sensor(
        name=config.name,
        description=config.description,
        job_name=job_name,
        minimum_interval_seconds=interval,
        default_status=dg.DefaultSensorStatus.RUNNING,
    )
    def polling_sensor(context: dg.SensorEvaluationContext):
        yield dg.RunRequest(
            run_key=f"{job_name}-{int(time.time())}"
        )

    return polling_sensor