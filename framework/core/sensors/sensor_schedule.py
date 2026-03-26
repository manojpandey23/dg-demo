# price_domain/framework/core/sensors/sensor_schedule.py
import dagster as dg
import time

from framework.core.sensors.sensor_registry import sensor_handler
from framework.model.config_models import SensorConfig, SensorType


@sensor_handler(SensorType.schedule)
def handle_schedule_sensor(config: SensorConfig):
    trigger = config.trigger
    cron = trigger.cron
    job_name = trigger.target

    if not cron:
        raise ValueError(
            f"Sensor '{config.name}': trigger.cron is required"
        )
    if not job_name:
        raise ValueError(
            f"Sensor '{config.name}': trigger.target (job name) is required"
        )

    @dg.sensor(
        name=config.name,
        description=config.description,
        job_name=job_name,
        minimum_interval_seconds=1,
        default_status=dg.DefaultSensorStatus.RUNNING,
    )
    def schedule_sensor(context: dg.SensorEvaluationContext):
        yield dg.RunRequest(
            run_key=f"{job_name}-{int(time.time())}"
        )

    return schedule_sensor