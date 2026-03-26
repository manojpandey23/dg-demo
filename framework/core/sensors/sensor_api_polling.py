"""
API-polling sensor handler.

Pings a configured API endpoint before every tick.  If the endpoint
returns a successful (2xx) response the underlying job is triggered;
otherwise the tick is skipped with a reason so Dagster records why
the run was not requested.
"""

import time
from typing import Callable

import dagster as dg

from framework.core.sensors.sensor_registry import sensor_handler
from framework.model.config_models import SensorConfig, SensorType


@sensor_handler(SensorType.api_polling)
def handle_api_polling_sensor(config: SensorConfig) -> Callable:
    """Build a sensor that health-checks an API before triggering a job.

    Expected ``config.config`` keys::

        resource : str — Dagster resource key for the API session (default ``api_resource``)
        endpoint : str — Path to ping, e.g. ``/cash_balance``
        method   : str — HTTP method (default ``GET``)

    ``config.trigger.target`` must reference the job to run.
    """

    trigger = config.trigger
    job_name: str | None = trigger.target
    interval = trigger.minimum_interval_seconds or 60

    if not job_name:
        raise ValueError(
            f"Sensor '{config.name}': trigger.target (job name) is required"
        )

    sensor_cfg = config.config or {}
    resource_key: str = sensor_cfg.get("resource", "api_resource")
    endpoint: str = sensor_cfg.get("endpoint", "/")
    method: str = sensor_cfg.get("method", "GET").upper()

    @dg.sensor(
        name=config.name,
        description=config.description,
        job_name=job_name,
        minimum_interval_seconds=interval,
        default_status=dg.DefaultSensorStatus.RUNNING,
        required_resource_keys={resource_key},
    )
    def api_polling_sensor(context: dg.SensorEvaluationContext):
        session = getattr(context.resources, resource_key)
        url = f"{session.base_url}{endpoint}"

        try:
            if method == "GET":
                resp = session.get(url, timeout=session.timeout)
            elif method == "POST":
                resp = session.post(url, timeout=session.timeout)
            elif method == "HEAD":
                resp = session.head(url, timeout=session.timeout)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            if resp.ok:
                context.log.info(
                    f"✅ API healthy ({resp.status_code}) at {url} — triggering '{job_name}'"
                )
                yield dg.RunRequest(
                    run_key=f"{job_name}-{int(time.time())}",
                    tags={
                        "sensor": config.name,
                        "api_status": str(resp.status_code),
                    },
                )
            else:
                reason = (
                    f"API returned {resp.status_code} at {url} — "
                    f"skipping '{job_name}'"
                )
                context.log.warning(f"⚠️ {reason}")
                yield dg.SkipReason(reason)

        except Exception as exc:
            reason = f"API unreachable at {url}: {exc} — skipping '{job_name}'"
            context.log.warning(f"⚠️ {reason}")
            yield dg.SkipReason(reason)

    return api_polling_sensor

