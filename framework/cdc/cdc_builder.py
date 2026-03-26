"""
CDC builder — auto-generates Dagster sensors and resources for assets
with ``change_tracking: true``.

For each CDC-enabled asset and each stream declared on it, the builder
creates:

1. An event-dispatcher resource (wrapping the stream handler).
2. A CDC sensor that polls the per-asset change log table and
   dispatches events through the resource.

The generated objects are merged into the ``Definitions`` by the
framework loader.
"""

from typing import Any

import dagster as dg

# Ensure stream handlers are registered before we resolve them
import framework.core.streams  # noqa: F401
from framework.cdc.store import derive_change_log_table
from framework.core.resources.resource_event_dispatcher import (
    build_event_dispatcher_resource,
)
from framework.core.sensors.sensor_registry import SENSOR_REGISTRY
from framework.model.config_models import (
    AssetConfig,
    SensorConfig,
    SensorTriggerConfig,
    SensorType,
    StreamConfig,
)


def _stream_resource_key(asset_name: str, index: int) -> str:
    """Deterministic resource key for a stream on an asset."""
    return f"cdc_stream_{asset_name}_{index}"


def _sensor_name(asset_name: str, index: int, stream: StreamConfig) -> str:
    """Deterministic sensor name for a stream on an asset."""
    return f"cdc_{asset_name}_{stream.type.value}_{index}"


class CDCBuilder:
    """Factory for building CDC sensors and resources from asset configs."""

    @staticmethod
    def build(
        asset_configs: list[AssetConfig],
    ) -> tuple[list[Any], dict[str, dg.ResourceDefinition]]:
        """Scan assets for CDC configuration and build objects.

        Parameters
        ----------
        asset_configs:
            All asset configs from the pipeline.

        Returns
        -------
        Tuple of (sensors, resources_dict):
            - sensors: list of Dagster sensor definitions
            - resources_dict: ``{resource_key: ResourceDefinition}``
        """
        sensors: list[Any] = []
        resources: dict[str, dg.ResourceDefinition] = {}

        for asset_cfg in asset_configs:
            if not asset_cfg.change_tracking or not asset_cfg.streams:
                continue

            source = asset_cfg.source or {}
            table_fqn: str = source.get("table", "")
            db_resource_key: str = source.get("resource", "postgres_resource")

            if not table_fqn:
                raise ValueError(
                    f"CDC-enabled asset '{asset_cfg.name}' must have "
                    f"source.table defined"
                )

            change_log_fqn = derive_change_log_table(table_fqn)

            for idx, stream_cfg in enumerate(asset_cfg.streams):
                # 1. Build event dispatcher resource
                res_key = _stream_resource_key(asset_cfg.name, idx)
                resources[res_key] = build_event_dispatcher_resource(stream_cfg)

                # 2. Derive topic name and build sensor tags
                topic = f"cdc.{asset_cfg.name}"
                sensor_tags: dict[str, str] = {}
                if asset_cfg.tags:
                    sensor_tags.update(asset_cfg.tags)
                sensor_tags["cdc/topic"] = topic
                sensor_tags["cdc/stream_type"] = stream_cfg.type.value
                sensor_tags["cdc/asset"] = asset_cfg.name

                # 3. Build CDC sensor via the registered handler
                sensor_cfg = SensorConfig(
                    name=_sensor_name(asset_cfg.name, idx, stream_cfg),
                    type=SensorType.cdc,
                    description=(
                        f"CDC dispatch for '{asset_cfg.name}' "
                        f"via {stream_cfg.type.value} → "
                        f"{stream_cfg.relay_endpoint}"
                    ),
                    tags=sensor_tags,
                    trigger=SensorTriggerConfig(
                        type="cdc",
                        minimum_interval_seconds=stream_cfg.config.get(
                            "poll_interval_seconds", 10
                        ),
                    ),
                    config={
                        "change_log_table": change_log_fqn,
                        "db_resource_key": db_resource_key,
                        "stream_resource_key": res_key,
                        "batch_size": stream_cfg.config.get("batch_size", 100),
                        "asset_name": asset_cfg.name,
                        "table_fqn": table_fqn,
                        "topic": topic,
                    },
                )

                handler = SENSOR_REGISTRY.get(SensorType.cdc)
                sensor_def = handler(sensor_cfg)
                sensors.append(sensor_def)

        return sensors, resources
