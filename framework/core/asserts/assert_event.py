"""
Event asset handler.

Produces an asset that listens for external events (e.g. via a message queue
or webhook) and materialises the event payload as a DataFrame.

The ``source`` config block must specify:
    - ``resource``:  resource key for the event source
    - ``event_type``:  (optional) filter expression for the event type
"""

from typing import Callable, Optional

import dagster as dg
import pandas as pd

from framework.core.asserts.assert_registry import assert_handler
from framework.model.config_models import AssetConfig, AssertType


@assert_handler(AssertType.event)
def handle_event_asset(
    config: AssetConfig,
    asset_deps: dict[str, list[dg.AssetKey]],
) -> Optional[Callable]:
    """Build an event-driven asset that materialises event payloads."""
    source_config = config.source or {}
    resource_key = source_config.get("resource", "event_resource")
    event_type = source_config.get("event_type")

    required_resources = {resource_key} if resource_key else set()

    deps = asset_deps.get(config.name, [])

    @dg.asset(
        name=config.name,
        group_name=config.group_name,
        tags=config.tags or {},
        deps=deps,
        required_resource_keys=required_resources,
        description=config.description,
    )
    def event_asset(context: dg.AssetExecutionContext) -> pd.DataFrame:
        """Consume events from the configured event source."""
        event_source = getattr(context.resources, resource_key, None)
        if event_source is None:
            raise RuntimeError(
                f"Event asset '{config.name}': resource '{resource_key}' not available"
            )

        # Retrieve events — the resource is expected to expose a
        # ``consume(event_type=...)`` method returning a list of dicts.
        if not hasattr(event_source, "consume"):
            raise TypeError(
                f"Event resource '{resource_key}' must implement a 'consume' method"
            )

        events: list[dict] = event_source.consume(event_type=event_type)

        if not events:
            context.log.info(
                f"Event asset '{config.name}': no events received"
            )
            df = pd.DataFrame()
        else:
            df = pd.DataFrame(events)

        context.add_output_metadata(
            {
                "source": dg.MetadataValue.text("event"),
                "resource": dg.MetadataValue.text(resource_key),
                "event_type": dg.MetadataValue.text(event_type or "*"),
                "rows": dg.MetadataValue.int(len(df)),
            }
        )

        context.log.info(
            f"✅ Event asset '{config.name}' consumed {len(df)} events"
        )
        return df

    return event_asset
