"""
Event dispatcher Dagster resource.

Wraps a stream dispatcher (from the stream registry) as a Dagster
resource so it can be injected into CDC sensors via
``required_resource_keys``.

Not registered via the resource registry — created programmatically
by the CDC builder for each stream configuration.
"""

import dagster as dg

from framework.core.streams.stream_registry import STREAM_REGISTRY, EventDispatcher
from framework.model.config_models import StreamConfig


def build_event_dispatcher_resource(
    stream_config: StreamConfig,
) -> dg.ResourceDefinition:
    """Create a Dagster resource wrapping a stream dispatcher.

    Parameters
    ----------
    stream_config:
        The ``StreamConfig`` from the asset's ``streams`` list.

    Returns
    -------
    A ``dg.ResourceDefinition`` that yields an ``EventDispatcher``.
    """
    # Resolve the factory from the stream registry at definition time
    factory = STREAM_REGISTRY.get(stream_config.type)

    @dg.resource(
        description=(
            f"CDC event dispatcher ({stream_config.type.value}) → "
            f"{stream_config.relay_endpoint}"
        ),
    )
    def _resource(_context: dg.InitResourceContext) -> EventDispatcher:
        dispatcher = factory(stream_config)
        try:
            yield dispatcher
        finally:
            dispatcher.close()

    return _resource

