"""
Stream registry — decorator-based registration of stream handlers.

Each stream type (websocket, kafka, jms, …) registers a factory
function via the ``@stream_handler`` decorator.  The factory receives
a ``StreamConfig`` and returns an ``EventDispatcher`` instance.
"""

from typing import Any, Callable, Dict, Protocol

from framework.model.config_models import StreamType


# ------------------------------------------------------------------
# EventDispatcher protocol — all stream implementations must satisfy
# ------------------------------------------------------------------

class EventDispatcher(Protocol):
    """Protocol that all stream dispatchers must implement."""

    def dispatch(self, events: list[dict[str, Any]]) -> None:
        """Send a batch of change events to the downstream consumer."""
        ...

    def close(self) -> None:
        """Release any resources held by the dispatcher."""
        ...


# ------------------------------------------------------------------
# Registry
# ------------------------------------------------------------------

class StreamRegistry:
    """Singleton registry mapping ``StreamType`` → factory callable."""

    _handlers: Dict[StreamType, Callable] = {}

    @classmethod
    def register(cls, stream_type: StreamType, handler: Callable) -> None:
        if stream_type in cls._handlers:
            raise ValueError(
                f"Duplicate stream handler for type '{stream_type}'"
            )
        cls._handlers[stream_type] = handler

    @classmethod
    def get(cls, stream_type: StreamType) -> Callable:
        if stream_type not in cls._handlers:
            raise ValueError(
                f"No stream handler registered for type '{stream_type}'"
            )
        return cls._handlers[stream_type]

    @classmethod
    def all(cls) -> Dict[StreamType, Callable]:
        return dict(cls._handlers)


STREAM_REGISTRY = StreamRegistry()


def stream_handler(stream_type: StreamType) -> Callable:
    """Decorator to register a stream handler factory.

    Usage::

        @stream_handler(StreamType.websocket)
        def create_websocket_dispatcher(config: StreamConfig) -> EventDispatcher:
            return WebSocketDispatcher(endpoint=config.relay_endpoint)
    """
    def decorator(fn: Callable) -> Callable:
        StreamRegistry.register(stream_type, fn)
        return fn
    return decorator

