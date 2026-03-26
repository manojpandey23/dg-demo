"""
WebSocket stream handler.

Dispatches CDC change events to a relay server.  The relay endpoint
is typically an HTTP POST URL (e.g. ``http://host:port/ws/emit``)
that broadcasts to WebSocket subscribers.  When the endpoint uses a
``ws://`` or ``wss://`` scheme, events are sent directly over a
WebSocket connection instead.

Events are enqueued on the calling thread and sent asynchronously via
a background worker so the Dagster sensor tick is never blocked by
network I/O.
"""

import asyncio
import json
import logging
import queue
import threading
from typing import Any

from framework.core.streams.stream_registry import stream_handler
from framework.model.config_models import StreamConfig, StreamType

logger = logging.getLogger(__name__)


def _is_ws_endpoint(endpoint: str) -> bool:
    """Return *True* if the endpoint uses a WebSocket scheme."""
    return endpoint.startswith("ws://") or endpoint.startswith("wss://")


class WebSocketDispatcher:
    """Async, non-blocking event dispatcher.

    Runs a background thread with its own asyncio event loop.
    The ``dispatch`` method is safe to call from synchronous code.

    Dispatch modes
    --------------
    * **HTTP** (default): POSTs each message as
      ``{"topic": <topic>, "data": <message>}`` to the relay endpoint.
      Requires ``httpx``.
    * **WebSocket**: Opens a WS connection and sends the JSON payload
      directly.  Used when the relay endpoint starts with ``ws://``
      or ``wss://``.  Requires ``websockets``.
    """

    def __init__(self, endpoint: str, config: dict[str, Any] | None = None) -> None:
        self._endpoint = endpoint
        self._config = config or {}
        self._queue: queue.Queue[list[dict[str, Any]]] = queue.Queue()
        self._closed = False
        self._thread = threading.Thread(
            target=self._run_event_loop, daemon=True, name="ws-dispatcher"
        )
        self._thread.start()

    # ----------------------------------------------------------------
    # Public API (called from sensor / sync context)
    # ----------------------------------------------------------------

    def dispatch(self, events: list[dict[str, Any]]) -> None:
        """Enqueue a batch of events for async delivery."""
        if self._closed:
            raise RuntimeError("Dispatcher is closed")
        if events:
            self._queue.put(events)

    def close(self) -> None:
        """Signal the background worker to stop."""
        self._closed = True

    # ----------------------------------------------------------------
    # Background worker
    # ----------------------------------------------------------------

    def _run_event_loop(self) -> None:
        asyncio.run(self._worker())

    async def _worker(self) -> None:
        if _is_ws_endpoint(self._endpoint):
            await self._ws_worker()
        else:
            await self._http_worker()

    # ----------------------------------------------------------------
    # HTTP POST mode (primary)
    # ----------------------------------------------------------------

    async def _http_worker(self) -> None:
        """POST events to an HTTP relay endpoint.

        Each message in the batch is sent as::

            {"topic": "<topic>", "data": <message>}
        """
        try:
            import httpx
        except ImportError:
            logger.error(
                "httpx is not installed — CDC events will be dropped. "
                "Install it with: pip install httpx"
            )
            return

        async with httpx.AsyncClient(timeout=5.0) as client:
            while not self._closed:
                try:
                    batch = await asyncio.to_thread(
                        self._queue.get, timeout=1.0
                    )
                except Exception:
                    continue

                for message in batch:
                    topic = message.get("topic", "unknown")
                    envelope = {"topic": topic, "data": message}
                    try:
                        resp = await client.post(
                            self._endpoint,
                            json=envelope,
                        )
                        resp.raise_for_status()
                        logger.debug(
                            "Dispatched event (topic=%s) to %s",
                            topic,
                            self._endpoint,
                        )
                    except Exception:
                        logger.exception(
                            "HTTP dispatch failed for %s (topic=%s)",
                            self._endpoint,
                            topic,
                        )

    # ----------------------------------------------------------------
    # WebSocket mode (ws:// / wss:// endpoints)
    # ----------------------------------------------------------------

    async def _ws_worker(self) -> None:
        """Send events directly over a WebSocket connection."""
        try:
            import websockets  # type: ignore[import-untyped]
        except ImportError:
            logger.error(
                "websockets package not installed — CDC events will be "
                "dropped.  Install it with: pip install websockets"
            )
            return

        while not self._closed:
            try:
                batch = await asyncio.to_thread(self._queue.get, timeout=1.0)
            except Exception:
                continue

            try:
                async with websockets.connect(self._endpoint) as ws:
                    payload = json.dumps(batch, default=str)
                    await ws.send(payload)
                    logger.debug(
                        "WS-dispatched %d event(s) to %s",
                        len(batch),
                        self._endpoint,
                    )
            except Exception:
                logger.exception(
                    "WebSocket dispatch failed for %s", self._endpoint
                )


# ------------------------------------------------------------------
# Registry entry
# ------------------------------------------------------------------


@stream_handler(StreamType.websocket)
def create_websocket_dispatcher(config: StreamConfig) -> WebSocketDispatcher:
    """Factory registered with the stream registry."""
    return WebSocketDispatcher(
        endpoint=config.relay_endpoint,
        config=config.config,
    )

