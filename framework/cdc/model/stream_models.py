from enum import Enum

from pydantic import BaseModel


class StreamType(str, Enum):
    websocket = "websocket"


class StreamConfig(BaseModel):
    type: StreamType
    topic: str
