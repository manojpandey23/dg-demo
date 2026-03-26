from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel


class ChangeEvent(BaseModel):
    asset: str
    op: Literal["INSERT", "UPDATE", "DELETE"]
    pk: dict[str, Any]
    before: dict | None
    after: dict | None
    run_id: str
    ts: datetime
