# framework/transformation/system_context.py
from dataclasses import dataclass
from typing import Optional, Mapping
import dagster as dg


@dataclass(frozen=True)
class RunContextView:
    run_id: str
    tags: Mapping[str, str]


@dataclass(frozen=True)
class AssetContextView:
    run: RunContextView
    asset_key: str
    partition_key: Optional[str]


def build_system_context(
    context: dg.AssetExecutionContext,
) -> AssetContextView:
    return AssetContextView(
        run=RunContextView(
            run_id=context.run.run_id,
            tags=dict(context.run.tags or {}),
        ),
        asset_key=".".join(context.asset_key.path) if context.asset_key else None,
        partition_key=(context.partition_key if context.has_partition_key else None),
    )
