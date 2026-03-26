from typing import Callable

import dagster as dg
import pandas as pd

from framework.core.asserts.assert_registry import assert_handler
from framework.model.config_models import AssetConfig, AssertType


@assert_handler(AssertType.computed)
def handle_computed_asset(config: AssetConfig,
                          asset_deps: dict[str, list[dg.AssetKey]], ) -> Callable:
    """Build a computed/transformed asset"""
    depends_on = config.depends_on or []

    if not depends_on:
        raise ValueError(f"Computed asset '{config.name}': depends_on is required")

    @dg.asset(
        name=config.name,
        group_name=config.group_name,
        ins={
            "input_df": dg.AssetIn(key=asset_deps[config.name][0]),
        },
        tags=config.tags or {},
        deps=asset_deps.get(config.name, []),
    )
    def computed_asset(context: dg.AssetExecutionContext, input_df: pd.DataFrame) -> pd.DataFrame:
        """Placeholder computed asset - transform input data"""

        context.log.info(f"✅ Computed asset '{config.name}' transformed {len(input_df)} rows")
        return input_df

    return computed_asset