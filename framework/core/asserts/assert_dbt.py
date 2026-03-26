from typing import Callable

import dagster as dg
from dagster_dbt import DbtProjectComponent

from framework.core.asserts.assert_registry import assert_handler
from framework.model.config_models import AssetConfig, AssertType


@assert_handler(AssertType.dbt)
def handle_dbt_component(
        config: AssetConfig,
        asset_deps: dict[str, list[dg.AssetKey]],
) -> Callable:
    source = config.source or {}

    component = DbtProjectComponent(
        project=source["project"],
        select=source.get("select"),
        exclude=source.get("exclude"),
    )

    # ✅ Convert component → Dagster assets
    assets_def = component.build_assets_definition(
        key_prefix=[config.name],
        group_name=config.group_name,
        tags=config.tags or {},
    )

    return assets_def