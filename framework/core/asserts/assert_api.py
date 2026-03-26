from typing import Callable

import dagster as dg
import pandas as pd
from dagster import MetadataValue
from framework.core.utils.json_to_pd import to_dataframe

from framework.core.asserts.assert_registry import assert_handler
from framework.model.config_models import AssertType, AssetConfig


@assert_handler(AssertType.api)
def handle_api_asset(
    config: AssetConfig,
    asset_deps: dict[str, list[dg.AssetKey]],
) -> Callable:
    """Build an API polling asset using api_resource"""
    source_config = config.source or {}
    endpoint = source_config.get("endpoint", "/prices")
    method = source_config.get("method", "GET").upper()

    @dg.asset(
        name=config.name,
        group_name=config.group_name,
        tags=config.tags or {},
        deps=asset_deps.get(config.name, []),
        required_resource_keys={"api_resource"},  # Require api_resource
    )
    def api_asset(context: dg.AssetExecutionContext) -> pd.DataFrame:
        """Fetch data from REST API using resource"""
        try:
            # Get API resource from context
            session = context.resources.api_resource

            # Build URL
            url = f"{session.base_url}{endpoint}"

            # Make request
            if method == "GET":
                resp = session.get(url, timeout=session.timeout)
            elif method == "POST":
                resp = session.post(url, timeout=session.timeout)
            else:
                raise ValueError(f"Unsupported HTTP method: {method}")

            df = to_dataframe(resp)

            # Log metadata
            context.add_output_metadata(
                {
                    "source": MetadataValue.text("api"),
                    "url": MetadataValue.text(url),
                    "method": MetadataValue.text(method),
                    "rows": MetadataValue.int(len(df)),
                    "columns": MetadataValue.json(list(df.columns)),
                }
            )

            context.log.info(f"✅ Fetched {len(df)} rows from {url}")
            return df

        except Exception as e:
            context.log.error(f"API asset '{config.name}' failed: {str(e)}")
            raise

    return api_asset
