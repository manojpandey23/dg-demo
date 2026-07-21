from collections import defaultdict
from typing import Callable

import dagster as dg
import pandas as pd
from dagster import MetadataValue, TableColumnDep, TableColumnLineage

from framework.backends import get_backend_for_resource
from framework.cdc.capture import capture_cdc_events
from framework.core.asserts.assert_registry import assert_handler
from framework.model.config_models import AssertType, AssetConfig
from framework.transformation.system_context import build_system_context
from framework.transformation.transformation_executor import apply_transformations


def build_asset_ins(
    upstream_keys: list[dg.AssetKey],
) -> dict[str, dg.AssetIn]:
    return {f"input_{i}": dg.AssetIn(key=key) for i, key in enumerate(upstream_keys)}


def normalize_upstream_inputs(
    inputs: dict[str, pd.DataFrame | None],
    upstream_keys: list[dg.AssetKey],
) -> list[tuple[dg.AssetKey, pd.DataFrame]]:
    """
    Align Dagster inputs with their upstream AssetKeys,
    ignoring upstreams that were not materialized.
    """
    normalized: list[tuple[dg.AssetKey, pd.DataFrame]] = []

    for key, df in zip(upstream_keys, inputs.values()):
        if df is None:
            continue
        if isinstance(df, pd.DataFrame) and df.empty:
            continue
        normalized.append((key, df))

    return normalized


def build_table_column_lineage(
    lineage_maps: list[dict[str, dict[dg.AssetKey, set[str]]]],
) -> TableColumnLineage:

    deps_by_column: dict[str, list[TableColumnDep]] = defaultdict(list)

    for lineage in lineage_maps:
        for out_col, upstreams in lineage.items():
            for asset_key, input_cols in upstreams.items():
                for input_col in input_cols:
                    deps_by_column[out_col].append(
                        TableColumnDep(
                            asset_key=asset_key,
                            column_name=input_col,
                        )
                    )

    return TableColumnLineage(deps_by_column)


@assert_handler(AssertType.database)
def handle_database_asset_v2(
    config: AssetConfig,
    asset_deps: dict[str, list[dg.AssetKey]],
) -> Callable:

    source = config.source or {}
    table_fqn = source["table"]
    resource = source["resource"]

    model = config.get_database_model()

    upstream_keys = asset_deps.get(config.name, [])
    ins = {f"input_{i}": dg.AssetIn(key=k) for i, k in enumerate(upstream_keys)}

    resource_type = source.get("backend", "postgres")
    backend = get_backend_for_resource(resource_type)

    @dg.asset(
        name=config.name,
        group_name=config.group_name,
        tags=config.tags or {},
        ins=ins,
        deps=upstream_keys,
        required_resource_keys={resource},
    )
    def db_asset(
        context: dg.AssetExecutionContext,
        **inputs: pd.DataFrame,
    ) -> pd.DataFrame:

        db = getattr(context.resources, resource)
        backend.set_connection(db)
        cursor = backend.get_cursor()

        try:
            backend.begin_transaction(cursor)
            system_context = build_system_context(context)

            dfs: list[pd.DataFrame] = []
            lineage_maps: list[dict] = []

            for upstream_key, df in zip(upstream_keys, inputs.values()):
                out_df, lineage = apply_transformations(
                    df=df,
                    schema=config.columns,
                    upstream_asset_key=upstream_key,
                    system_context=system_context,
                    transforms=config.transforms,
                )
                dfs.append(out_df)
                if lineage:
                    lineage_maps.append(lineage)

            final_df = (
                dfs[0]
                if len(dfs) == 1
                else pd.concat(dfs, ignore_index=True, copy=False)
            )

            op_meta = backend.apply_schema_and_materialize(
                cursor=cursor,
                table_fqn=table_fqn,
                target_df=final_df,
                schema=config.columns,
                materialization=model.materialization,
                on_schema_change=model.on_schema_change,
                inc_strategy=model.incremental_strategy,
                unique_key=model.unique_key,
                snapshot_strategy=model.strategy.value if model.strategy else None,
                updated_at=model.updated_at,
                check_cols=model.check_cols,
                hard_deletes=model.hard_deletes.value,
            )

            # ── CDC: capture change events (same transaction) ──
            cdc_count = 0
            if config.change_tracking:
                cdc_count = capture_cdc_events(
                    cursor=cursor,
                    config=config,
                    table_fqn=table_fqn,
                    final_df=final_df,
                    run_id=context.run.run_id,
                    materialization_type=model.materialization.value,
                    backend=backend,
                )
                context.log.info(
                    f"CDC: captured {cdc_count} change event(s) for "
                    f"'{config.name}'"
                )

            backend.commit_transaction(cursor)

            metadata = {
                "rows": MetadataValue.int(op_meta["rows_loaded"]),
                "table": MetadataValue.text(table_fqn),
                "materialization": MetadataValue.text(model.materialization.value),
                "table_created": MetadataValue.bool(op_meta["table_created"]),
                "upstream_assets": MetadataValue.json(
                    [k.to_user_string() for k in upstream_keys]
                ),
            }

            if config.change_tracking:
                metadata["cdc_events"] = MetadataValue.int(cdc_count)
                metadata["change_tracking"] = MetadataValue.bool(True)

            if lineage_maps:
                metadata["dagster/column_lineage"] = build_table_column_lineage(
                    lineage_maps
                )

            context.add_output_metadata(metadata)

        except Exception:
            backend.rollback_transaction(cursor)
            raise
        finally:
            backend.close_cursor(cursor)

        return final_df

    return db_asset
