from typing import List, Tuple

import pandas as pd
from dagster import AssetKey

from framework.model.config_models import AssetSchema, TransformConfig
from framework.transformation.system_context import AssetContextView
from framework.transformation.table_transforms import apply_table_transform
from framework.transformation.transform_registry import TRANSFORMS
from framework.transformation.transformation_context import TransformationContext
from framework.utils.pd_utils import coerce_dataframe_types


def apply_transformations(
    df: pd.DataFrame,
    schema: List[AssetSchema],
    upstream_asset_key: AssetKey,
    *,
    system_context: AssetContextView | None = None,
    transforms: TransformConfig | None = None,
) -> Tuple[pd.DataFrame, dict[str, dict[AssetKey, set[str]]] | None]:
    """
    Apply transformations and return raw column lineage mapping.

    Execution order:
        Phase 0 — pre  table-level  (filter / dedup / sort on input DF)
        Phase 1 — column-level      (ref / value / when / … per column expr)
        Phase 2 — post table-level  (group_by / agg / order_by / limit on output DF)
        Phase 3 — type coercion
    """

    if not schema and not transforms:
        return df, None

    # ─── Phase 0: Pre-transforms (table-level) ───
    if transforms and transforms.pre:
        df = apply_table_transform(df, transforms.pre)

    # ─── Phase 1: Column expressions ───
    ctx = TransformationContext(upstream_asset_key)

    if schema:
        output: dict[str, pd.Series] = {}

        for col in schema:
            out_col = col.name
            expr = col.expr or f'ref("{out_col}")'

            scope = TRANSFORMS.build_scope(
                df,
                ctx,
                out_col,
                system_context=system_context,
            )

            try:
                output[out_col] = eval(expr, {}, scope)
            except Exception as e:
                raise ValueError(
                    f"Failed to evaluate expression for column '{out_col}': {expr}"
                ) from e

        target_df = pd.DataFrame(output, index=df.index)
    else:
        target_df = df

    # ─── Phase 2: Post-transforms (table-level) ───
    if transforms and transforms.post:
        target_df = apply_table_transform(target_df, transforms.post)

    # ─── Phase 3: Type coercion ───
    if schema:
        target_df = coerce_dataframe_types(target_df, schema)

    # ─── Lineage ───
    if not ctx.column_lineage:
        return target_df, None

    lineage: dict[str, dict[AssetKey, set[str]]] = {}

    for out_col, in_cols in ctx.column_lineage.items():
        lineage[out_col] = {upstream_asset_key: set(in_cols)}

    return target_df, lineage
