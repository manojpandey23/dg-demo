# src/price_domain/defs/utils/diff.py
import pandas as pd
import numpy as np


def compute_row_diff(
    existing: pd.DataFrame,
    incoming: pd.DataFrame,
    key: str,
    compare_cols: list[str],
    ts_col: str,
):
    """
    CDC-style diff:
    - 1 row per key on BOTH sides
    - consistent dedup logic
    - stable ordering
    """

    # -------------------------
    # 1️⃣ Deduplicate BOTH sides
    # -------------------------
    def dedup(df: pd.DataFrame) -> pd.DataFrame:
        return (
            df.sort_values(ts_col)
            .drop_duplicates(subset=[key], keep="last")
        )

    incoming = dedup(incoming)
    existing = dedup(existing)

    # -------------------------
    # 2️⃣ Index on key
    # -------------------------
    existing_idx = existing.set_index(key)
    incoming_idx = incoming.set_index(key)

    # -------------------------
    # 3️⃣ Added rows
    # -------------------------
    added_keys = incoming_idx.index.difference(existing_idx.index)
    added = incoming_idx.loc[added_keys].reset_index()

    # -------------------------
    # 4️⃣ Updated rows
    # -------------------------
    common_keys = existing_idx.index.intersection(incoming_idx.index)

    if common_keys.empty:
        return {
            "added": added.to_dict(orient="records"),
            "updated": [],
        }

    common_keys = common_keys.sort_values()

    existing_vals = existing_idx.loc[common_keys, compare_cols].to_numpy()
    incoming_vals = incoming_idx.loc[common_keys, compare_cols].to_numpy()

    changed_mask = np.any(existing_vals != incoming_vals, axis=1)

    updated = (
        incoming_idx.loc[common_keys]
        .iloc[changed_mask]
        .reset_index()
    )

    return {
        "added": added.to_dict(orient="records"),
        "updated": updated.to_dict(orient="records"),
    }


def json_sanitize(obj):
    """
    Recursively convert objects to JSON-safe types.
    """
    if isinstance(obj, dict):
        return {k: json_sanitize(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [json_sanitize(v) for v in obj]
    if isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    if isinstance(obj, np.generic):
        return obj.item()
    return obj