from framework.model.config_models import AssetSchema, Materialization
from typing import Dict, Any

from framework.postgres.pghelper import extract_primary_keys


def schema_dtype_to_pg(dtype: str) -> str:
    dtype = dtype.lower()

    if dtype in ("int", "integer"):
        return "BIGINT"
    if dtype in ("float", "double"):
        return "DOUBLE PRECISION"
    if dtype in ("bool", "boolean"):
        return "BOOLEAN"
    if dtype in ("string", "text"):
        return "TEXT"
    if dtype == "date":
        return "DATE"
    if dtype in ("datetime", "timestamp"):
        return "TIMESTAMPTZ"

    raise ValueError(f"Unsupported dtype: {dtype}")


def build_pg_schema_from_config(
    schema: list[AssetSchema],
    materialization: Materialization,
) -> Dict[str, Dict[str, Any]]:

    pg_schema = {}

    for col in schema:
        pg_schema[col.name] = {
            "name": col.name,
            "pgType": schema_dtype_to_pg(col.dtype),
            "dtype": col.dtype,
            # Snapshot tables must NOT have a PK on business keys because
            # SCD Type 2 stores multiple versions of the same key.
            "isKey": False if materialization == Materialization.snapshot else col.isKey,
            "nullable": False if col.isKey else col.nullable,
        }

    if materialization == Materialization.snapshot:
        pg_schema |= {
            "valid_from": {
                "name": "valid_from",
                "pgType": "TIMESTAMPTZ",
                "dtype": "datetime",
                "isKey": False,
                "nullable": False,
            },
            "valid_to": {
                "name": "valid_to",
                "pgType": "TIMESTAMPTZ",
                "dtype": "datetime",
                "isKey": False,
                "nullable": True,
            },
            "is_current": {
                "name": "is_current",
                "pgType": "BOOLEAN",
                "dtype": "boolean",
                "isKey": False,
                "nullable": False,
            },
            "is_deleted": {
                "name": "is_deleted",
                "pgType": "BOOLEAN",
                "dtype": "boolean",
                "isKey": False,
                "nullable": False,
            },
        }

    return pg_schema