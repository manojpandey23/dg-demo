
from pathlib import Path
from typing import Callable

import dagster as dg
import pandas as pd
from dagster import MetadataValue

from framework.core.asserts.assert_registry import assert_handler
from framework.model.config_models import (
    AssertType,
    AssetConfig,
    FileFormatConfig,
    FileFormatType,
)


def _read_file(
    file_path: str,
    file_format: FileFormatConfig | None,
    context: dg.AssetExecutionContext,
) -> pd.DataFrame:
    """Read a file into a DataFrame using an optional file format config.

    Parameters
    ----------
    file_path:
        Absolute path to the file on disk.
    file_format:
        Optional ``FileFormatConfig``.  When provided its ``type``
        determines the reader and ``to_pandas_kwargs()`` supplies the
        keyword arguments.  When ``None`` the reader is inferred from
        the file extension with default kwargs.
    context:
        Dagster execution context (for logging).
    """
    ext = Path(file_path).suffix.lower()

    if file_format is not None:
        kwargs = file_format.to_pandas_kwargs()
        fmt_type = file_format.type

        context.log.info(
            f"Using file formatter '{file_format.name}' "
            f"(type={fmt_type.value}) with kwargs={kwargs}"
        )

        if fmt_type == FileFormatType.csv:
            encoding = kwargs.pop("encoding", "utf-8")
            try:
                return pd.read_csv(file_path, encoding=encoding, **kwargs)
            except UnicodeDecodeError:
                context.log.warning(
                    f"Encoding '{encoding}' failed, retrying with latin1"
                )
                return pd.read_csv(file_path, encoding="latin1", **kwargs)

        elif fmt_type == FileFormatType.json:
            return pd.read_json(file_path, **kwargs)

        elif fmt_type == FileFormatType.excel:
            return pd.read_excel(file_path, **kwargs)

        else:
            raise ValueError(f"Unsupported file format type: {fmt_type}")

    # --- No formatter: infer from extension (legacy behaviour) ---
    if ext == ".csv":
        try:
            return pd.read_csv(file_path, encoding="utf-8")
        except UnicodeDecodeError:
            context.log.warning("UTF-8 decode failed, retrying with latin1")
            return pd.read_csv(file_path, encoding="latin1")

    elif ext == ".json":
        return pd.read_json(file_path)

    elif ext in (".xlsx", ".xls"):
        return pd.read_excel(file_path)

    else:
        raise ValueError(
            f"Unsupported file type '{ext}' for file '{file_path}'"
        )


@assert_handler(AssertType.file)
def handle_file_asset(
    config: AssetConfig,
    asset_deps: dict[str, list[dg.AssetKey]],
    file_formatters: dict[str, FileFormatConfig] | None = None,
) -> Callable:
    """Build a file-reading Dagster asset.

    Parameters
    ----------
    config:
        The ``AssetConfig`` for this file asset.
    asset_deps:
        Pre-computed dependency map.
    file_formatters:
        Registry of named ``FileFormatConfig`` objects, keyed by name.
        Resolved from the ``file_formatters`` section of the pipeline
        config.
    """
    partition_name: str | None = config.partition_name
    partitions_def = dg.DynamicPartitionsDefinition(name=partition_name)

    # Resolve the file format (if referenced)
    file_format: FileFormatConfig | None = None
    if config.file_format:
        if not file_formatters or config.file_format not in file_formatters:
            raise ValueError(
                f"Asset '{config.name}' references file_format "
                f"'{config.file_format}' which is not defined in "
                f"file_formatters"
            )
        file_format = file_formatters[config.file_format]

    @dg.asset(
        name=config.name,
        group_name=config.group_name,
        tags=config.tags or {},
        deps=asset_deps.get(config.name, []),
        partitions_def=partitions_def,
    )
    def file_asset(context: dg.AssetExecutionContext) -> pd.DataFrame:
        file_path = context.partition_key
        file_ext = Path(file_path).suffix.lower()

        context.log.info(f"Reading file: {file_path}")

        try:
            df = _read_file(file_path, file_format, context)
        except Exception as e:
            context.log.error(f"Failed to read file '{file_path}': {e}")
            raise

        df["file_name"] = Path(file_path).name

        context.add_output_metadata(
            {
                "file_path": MetadataValue.text(file_path),
                "rows": MetadataValue.int(len(df)),
                "columns": MetadataValue.json(list(df.columns)),
                "file_type": MetadataValue.text(file_ext),
                "file_format": MetadataValue.text(
                    file_format.name if file_format else "auto"
                ),
            }
        )

        context.log.info(f"✅ Loaded {len(df)} rows from {file_path}")


        return df

    return file_asset
